package beads

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
	beadsdk "github.com/steveyegge/beads"
)

type exportStore interface {
	GetInfraTypes(context.Context) map[string]bool
	GetLabelsForIssues(context.Context, []string) (map[string][]string, error)
	GetDependencyRecordsForIssues(context.Context, []string) (map[string][]*beadsdk.Dependency, error)
	GetCommentsForIssues(context.Context, []string) (map[string][]*beadsdk.Comment, error)
	GetDependencyCounts(context.Context, []string) (map[string]*beadsdk.DependencyCounts, error)
	GetCommentCounts(context.Context, []string) (map[string]int, error)
}

// ExportIssues writes the same durable issue records as bd export (without
// --all). Relational read errors abort rather than replacing a valid backup.
func ExportIssues(ctx context.Context, store beadsdk.Storage, path string) error {
	extended, ok := store.(exportStore)
	if !ok {
		return fmt.Errorf("store does not support native export")
	}
	lock := flock.New(path + ".lock")
	locked, err := lock.TryLockContext(ctx, 50*time.Millisecond)
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("export lock unavailable")
	}
	defer lock.Unlock()
	no := false
	filter := beadsdk.IssueFilter{IsTemplate: &no, Ephemeral: &no}
	for kind := range extended.GetInfraTypes(ctx) {
		filter.ExcludeTypes = append(filter.ExcludeTypes, beadsdk.IssueType(kind))
	}
	if len(filter.ExcludeTypes) == 0 {
		filter.ExcludeTypes = []beadsdk.IssueType{"agent", "rig", "role", "message"}
	}
	issues, err := store.SearchIssues(ctx, "", filter)
	if err != nil {
		return err
	}
	ids := make([]string, len(issues))
	for i, issue := range issues {
		ids[i] = issue.ID
	}
	labels, err := extended.GetLabelsForIssues(ctx, ids)
	if err != nil {
		return err
	}
	deps, err := extended.GetDependencyRecordsForIssues(ctx, ids)
	if err != nil {
		return err
	}
	comments, err := extended.GetCommentsForIssues(ctx, ids)
	if err != nil {
		return err
	}
	counts, err := extended.GetDependencyCounts(ctx, ids)
	if err != nil {
		return err
	}
	commentCounts, err := extended.GetCommentCounts(ctx, ids)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".issues-export-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := file.Chmod(0644); err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, issue := range issues {
		issue.Labels, issue.Dependencies, issue.Comments = labels[issue.ID], deps[issue.ID], comments[issue.ID]
		row := struct {
			RecordType string `json:"_type"`
			*beadsdk.IssueWithCounts
		}{RecordType: "issue", IssueWithCounts: &beadsdk.IssueWithCounts{Issue: issue, CommentCount: commentCounts[issue.ID]}}
		if count := counts[issue.ID]; count != nil {
			row.DependencyCount = count.DependencyCount
			row.DependentCount = count.DependentCount
		}
		if err := encoder.Encode(row); err != nil {
			return fmt.Errorf("export %s: %w", issue.ID, err)
		}
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
