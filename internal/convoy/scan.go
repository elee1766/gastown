package convoy

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	beadsdk "github.com/steveyegge/beads"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

// Snapshot is fresh convoy state. Unresolved work is never ready or complete.
type Snapshot struct {
	ID           string    `json:"id"`
	Title        string    `json:"title"`
	TrackedCount int       `json:"tracked_count"`
	ReadyCount   int       `json:"ready_count"`
	ReadyIssues  []string  `json:"ready_issues"`
	CreatedAt    time.Time `json:"created_at"`
	BaseBranch   string    `json:"base_branch,omitempty"`
	Complete     bool      `json:"complete"`
}

// Scan uses retained stores and reads scheduling state once per database.
// Only unassigned open work is dispatchable: recovery of assigned work belongs
// to the witness, not a competing periodic sling invocation.
func Scan(ctx context.Context, townRoot string, stores map[string]beadsdk.Storage, parked func(string) bool) ([]Snapshot, error) {
	hq := stores["hq"]
	if hq == nil {
		return nil, fmt.Errorf("hq store unavailable")
	}
	open := beadsdk.StatusOpen
	convoys, err := hq.SearchIssues(ctx, "", beadsdk.IssueFilter{SkipWisps: true, Labels: []string{"gt:convoy"}})
	if err != nil {
		return nil, err
	}
	legacyType := beadsdk.IssueType("convoy")
	legacy, err := hq.SearchIssues(ctx, "", beadsdk.IssueFilter{SkipWisps: true, IssueType: &legacyType})
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(convoys))
	for _, issue := range convoys {
		seen[issue.ID] = true
	}
	for _, issue := range legacy {
		if !seen[issue.ID] {
			convoys = append(convoys, issue)
		}
	}
	if len(convoys) == 0 {
		return []Snapshot{}, nil
	}
	scheduled := make(map[string]bool)
	for name, store := range stores {
		if name != "hq" && parked != nil && parked(name) {
			continue
		}
		ephemeral := true
		contexts, err := store.SearchIssues(ctx, "", beadsdk.IssueFilter{Status: &open, Labels: []string{capacity.LabelSlingContext}, Ephemeral: &ephemeral})
		if err != nil {
			return nil, fmt.Errorf("scheduling state %s: %w", name, err)
		}
		for _, issue := range contexts {
			if fields := beads.ParseSlingContextFields(issue.Description); fields != nil {
				scheduled[fields.WorkBeadID] = true
			}
		}
	}
	resolver := NewStoreResolver(townRoot, stores)
	result := make([]Snapshot, 0)
	for _, issue := range convoys {
		isConvoy := string(issue.IssueType) == "convoy"
		for _, label := range issue.Labels {
			isConvoy = isConvoy || label == "gt:convoy"
		}
		if !isConvoy {
			continue
		}
		status := string(issue.Status)
		if status != "open" {
			fields := beads.ParseConvoyFields(&beads.Issue{Description: issue.Description})
			if status != "closed" || issue.CloseReason != "All tracked issues completed" || (fields != nil && fields.CompletionNotifiedAt != "") {
				continue
			}
		}
		snapshot, err := Inspect(ctx, hq, resolver, issue)
		if err != nil {
			return nil, err
		}
		for _, candidate := range snapshot.ReadyIssues {
			rig := rigForIssue(townRoot, candidate)
			if !scheduled[candidate] && rig != "" && (parked == nil || !parked(rig)) {
				snapshot.ReadyIssues[snapshot.ReadyCount] = candidate
				snapshot.ReadyCount++
			}
		}
		snapshot.ReadyIssues = snapshot.ReadyIssues[:snapshot.ReadyCount]
		result = append(result, snapshot)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

// dependencyRecords keeps unresolved external edges that joined issue reads omit.
func dependencyRecords(ctx context.Context, store beadsdk.Storage, id string) ([]*beadsdk.Dependency, error) {
	reader, ok := store.(interface {
		GetDependencyRecords(context.Context, string) ([]*beadsdk.Dependency, error)
	})
	if !ok {
		return nil, fmt.Errorf("store does not expose raw dependency records")
	}
	return reader.GetDependencyRecords(ctx, id)
}

// Inspect refuses completion when any tracked issue cannot be resolved in its
// owning store. Dependency snapshots in HQ are not authoritative across rigs.
func Inspect(ctx context.Context, hq beadsdk.Storage, resolver *StoreResolver, issue *beadsdk.Issue) (Snapshot, error) {
	snapshot := Snapshot{ID: issue.ID, Title: issue.Title, CreatedAt: issue.CreatedAt, ReadyIssues: []string{}}
	if fields := beads.ParseConvoyFields(&beads.Issue{Description: issue.Description}); fields != nil {
		snapshot.BaseBranch = fields.BaseBranch
	}
	deps, err := dependencyRecords(ctx, hq, issue.ID)
	if err != nil {
		return snapshot, err
	}
	ids := make([]string, 0, len(deps))
	for _, dep := range deps {
		if string(dep.Type) == "tracks" {
			ids = append(ids, extractIssueID(dep.DependsOnID))
		}
	}
	snapshot.TrackedCount = len(ids)
	snapshot.Complete = len(ids) > 0
	fresh := resolver.ResolveIssues(ctx, ids)
	candidates := make([]*beadsdk.Issue, 0)
	for _, id := range ids {
		work := fresh[id]
		if work == nil {
			snapshot.Complete = false
			continue
		}
		status := strings.TrimSpace(string(work.Status))
		if status != "closed" && status != "tombstone" {
			snapshot.Complete = false
		}
		if status != "open" || work.Assignee != "" || !IsSlingableType(string(work.IssueType)) {
			continue
		}
		home := resolver.stores[resolver.storeForID(id)]
		if home == nil {
			continue
		}
		blockers, err := dependencyRecords(ctx, home, id)
		if err != nil {
			return snapshot, fmt.Errorf("dependencies for %s: %w", id, err)
		}
		blocked := false
		for _, dep := range blockers {
			kind := string(dep.Type)
			if !blockingDepTypes[kind] {
				continue
			}
			blockerID := extractIssueID(dep.DependsOnID)
			blocker := resolver.ResolveIssues(ctx, []string{blockerID})[blockerID]
			if blocker == nil {
				blocked = true
				break
			}
			if string(blocker.Status) == "tombstone" {
				continue
			}
			if string(blocker.Status) != "closed" || (kind == "merge-blocks" && !strings.HasPrefix(blocker.CloseReason, "Merged in ")) {
				blocked = true
				break
			}
		}
		if !blocked {
			candidates = append(candidates, work)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority < candidates[j].Priority
		}
		return candidates[i].ID < candidates[j].ID
	})
	for _, work := range candidates {
		snapshot.ReadyIssues = append(snapshot.ReadyIssues, work.ID)
	}
	return snapshot, nil
}
