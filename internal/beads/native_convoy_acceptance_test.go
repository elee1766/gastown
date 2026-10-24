package beads_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	beadsdk "github.com/steveyegge/beads"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/convoy"
	"github.com/steveyegge/gastown/internal/mail"
)

// This acceptance test uses disposable SQL databases with no bd or gt executable.
func TestNativeConvoyLifecycle(t *testing.T) {
	host := os.Getenv("GT_TEST_SDK_HOST")
	if host == "" {
		t.Skip("requires disposable GT_TEST_SDK_HOST")
	}
	t.Setenv("BEADS_DOLT_SERVER_HOST", host)
	t.Setenv("BEADS_DOLT_SERVER_PORT", "3306")
	t.Setenv("BEADS_DOLT_AUTO_START", "0")
	t.Setenv("GT_ROOT", "")
	t.Setenv("BEADS_DIR", "")
	t.Setenv("PATH", t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	root := t.TempDir()
	stores := map[string]beadsdk.Storage{}
	for _, name := range []string{"hq", "rig"} {
		dir := filepath.Join(root, ".beads")
		if name == "rig" {
			dir = filepath.Join(root, "rig", "mayor", "rig", ".beads")
		}
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(map[string]any{"database": "dolt", "backend": "dolt", "dolt_mode": "server", "dolt_database": fmt.Sprintf("native_%s_%d", name, time.Now().UnixNano()), "dolt_server_host": host, "dolt_server_port": 3306})
		if err := os.WriteFile(filepath.Join(dir, "metadata.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		store, err := beadsdk.OpenFromConfig(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		stores[name] = store
		if err := store.SetConfig(ctx, "issue_prefix", name); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, ".beads", "routes.jsonl"), []byte("{\"prefix\":\"hq-\",\"path\":\".\"}\n{\"prefix\":\"rig-\",\"path\":\"rig/mayor/rig\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	hq, rig := stores["hq"], stores["rig"]
	work := &beadsdk.Issue{Title: "assigned work", Status: beadsdk.StatusOpen, IssueType: beadsdk.TypeTask, Assignee: "mayor", Priority: 2}
	if err := rig.CreateIssue(ctx, work, "test"); err != nil {
		t.Fatal(err)
	}
	cv := &beadsdk.Issue{Title: "native acceptance", Status: beadsdk.StatusOpen, IssueType: beadsdk.TypeTask, Labels: []string{"gt:convoy"}, Priority: 2}
	if err := hq.CreateIssue(ctx, cv, "test"); err != nil {
		t.Fatal(err)
	}
	if err := hq.AddDependency(ctx, &beadsdk.Dependency{IssueID: cv.ID, DependsOnID: "external:rig:" + work.ID, Type: beadsdk.DependencyType("tracks")}, "test"); err != nil {
		t.Fatal(err)
	}
	scan := func() convoy.Snapshot {
		t.Helper()
		result, err := convoy.Scan(ctx, root, stores, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, snapshot := range result {
			if snapshot.ID == cv.ID {
				return snapshot
			}
		}
		t.Fatalf("convoy absent: %+v", result)
		return convoy.Snapshot{}
	}
	if snapshot := scan(); snapshot.ReadyCount != 0 || snapshot.Complete {
		t.Fatalf("assigned work dispatchable: %+v", snapshot)
	}
	if err := convoy.Complete(ctx, root, stores, cv.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := hq.GetIssue(ctx, cv.ID); err != nil || got.Status != beadsdk.StatusOpen {
		t.Fatalf("premature close: %+v %v", got, err)
	}
	if err := rig.UpdateIssue(ctx, work.ID, map[string]interface{}{"assignee": ""}, "test"); err != nil {
		t.Fatal(err)
	}
	if snapshot := scan(); snapshot.ReadyCount != 1 || snapshot.ReadyIssues[0] != work.ID {
		t.Fatalf("unassigned work missing: %+v", snapshot)
	}
	if err := rig.CloseIssue(ctx, work.ID, "complete", "test", ""); err != nil {
		t.Fatal(err)
	}
	if snapshot := scan(); !snapshot.Complete || snapshot.ReadyCount != 0 {
		t.Fatalf("closed work unresolved: %+v", snapshot)
	}
	for i := 0; i < 2; i++ {
		if err := convoy.Complete(ctx, root, stores, cv.ID); err != nil {
			t.Fatal(err)
		}
	}
	got, err := hq.GetIssue(ctx, cv.ID)
	if err != nil || got.Status != beadsdk.StatusClosed || beads.ParseConvoyFields(&beads.Issue{Description: got.Description}).CompletionNotifiedAt == "" {
		t.Fatalf("completion not persisted: %+v %v", got, err)
	}
	mailbox := mail.NewMailboxWithBeadsDirAndStore("mayor", root, filepath.Join(root, ".beads"), hq)
	messages, err := mailbox.List()
	if err != nil || len(messages) != 1 {
		t.Fatalf("expected one completion notification: %d %v", len(messages), err)
	}
	data, err := os.ReadFile(filepath.Join(root, ".beads", "issues.jsonl"))
	if err != nil || !strings.Contains(string(data), cv.ID) || !strings.Contains(string(data), "completion_notified_at") {
		t.Fatalf("missing durable export: %s %v", data, err)
	}
}
