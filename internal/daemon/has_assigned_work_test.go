package daemon

import (
	"context"
	"path/filepath"
	"testing"

	beadsdk "github.com/steveyegge/beads"
)

func TestHasAssignedOpenWork_UsesRetainedRigStore(t *testing.T) {
	stores := newMissingAgentStore(t, t.TempDir(), true)
	d := &Daemon{
		config:      &Config{TownRoot: t.TempDir()},
		ctx:         context.Background(),
		beadsStores: stores,
		bdPath:      filepath.Join(t.TempDir(), "must-not-run-bd"),
	}
	if !d.hasAssignedOpenWork("myr", "myr/polecats/mycat") {
		t.Fatal("missing assigned work from retained store")
	}
	if d.hasAssignedOpenWork("myr", "myr/polecats/other") {
		t.Fatal("another agent's work must not prevent reaping")
	}
	issues, err := stores["myr"].SearchIssues(d.ctx, "", beadsdk.IssueFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, issue := range issues {
		if err := stores["myr"].CloseIssue(d.ctx, issue.ID, "done", "test", ""); err != nil {
			t.Fatal(err)
		}
	}
	if d.hasAssignedOpenWork("myr", "myr/polecats/mycat") {
		t.Fatal("closed work must not prevent reaping")
	}
	if !d.hasAssignedOpenWork("unavailable", "unavailable/polecats/mycat") {
		t.Fatal("unavailable store must fail safe")
	}
}
