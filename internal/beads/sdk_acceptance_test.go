package beads_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	beadsdk "github.com/steveyegge/beads"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/mail"
)

// TestAutomaticSDK exercises production constructors without a bd executable.
// GT_TEST_SDK_HOST must identify a disposable Dolt server, never a live town.
func TestAutomaticSDK(t *testing.T) {
	host := os.Getenv("GT_TEST_SDK_HOST")
	if host == "" {
		t.Skip("requires a disposable Dolt server via GT_TEST_SDK_HOST")
	}
	t.Setenv("BEADS_DOLT_SERVER_HOST", host)
	t.Setenv("BEADS_DOLT_SERVER_PORT", "3306")
	t.Setenv("BEADS_DOLT_AUTO_START", "0")
	t.Setenv("GT_BEADS_SDK", "on")
	t.Setenv("GT_ROOT", "")
	t.Setenv("BEADS_DIR", "")
	t.Setenv("BD_ACTOR", "sdk-acceptance")
	t.Setenv("PATH", t.TempDir())
	ctx := context.Background()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "mayor"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "mayor", "town.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, ".beads")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"database": "dolt", "backend": "dolt", "dolt_mode": "server", "dolt_database": fmt.Sprintf("sdk_%d", time.Now().UnixNano()), "dolt_server_host": host, "dolt_server_port": 3306})
	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	setup, err := beadsdk.OpenFromConfig(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()
	if err := setup.SetConfig(ctx, "issue_prefix", "sdk"); err != nil {
		t.Fatal(err)
	}
	if err := setup.SetConfig(ctx, "status.custom", "archived:done"); err != nil {
		t.Fatal(err)
	}
	b := beads.NewWithBeadsDir(root, dir)
	create := func(title string, ephemeral bool) *beads.Issue {
		t.Helper()
		issue, err := b.Create(beads.CreateOptions{Title: title, Priority: 2, Ephemeral: ephemeral})
		if err != nil {
			t.Fatal(err)
		}
		if issue.Status != "open" || issue.Type != "task" {
			t.Fatalf("create defaults: %+v", issue)
		}
		return issue
	}
	a := create("automatic SDK primary", false)
	blocker := create("automatic SDK blocker", false)
	wisp := create("automatic SDK wisp", true)
	if _, err := b.Show(a.ID); err != nil {
		t.Fatal(err)
	}
	if issues, err := b.ShowMultiple([]string{a.ID, blocker.ID}); err != nil || len(issues) != 2 {
		t.Fatalf("ShowMultiple: %d %v", len(issues), err)
	}
	if issues, err := b.List(beads.ListOptions{Status: "all", Priority: -1}); err != nil || len(issues) != 2 {
		t.Fatalf("durable list: %d %v", len(issues), err)
	}
	if issues, err := b.List(beads.ListOptions{Status: "all", Priority: -1, Ephemeral: true}); err != nil || len(issues) != 1 || issues[0].ID != wisp.ID {
		t.Fatalf("wisp list: %+v %v", issues, err)
	}
	if issues, err := b.ListIssueStatuses(beads.StatusOpen); err != nil || len(issues) != 2 {
		t.Fatalf("statuses: %d %v", len(issues), err)
	}
	if issues, err := b.Search(beads.SearchOptions{Query: "primary"}); err != nil || len(issues) != 1 {
		t.Fatalf("search: %d %v", len(issues), err)
	}
	title := "updated SDK primary"
	if err := b.Update(a.ID, beads.UpdateOptions{Title: &title, AddLabels: []string{"gt:test"}}); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Show(a.ID); err != nil || got.Title != title {
		t.Fatalf("updated title: %+v %v", got, err)
	}
	if _, err := b.Comments(a.ID); err != nil {
		t.Fatal(err)
	}
	inProgress := "in_progress"
	if err := b.Update(a.ID, beads.UpdateOptions{Status: &inProgress}); err != nil {
		t.Fatal(err)
	}
	if issues, err := b.Ready(); err != nil {
		t.Fatal(err)
	} else {
		for _, issue := range issues {
			if issue.ID == a.ID {
				t.Fatal("claimed issue returned as ready")
			}
		}
	}
	open := "open"
	if err := b.Update(a.ID, beads.UpdateOptions{Status: &open}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Ready(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ReadyForMol(a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ReadyWithType("test"); err != nil {
		t.Fatal(err)
	}
	if err := b.AddDependency(a.ID, blocker.ID); err != nil {
		t.Fatal(err)
	}
	if issues, err := b.Blocked(); err != nil || len(issues) != 1 {
		t.Fatalf("blocked: %d %v", len(issues), err)
	}
	if err := b.Close(a.ID); err == nil {
		t.Fatal("closed blocked issue")
	}
	if err := b.RemoveDependency(a.ID, blocker.ID); err != nil {
		t.Fatal(err)
	}
	if err := b.ReleaseWithReason(a.ID, "acceptance"); err != nil {
		t.Fatal(err)
	}
	if err := b.AddDelegation(&beads.Delegation{Parent: a.ID, Child: blocker.ID, DelegatedBy: "mayor", DelegatedTo: "worker"}); err != nil {
		t.Fatal(err)
	}
	if got, err := b.GetDelegation(blocker.ID); err != nil || got == nil || got.Parent != a.ID {
		t.Fatalf("delegation: %+v %v", got, err)
	}
	if err := b.RemoveDelegation(a.ID, blocker.ID); err != nil {
		t.Fatal(err)
	}
	if err := b.CloseWithReason("accepted", a.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Show(a.ID); err != nil || got.Status != "closed" {
		t.Fatalf("close: %+v %v", got, err)
	}
	if issues, err := b.Search(beads.SearchOptions{Query: "primary"}); err != nil || len(issues) != 0 {
		t.Fatalf("default search included closed issue: %d %v", len(issues), err)
	}
	archived := "archived"
	if err := b.Update(blocker.ID, beads.UpdateOptions{Status: &archived}); err != nil {
		t.Fatal(err)
	}
	if issues, err := b.List(beads.ListOptions{Priority: -1}); err != nil || len(issues) != 0 {
		t.Fatalf("default list included done issue: %d %v", len(issues), err)
	}

	// Mail must include primary, CC, and ephemeral messages through the same constructors.
	mailbox := mail.NewMailboxWithBeadsDir("sdk/worker", root, dir)
	for _, tc := range []struct {
		title, assignee string
		ephemeral       bool
		labels          []string
	}{
		{"primary mail", "sdk/worker", false, []string{"gt:message", "from:mayor", "delivery:pending"}},
		{"CC mail", "sdk/other", false, []string{"gt:message", "from:mayor", "cc:sdk/worker"}},
		{"ephemeral mail", "sdk/worker", true, []string{"gt:message", "from:mayor"}},
	} {
		msg, err := b.Create(beads.CreateOptions{Title: tc.title, Priority: 2, Labels: tc.labels, Ephemeral: tc.ephemeral})
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Update(msg.ID, beads.UpdateOptions{Assignee: &tc.assignee}); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := mailbox.List()
	if err != nil || len(messages) != 3 {
		t.Fatalf("mailbox primary/CC/wisps: %d %v", len(messages), err)
	}
	var messageID string
	for _, m := range messages {
		if m.Subject == "primary mail" {
			messageID = m.ID
		}
	}
	if messageID == "" {
		t.Fatal("primary message missing")
	}
	if _, err := mailbox.Get(messageID); err != nil {
		t.Fatal(err)
	}
	if err := mailbox.MarkReadOnly(messageID); err != nil {
		t.Fatal(err)
	}
	if got, err := mailbox.Get(messageID); err != nil || got.DeliveryState != "acked" || !got.Read {
		t.Fatalf("read and delivery acknowledgment: %+v %v", got, err)
	}
	if err := mailbox.MarkUnreadOnly(messageID); err != nil {
		t.Fatal(err)
	}
	if got, err := mailbox.Get(messageID); err != nil || got.Read {
		t.Fatalf("unread label: %+v %v", got, err)
	}
	if err := mailbox.MarkRead(messageID); err != nil {
		t.Fatal(err)
	}
	if err := mailbox.MarkUnread(messageID); err != nil {
		t.Fatal(err)
	}
	if got, err := mailbox.Get(messageID); err != nil || got == nil {
		t.Fatalf("reopened message: %+v %v", got, err)
	}
	// A routed operation must acquire the target database, not reuse the home pool.
	remoteDir := filepath.Join(root, "rig", "mayor", "rig", ".beads")
	if err := os.MkdirAll(remoteDir, 0700); err != nil {
		t.Fatal(err)
	}
	remoteData, _ := json.Marshal(map[string]any{"database": "dolt", "backend": "dolt", "dolt_mode": "server", "dolt_database": fmt.Sprintf("other_%d", time.Now().UnixNano()), "dolt_server_host": host, "dolt_server_port": 3306})
	if err := os.WriteFile(filepath.Join(remoteDir, "metadata.json"), remoteData, 0600); err != nil {
		t.Fatal(err)
	}
	remoteStore, err := beadsdk.OpenFromConfig(ctx, remoteDir)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteStore.Close()
	if err := remoteStore.SetConfig(ctx, "issue_prefix", "oth"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "routes.jsonl"), []byte("{\"prefix\":\"sdk-\",\"path\":\".\"}\n{\"prefix\":\"oth-\",\"path\":\"rig/mayor/rig\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	remoteB := beads.NewWithBeadsDir(filepath.Dir(remoteDir), remoteDir)
	remoteIssue, err := remoteB.Create(beads.CreateOptions{Title: "routed issue", Priority: 2})
	if err != nil {
		t.Fatal(err)
	}
	remoteTitle := "updated through home wrapper"
	if err := b.Update(remoteIssue.ID, beads.UpdateOptions{Title: &remoteTitle}); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Show(remoteIssue.ID); err != nil || got.Title != remoteTitle {
		t.Fatalf("routed issue: %+v %v", got, err)
	}
	if _, err := setup.GetIssue(ctx, remoteIssue.ID); err == nil {
		t.Fatal("routed mutation wrote to home database")
	}
	if err := b.ReleaseWithReason(remoteIssue.ID, "route check"); err != nil {
		t.Fatal(err)
	}
	if issues, err := b.ShowMultiple([]string{a.ID, remoteIssue.ID}); err != nil || len(issues) != 2 {
		t.Fatalf("routed batch: %d %v", len(issues), err)
	}
	if got, err := remoteB.ForAgentBead().Show(a.ID); err != nil || got.ID != a.ID {
		t.Fatalf("agent home route: %+v %v", got, err)
	}
	if err := b.Close(remoteIssue.ID); err != nil {
		t.Fatal(err)
	}

	// Mail can live in the home DB even when its prefix routes to another rig.
	homeMail := &beadsdk.Issue{ID: "oth-home-mail", Title: "home route fallback", Status: beadsdk.StatusOpen, IssueType: beadsdk.TypeTask, Priority: 2, Assignee: "sdk/worker", Labels: []string{"gt:message", "delivery:pending"}}
	if err := setup.CreateIssue(ctx, homeMail, "sdk-acceptance"); err != nil {
		t.Fatal(err)
	}
	if err := mailbox.MarkReadOnly(homeMail.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := mailbox.Get(homeMail.ID); err != nil || !got.Read || got.DeliveryState != "acked" {
		t.Fatalf("home mailbox fallback: %+v %v", got, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("readonly: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	forbidden := "must not be written"
	if err := b.Update(a.ID, beads.UpdateOptions{Title: &forbidden}); err == nil {
		t.Fatal("SDK bypassed read-only configuration")
	}
	if got, err := setup.GetIssue(ctx, a.ID); err != nil || got.Title == forbidden {
		t.Fatalf("read-only mutation: %+v %v", got, err)
	}
}
