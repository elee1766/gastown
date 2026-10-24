package convoy

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
	beadsdk "github.com/steveyegge/beads"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/mail"
	"github.com/steveyegge/gastown/internal/tmux"
)

// Complete verifies current work, persists closure before notifying, and records
// successful notification. The per-convoy lock also serializes event/scan paths.
func Complete(ctx context.Context, townRoot string, stores map[string]beadsdk.Storage, id string) error {
	hq := stores["hq"]
	if hq == nil {
		return fmt.Errorf("hq store unavailable")
	}
	lock := flock.New(filepath.Join(townRoot, ".beads", "convoy-completion-"+filepath.Base(id)+".lock"))
	locked, err := lock.TryLockContext(ctx, 50*time.Millisecond)
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("convoy completion busy: %s", id)
	}
	defer lock.Unlock()
	issue, err := hq.GetIssue(ctx, id)
	if err != nil {
		return err
	}
	if issue == nil {
		return fmt.Errorf("convoy not found: %s", id)
	}
	isConvoy := string(issue.IssueType) == "convoy"
	for _, label := range issue.Labels {
		isConvoy = isConvoy || label == "gt:convoy"
	}
	if !isConvoy {
		return fmt.Errorf("issue %s is not a convoy", id)
	}
	if string(issue.Status) != "open" && string(issue.Status) != "closed" {
		return nil
	}
	snapshot, err := Inspect(ctx, hq, NewStoreResolver(townRoot, stores), issue)
	if err != nil {
		return err
	}
	if !snapshot.Complete {
		return nil
	}
	if string(issue.Status) != "closed" {
		if err := hq.CloseIssue(ctx, id, "All tracked issues completed", "convoy/"+id, ""); err != nil {
			return err
		}
	}
	if committer, ok := hq.(interface {
		CommitPending(context.Context, string) (bool, error)
	}); ok {
		if _, err := committer.CommitPending(ctx, "convoy/"+id); err != nil {
			return err
		}
	}
	path := filepath.Join(townRoot, ".beads", "issues.jsonl")
	if err := beads.ExportIssues(ctx, hq, path); err != nil {
		return fmt.Errorf("persisting convoy closure: %w", err)
	}
	fields := beads.ParseConvoyFields(&beads.Issue{Description: issue.Description})
	if fields == nil {
		fields = &beads.ConvoyFields{}
	}
	if fields.CompletionNotifiedAt != "" {
		return nil
	}
	router := mail.NewRouterWithTownRoot(townRoot, townRoot)
	router.SetStore(hq)
	defer router.WaitPendingNotifications()
	recipients := fields.NotificationAddresses()
	seen := make(map[string]bool)
	for _, addr := range recipients {
		seen[addr] = true
	}
	if !seen["mayor/"] {
		recipients = append(recipients, "mayor/")
	}
	for _, addr := range recipients {
		if seen["sent:"+addr] {
			continue
		}
		msg := mail.NewMessage("convoy/"+id, addr, "Convoy complete: "+issue.Title,
			fmt.Sprintf("Convoy %s has completed. All %d tracked issues are now closed.", id, snapshot.TrackedCount))
		msg.SuppressNotify = true
		if err := router.Send(msg); err != nil {
			return fmt.Errorf("notifying %s: %w", addr, err)
		}
		seen["sent:"+addr] = true
	}
	watchers := fields.NudgeNotificationAddresses()
	settings, err := config.LoadOrCreateTownSettings(config.TownSettingsPath(townRoot))
	if err == nil && settings.Convoy != nil && settings.Convoy.NotifyOnComplete {
		watchers = append(watchers, "mayor/")
	}
	for _, addr := range watchers {
		for _, session := range mail.AddressToSessionIDs(addr) {
			if err := tmux.NewTmux().NudgeSessionWithOpts(session, fmt.Sprintf("Convoy landed: %s (%s)", issue.Title, id), tmux.NudgeOpts{TownRoot: townRoot}); err != nil {
				return err
			}
		}
	}
	fields.CompletionNotifiedAt = time.Now().UTC().Format(time.RFC3339)
	description := beads.SetConvoyFields(&beads.Issue{Description: issue.Description}, fields)
	if err := hq.UpdateIssue(ctx, id, map[string]interface{}{"description": description}, "convoy/"+id); err != nil {
		return err
	}
	if committer, ok := hq.(interface {
		CommitPending(context.Context, string) (bool, error)
	}); ok {
		if _, err := committer.CommitPending(ctx, "convoy/"+id); err != nil {
			return err
		}
	}
	return beads.ExportIssues(ctx, hq, path)
}
