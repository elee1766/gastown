package daemon

import (
	"fmt"
	"path/filepath"

	beadsdk "github.com/steveyegge/beads"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/mail"
)

// nativeStore borrows a daemon-owned store. Callers must not close it.
func (d *Daemon) nativeStore(name string) (beadsdk.Storage, error) {
	if d.convoyManager != nil {
		stores := d.convoyManager.storeSnapshot()
		if store := stores[name]; store != nil {
			return store, nil
		}
	} else if store := d.beadsStores[name]; store != nil {
		return store, nil
	}
	return nil, fmt.Errorf("beads store %q unavailable", name)
}

func (d *Daemon) sendNativeMail(to, subject, body string) error {
	store, err := d.nativeStore("hq")
	if err != nil {
		return err
	}
	router := mail.NewRouterWithTownRoot(d.config.TownRoot, d.config.TownRoot)
	router.SetStore(store)
	defer router.WaitPendingNotifications()
	return router.Send(mail.NewMessage("daemon", to, subject, body))
}

// hasNativeQueuedWork avoids launching the dispatcher when its queue is empty.
func (d *Daemon) hasNativeQueuedWork() (bool, error) {
	stores := d.beadsStores
	if d.convoyManager != nil {
		stores = d.convoyManager.storeSnapshot()
	}
	if len(stores) == 0 {
		return false, fmt.Errorf("beads stores unavailable")
	}
	open, ephemeral := beadsdk.StatusOpen, true
	for _, store := range stores {
		issues, err := store.SearchIssues(d.ctx, "", beadsdk.IssueFilter{Status: &open, Ephemeral: &ephemeral, Labels: []string{"gt:sling-context"}, Limit: 1})
		if err != nil {
			return false, err
		}
		if len(issues) > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (d *Daemon) nativeMailbox(identity string) (*mail.Mailbox, error) {
	store, err := d.nativeStore("hq")
	if err != nil {
		return nil, err
	}
	return mail.NewMailboxWithBeadsDirAndStore(identity, d.config.TownRoot, filepath.Join(d.config.TownRoot, ".beads"), store), nil
}

func (d *Daemon) nativeAgentIssue(id string) (*beadsdk.Issue, error) {
	store, err := d.nativeStore("hq")
	if err != nil {
		return nil, err
	}
	issue, err := store.GetIssue(d.ctx, id)
	if err != nil {
		return nil, err
	}
	if issue == nil {
		return nil, fmt.Errorf("agent bead not found: %s", id)
	}
	if issue.Labels == nil {
		issue.Labels, err = store.GetLabels(d.ctx, id)
		if err != nil {
			return nil, err
		}
	}
	if !beads.IsAgentBead(&beads.Issue{Type: string(issue.IssueType), Labels: issue.Labels}) {
		return nil, fmt.Errorf("bead %s is not an agent bead", id)
	}
	return issue, nil
}
