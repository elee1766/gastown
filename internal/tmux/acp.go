package tmux

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/steveyegge/gastown/internal/acpworker"
)

// ACP metadata is authoritative. Never fall back to typing into its viewer.
func (t *Tmux) acpClient(session string) (*acpworker.Client, bool, error) {
	transport, _, err := t.getEnvironmentOptional(session, "GT_RUNTIME_TRANSPORT")
	if err != nil {
		return nil, false, err
	}
	if transport != "acp" {
		return nil, false, nil
	}
	socket, _, err := t.getEnvironmentOptional(session, "GT_ACP_SOCKET")
	if err != nil {
		return nil, true, err
	}
	if socket == "" {
		return nil, true, fmt.Errorf("ACP session %s has no control socket", session)
	}
	return acpworker.NewClient(socket), true, nil
}

// IsACP reports whether the session runs an ACP worker.
func (t *Tmux) IsACP(session string) bool {
	_, acp, _ := t.acpClient(session)
	return acp
}

func (t *Tmux) acpAlive(session string) (bool, bool, error) {
	client, acp, err := t.acpClient(session)
	if !acp || err != nil {
		return false, acp, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status, err := client.Status(ctx)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
		return false, true, nil
	}
	if err != nil {
		return false, true, err
	}
	return status.State != "error" && status.State != "stopping", true, nil
}
func (t *Tmux) waitACP(session string, timeout time.Duration, idle bool) (bool, error) {
	client, acp, err := t.acpClient(session)
	if !acp || err != nil {
		return acp, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if idle {
		return true, client.WaitIdle(ctx)
	}
	return true, client.WaitReady(ctx)
}
func (t *Tmux) nudgeACP(session, message string) (bool, error) {
	client, acp, err := t.acpClient(session)
	if !acp || err != nil {
		return acp, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Reuse the same receipt ID when retrying an ambiguous transport failure.
	id := acpworker.NewPromptID()
	_, err = client.Prompt(ctx, id, message)
	if err != nil && ctx.Err() == nil {
		_, err = client.Prompt(ctx, id, message)
	}
	if err != nil {
		return true, fmt.Errorf("ACP delivery %s failed or has an unknown outcome; inspect its receipt before retrying: %w", id, err)
	}
	return true, nil
}
