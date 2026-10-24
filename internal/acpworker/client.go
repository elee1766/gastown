// Package acpworker runs persistent agents over ACP instead of terminal input.
package acpworker

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type Status struct {
	Ready        bool      `json:"ready"`
	State        string    `json:"state"`
	SessionID    string    `json:"session_id"`
	QueueDepth   int       `json:"queue_depth"`
	LastActivity time.Time `json:"last_activity"`
	PID          int       `json:"pid"`
	LastError    string    `json:"last_error,omitempty"`
}

type Receipt struct {
	ID      string `json:"id"`
	Message string `json:"message,omitempty"`
	State   string `json:"state"`
	Error   string `json:"error,omitempty"`
}

// SocketPath is short enough for Unix socket paths even in deep worktrees.
func SocketPath(townRoot, session string) string {
	root := os.Getenv("XDG_RUNTIME_DIR")
	if root == "" {
		root = filepath.Join(os.TempDir(), fmt.Sprintf("gastown-%d", os.Getuid()))
	}
	sum := sha256.Sum256([]byte(filepath.Clean(townRoot) + "\x00" + session))
	return filepath.Join(root, "gt-acp", hex.EncodeToString(sum[:16])+".sock")
}

func NewPromptID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

type Client struct{ http *http.Client }

func NewClient(socket string) *Client {
	return &Client{http: &http.Client{Transport: &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}}}
}
func (c *Client) request(ctx context.Context, method, path string, body, out any) error {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://acp"+path, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("ACP control: %s: %s", resp.Status, b)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
func (c *Client) Status(ctx context.Context) (s Status, err error) {
	err = c.request(ctx, "GET", "/status", nil, &s)
	return
}
func (c *Client) Prompt(ctx context.Context, id, message string) (r Receipt, err error) {
	err = c.request(ctx, "POST", "/prompt", Receipt{ID: id, Message: message}, &r)
	return
}
func (c *Client) Receipt(ctx context.Context, id string) (r Receipt, err error) {
	err = c.request(ctx, "GET", "/receipt/"+id, nil, &r)
	return
}
func (c *Client) Cancel(ctx context.Context) error {
	return c.request(ctx, "POST", "/cancel", nil, nil)
}
func (c *Client) WaitReady(ctx context.Context) error { return c.wait(ctx, false) }
func (c *Client) WaitIdle(ctx context.Context) error  { return c.wait(ctx, true) }
func (c *Client) wait(ctx context.Context, idle bool) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		s, err := c.Status(ctx)
		if err == nil {
			if s.Ready && (!idle || s.State == "idle" && s.QueueDepth == 0) {
				return nil
			}
			if s.State == "error" || s.State == "stopping" {
				return fmt.Errorf("ACP worker %s: %s", s.State, s.LastError)
			}
		}
		last = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for ACP worker: %w (last query: %v)", ctx.Err(), last)
		case <-ticker.C:
		}
	}
}
