package acpworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

type Options struct {
	TownRoot, Session, Cwd, Socket, InitialPrompt string
	Command                                       []string
	Output                                        io.Writer
}
type journal struct {
	SessionID   string    `json:"session_id"`
	BootstrapID string    `json:"bootstrap_id,omitempty"`
	Receipts    []Receipt `json:"receipts"`
}
type worker struct {
	mu         sync.Mutex
	dispatchMu sync.Mutex
	status     Status
	journal    journal
	statePath  string
	wake       chan struct{}
	peer       *rpcPeer
	output     io.Writer
}

// Run owns one ACP process and exposes a private, durable control endpoint.
func Run(ctx context.Context, o Options) error {
	if o.TownRoot == "" || o.Session == "" || o.Cwd == "" || len(o.Command) == 0 {
		return fmt.Errorf("ACP worker requires town, session, cwd and command")
	}
	if o.Output == nil {
		o.Output = os.Stdout
	}
	expectedSocket := SocketPath(o.TownRoot, o.Session)
	if o.Socket != "" && o.Socket != expectedSocket {
		return fmt.Errorf("ACP socket does not match the town/session identity")
	}
	o.Socket = expectedSocket
	sum := sha256.Sum256([]byte(o.Session))
	statePath := filepath.Join(o.TownRoot, ".runtime", "acp", hex.EncodeToString(sum[:16])+".json")
	for _, dir := range []string{filepath.Dir(statePath), filepath.Dir(o.Socket)} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		if err := os.Chmod(dir, 0700); err != nil {
			return err
		}
	}
	lock := flock.New(statePath + ".lock")
	locked, err := lock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("ACP worker already owns %s", o.Session)
	}
	defer lock.Unlock()
	w := &worker{statePath: statePath, wake: make(chan struct{}, 1), output: o.Output, status: Status{State: "starting", PID: os.Getpid(), LastActivity: time.Now()}}
	if err := w.restore(); err != nil {
		return err
	}
	// Lock proves there is no other owner before replacing a stale endpoint.
	if err := os.Remove(o.Socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.Listen("unix", o.Socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(o.Socket)
	if err := os.Chmod(o.Socket, 0600); err != nil {
		return err
	}
	server := &http.Server{Handler: w.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	defer server.Close()
	go server.Serve(listener)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	child := exec.Command(o.Command[0], o.Command[1:]...)
	child.Dir = o.Cwd
	child.Stderr = o.Output
	configureProcess(child)
	stdin, err := child.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := child.StdoutPipe()
	if err != nil {
		return err
	}
	if err = child.Start(); err != nil {
		return err
	}
	childDone := make(chan error, 1)
	go func() { childDone <- child.Wait() }()
	defer func() {
		stdin.Close()
		stopProcess(child)
		select {
		case <-childDone:
		case <-time.After(5 * time.Second):
			killProcess(child)
			<-childDone
		}
	}()
	peer := newPeer(stdout, stdin, w.update)
	startup, stopStartup := context.WithTimeout(ctx, 180*time.Second)
	defer stopStartup()
	var init struct {
		AgentCapabilities struct {
			LoadSession bool `json:"loadSession"`
		} `json:"agentCapabilities"`
	}
	if err = peer.call(startup, "initialize", map[string]any{"protocolVersion": 1, "clientInfo": map[string]string{"name": "gastown", "version": "1"}, "clientCapabilities": map[string]any{}}, &init); err != nil {
		return err
	}
	method := "session/new"
	params := map[string]any{"cwd": o.Cwd, "mcpServers": []any{}}
	if w.journal.SessionID != "" {
		if !init.AgentCapabilities.LoadSession {
			return fmt.Errorf("agent cannot reload saved ACP session %s", w.journal.SessionID)
		}
		method = "session/load"
		params["sessionId"] = w.journal.SessionID
	}
	var result struct {
		SessionID string `json:"sessionId"`
	}
	if err = peer.call(startup, method, params, &result); err != nil {
		return err
	}
	if result.SessionID == "" {
		result.SessionID = w.journal.SessionID
	}
	if result.SessionID == "" {
		return fmt.Errorf("agent returned no ACP session ID")
	}
	w.mu.Lock()
	w.peer = peer
	w.journal.SessionID = result.SessionID
	w.status.SessionID = result.SessionID
	if o.InitialPrompt != "" {
		for i := range w.journal.Receipts {
			if w.journal.Receipts[i].ID == w.journal.BootstrapID && w.journal.Receipts[i].State == "queued" {
				w.journal.Receipts[i].State = "cancelled"
				w.journal.Receipts[i].Error = "Superseded by the recovery bootstrap."
			}
		}
		text := o.InitialPrompt
		if method == "session/load" {
			text = "[Gas Town recovery] Resume the assigned work. Inspect existing progress and reconcile uncertain side effects before retrying.\n" + text
		}
		w.pruneReceiptsLocked()
		initial := Receipt{ID: NewPromptID(), Message: text, State: "queued"}
		w.journal.BootstrapID = initial.ID
		w.journal.Receipts = append([]Receipt{initial}, w.journal.Receipts...)
	}
	err = w.saveLocked()
	if err == nil {
		w.status.Ready = true
		w.status.State = "idle"
	}
	w.mu.Unlock()
	if err != nil {
		return err
	}
	fmt.Fprintf(o.Output, "\n[ACP ready] %s session=%s\n", o.Session, result.SessionID)
	queueDone := make(chan error, 1)
	queueStopped := make(chan struct{})
	go func() { defer close(queueStopped); queueDone <- w.runQueue(ctx) }()
	defer func() {
		w.mu.Lock()
		w.status.Ready = false
		w.status.State = "stopping"
		w.mu.Unlock()
		cancel()
		<-queueStopped
	}()
	w.signal()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-queueDone:
		return err
	case <-peer.done:
		return fmt.Errorf("ACP agent disconnected")
	}
}

func (w *worker) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", w.serveStatus)
	mux.HandleFunc("POST /prompt", w.servePrompt)
	mux.HandleFunc("GET /receipt/{id}", w.serveReceipt)
	mux.HandleFunc("POST /cancel", w.serveCancel)
	return mux
}

func (w *worker) restore() error {
	if data, err := os.ReadFile(w.statePath); err == nil {
		if err = json.Unmarshal(data, &w.journal); err != nil {
			return fmt.Errorf("invalid ACP journal: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for i := range w.journal.Receipts {
		if w.journal.Receipts[i].State == "running" {
			w.journal.Receipts[i].State = "interrupted"
			w.journal.Receipts[i].Error = "Outcome unknown after worker exit. Reconcile before retrying."
		}
	}
	return w.saveLocked()
}

func (w *worker) saveLocked() error {
	data, err := json.Marshal(w.journal)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(w.statePath), ".journal-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(name, w.statePath); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(w.statePath))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func (w *worker) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (w *worker) enqueue(r Receipt) (Receipt, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.status.Ready {
		return Receipt{}, fmt.Errorf("ACP worker not ready")
	}
	if r.ID == "" || len(r.ID) > 128 || r.Message == "" || len(r.Message) > 65536 {
		return Receipt{}, fmt.Errorf("prompt requires ID (up to 128 bytes) and message (up to 65536 bytes)")
	}
	queued := 0
	for _, existing := range w.journal.Receipts {
		if existing.ID == r.ID {
			if existing.Message != r.Message {
				return Receipt{}, fmt.Errorf("prompt ID reused with different message")
			}
			return existing, nil
		}
		if existing.State == "queued" {
			queued++
		}
	}
	if queued >= 64 {
		return Receipt{}, fmt.Errorf("ACP queue full")
	}
	old := append([]Receipt(nil), w.journal.Receipts...)
	w.pruneReceiptsLocked()
	r.State = "queued"
	w.journal.Receipts = append(w.journal.Receipts, r)
	if err := w.saveLocked(); err != nil {
		w.journal.Receipts = old
		return Receipt{}, err
	}
	w.signal()
	return r, nil
}

// Retain recent receipts without dropping unfinished work.
func (w *worker) pruneReceiptsLocked() {
	for len(w.journal.Receipts) >= 1024 {
		removed := false
		for i, r := range w.journal.Receipts {
			if r.State != "queued" && r.State != "running" {
				w.journal.Receipts = append(w.journal.Receipts[:i:i], w.journal.Receipts[i+1:]...)
				removed = true
				break
			}
		}
		if !removed {
			break
		}
	}
}

func (w *worker) runQueue(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		w.dispatchMu.Lock()
		w.mu.Lock()
		index := -1
		for i, r := range w.journal.Receipts {
			if r.State == "queued" {
				index = i
				break
			}
		}
		if index < 0 {
			w.status.State = "idle"
			w.mu.Unlock()
			w.dispatchMu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-w.wake:
				continue
			}
		}
		r := w.journal.Receipts[index]
		w.journal.Receipts[index].State = "running"
		w.status.State = "running"
		w.status.LastActivity = time.Now()
		if err := w.saveLocked(); err != nil {
			w.mu.Unlock()
			w.dispatchMu.Unlock()
			return err
		}
		w.mu.Unlock()
		fmt.Fprintf(w.output, "\n[ACP prompt %s]\n", r.ID)
		var response struct {
			StopReason string `json:"stopReason"`
		}
		err := w.peer.callWithSent(ctx, "session/prompt", map[string]any{"sessionId": w.journal.SessionID, "prompt": []any{map[string]string{"type": "text", "text": r.Message}}}, &response, w.dispatchMu.Unlock)
		w.mu.Lock()
		// enqueue may compact old receipts while this prompt is running.
		for i := range w.journal.Receipts {
			if w.journal.Receipts[i].ID == r.ID {
				state := "completed"
				if response.StopReason == "cancelled" {
					state = "cancelled"
				}
				if err != nil {
					state = "interrupted"
					w.journal.Receipts[i].Error = err.Error()
					w.status.LastError = err.Error()
				}
				w.journal.Receipts[i].State = state
				break
			}
		}
		w.status.LastActivity = time.Now()
		saveErr := w.saveLocked()
		w.mu.Unlock()
		if saveErr != nil {
			return saveErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-w.peer.done:
			return fmt.Errorf("ACP disconnected during prompt")
		default:
		}
		fmt.Fprintf(w.output, "\n[ACP turn ended: %s]\n", response.StopReason)
	}
}
func (w *worker) update(raw json.RawMessage) {
	var event struct {
		Update struct {
			SessionUpdate string `json:"sessionUpdate"`
			Content       struct{ Type, Text string }
			Title         string
		} `json:"update"`
	}
	if json.Unmarshal(raw, &event) != nil {
		return
	}
	w.mu.Lock()
	w.status.LastActivity = time.Now()
	w.mu.Unlock()
	switch event.Update.SessionUpdate {
	case "agent_message_chunk":
		fmt.Fprint(w.output, cleanText(event.Update.Content.Text))
	case "tool_call":
		fmt.Fprintf(w.output, "\n[tool] %s\n", cleanText(event.Update.Title))
	}
}
func (w *worker) serveStatus(rw http.ResponseWriter, r *http.Request) {
	w.mu.Lock()
	s := w.status
	for _, r := range w.journal.Receipts {
		if r.State == "queued" {
			s.QueueDepth++
		}
	}
	w.mu.Unlock()
	writeJSON(rw, s)
}
func (w *worker) servePrompt(rw http.ResponseWriter, r *http.Request) {
	var receipt Receipt
	if err := json.NewDecoder(http.MaxBytesReader(rw, r.Body, 128<<10)).Decode(&receipt); err != nil {
		http.Error(rw, err.Error(), 400)
		return
	}
	result, err := w.enqueue(receipt)
	if err != nil {
		http.Error(rw, err.Error(), 409)
		return
	}
	writeJSON(rw, result)
}
func (w *worker) serveReceipt(rw http.ResponseWriter, r *http.Request) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, receipt := range w.journal.Receipts {
		if receipt.ID == r.PathValue("id") {
			writeJSON(rw, receipt)
			return
		}
	}
	http.NotFound(rw, r)
}
func (w *worker) serveCancel(rw http.ResponseWriter, r *http.Request) {
	w.dispatchMu.Lock()
	defer w.dispatchMu.Unlock()
	w.mu.Lock()
	peer, id := w.peer, w.status.SessionID
	active := w.status.State == "running"
	w.mu.Unlock()
	if peer == nil {
		http.Error(rw, "ACP worker not ready", 503)
		return
	}
	if active {
		if err := peer.notify("session/cancel", map[string]string{"sessionId": id}); err != nil {
			http.Error(rw, err.Error(), 502)
			return
		}
	}
	writeJSON(rw, map[string]bool{"cancel_requested": active})
}
func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

// Sanitize terminal output supplied by an untrusted agent.
func cleanText(text string) string {
	return strings.Map(func(r rune) rune {
		if r == 27 {
			return -1
		}
		return r
	}, text)
}
