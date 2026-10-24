package acpworker

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// The control API owns delivery durability. No ACP provider is mocked here.
func TestControlDeliverySurvivesRestartWithoutReplay(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "journal.json")
	w := &worker{statePath: state, wake: make(chan struct{}, 1), status: Status{Ready: true, State: "idle"}}
	socket := filepath.Join(dir, "control.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: w.handler()}
	defer server.Close()
	go server.Serve(listener)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := NewClient(socket)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			r, err := client.Prompt(ctx, "same-delivery", "inspect assigned work")
			if err != nil || r.State != "queued" {
				t.Errorf("delivery: %+v %v", r, err)
			}
		})
	}
	wg.Wait()
	if _, err = client.Prompt(ctx, "same-delivery", "different work"); err == nil {
		t.Fatal("accepted conflicting reuse of delivery ID")
	}
	data, err := os.ReadFile(state)
	if err != nil {
		t.Fatal(err)
	}
	var saved journal
	if err = json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Receipts) != 1 {
		t.Fatalf("persisted %d deliveries for one ID", len(saved.Receipts))
	}
	w.mu.Lock()
	w.journal.SessionID = "existing-session"
	w.journal.Receipts[0].State = "running"
	err = w.saveLocked()
	w.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	restored := &worker{statePath: state, wake: make(chan struct{}, 1), status: Status{Ready: true}}
	if err = restored.restore(); err != nil {
		t.Fatal(err)
	}
	if restored.journal.SessionID != "existing-session" {
		t.Fatal("lost session identity")
	}
	r, err := restored.enqueue(Receipt{ID: "same-delivery", Message: "inspect assigned work"})
	if err != nil {
		t.Fatal(err)
	}
	if r.State != "interrupted" || r.Error == "" {
		t.Fatalf("uncertain operation was queued for replay: %+v", r)
	}
	if len(restored.journal.Receipts) != 1 {
		t.Fatal("duplicated interrupted delivery")
	}
}

func TestDeliveryRejectsUnpersistedAndExcessWork(t *testing.T) {
	dir := t.TempDir()
	w := &worker{statePath: filepath.Join(dir, "missing", "journal.json"), wake: make(chan struct{}, 1), status: Status{Ready: true}}
	if _, err := w.enqueue(Receipt{ID: "retry", Message: "work"}); err == nil {
		t.Fatal("acknowledged a delivery without durable storage")
	}
	if len(w.journal.Receipts) != 0 {
		t.Fatal("failed save left an executable delivery")
	}
	w.statePath = filepath.Join(dir, "journal.json")
	for i := range 64 {
		if _, err := w.enqueue(Receipt{ID: string(rune('a' + i)), Message: "work"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.enqueue(Receipt{ID: "overflow", Message: "work"}); err == nil {
		t.Fatal("queue exceeded its bound")
	}
}
