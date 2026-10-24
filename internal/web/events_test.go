package web

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/events"
)

// This trap reports process creation, not simulated status data.
func TestSSEDoesNotPollSubprocesses(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "spawned")
	trap := filepath.Join(dir, "gt-trap")
	if err := os.WriteFile(trap, []byte("#!/bin/sh\nprintf invoked >> '"+marker+"'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	h := NewAPIHandler(time.Second, time.Second, "test-token")
	h.workDir = dir
	h.gtPath = trap
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 2 {
		req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/events", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		line, err := bufio.NewReader(resp.Body).ReadString('\n')
		if err != nil || line != "event: connected\n" {
			t.Fatalf("initial event: %q %v", line, err)
		}
	}
	// The previous implementation launches three commands per tab after two seconds.
	time.Sleep(2300 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("SSE spawned a status subprocess: %v", err)
	}
}

func TestSSESharesFilesystemRefreshAndStopsWatching(t *testing.T) {
	dir := t.TempDir()
	h := NewAPIHandler(time.Second, time.Second, "test-token")
	h.workDir = dir
	var invalidations atomic.Int32
	h.dashboardEvents.invalidate = func() { invalidations.Add(1) }
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var bodies []io.ReadCloser
	received := make(chan string, 2)
	for range 2 {
		req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/events", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, resp.Body)
		defer resp.Body.Close()
		scanner := bufio.NewScanner(resp.Body)
		if !scanner.Scan() || scanner.Text() != "event: connected" {
			t.Fatal("missing connected event")
		}
		go func() {
			for scanner.Scan() {
				if strings.HasPrefix(scanner.Text(), "event: dashboard-update") {
					received <- scanner.Text()
					return
				}
			}
			received <- "stream ended"
		}()
	}
	if err := os.WriteFile(filepath.Join(dir, events.EventsFile), []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case event := <-received:
			if event != "event: dashboard-update" {
				t.Fatalf("unexpected update %q", event)
			}
		case <-ctx.Done():
			t.Fatal("filesystem change did not reach both clients")
		}
	}
	if n := invalidations.Load(); n != 1 {
		t.Fatalf("one file change caused %d shared refreshes", n)
	}
	for _, body := range bodies {
		body.Close()
	}
	deadline := time.Now().Add(time.Second)
	for {
		h.dashboardEvents.mu.Lock()
		stopped := h.dashboardEvents.active == nil
		h.dashboardEvents.mu.Unlock()
		if stopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("watcher remained active after all clients disconnected")
		}
		time.Sleep(time.Millisecond)
	}
}
