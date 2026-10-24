package web

import (
	"context"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/steveyegge/gastown/internal/events"
)

const (
	dashboardEventInterval    = 10 * time.Second
	dashboardFallbackInterval = 60 * time.Second
)

// dashboardEvents shares one filesystem watcher across all connected tabs.
// It sends refresh hints, never runs status commands to discover changes.
type dashboardEvents struct {
	mu          sync.Mutex
	root        string
	subscribers map[chan struct{}]struct{}
	active      *dashboardWatch
	invalidate  func()
}

type dashboardWatch struct {
	cancel  context.CancelFunc
	watcher *fsnotify.Watcher
}

func (h *dashboardEvents) subscribe() (<-chan struct{}, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subscribers == nil {
		h.subscribers = make(map[chan struct{}]struct{})
	}
	ch := make(chan struct{}, 1)
	h.subscribers[ch] = struct{}{}
	if h.active == nil {
		ctx, cancel := context.WithCancel(context.Background())
		run := &dashboardWatch{cancel: cancel}
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Printf("dashboard: filesystem notifications unavailable: %v", err)
		} else {
			run.watcher = watcher
			// Missing optional directories are covered by the slow fallback.
			for _, dir := range []string{h.root, filepath.Join(h.root, "settings"), filepath.Join(h.root, "deacon")} {
				_ = watcher.Add(dir)
			}
		}
		h.active = run
		go h.watch(ctx, run)
	}
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subscribers, ch)
		if len(h.subscribers) == 0 && h.active != nil {
			h.active.cancel()
			h.active = nil
		}
	}
}

func (h *dashboardEvents) publish(run *dashboardWatch) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active != run {
		return
	}
	if h.invalidate != nil {
		h.invalidate()
	}
	for ch := range h.subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (h *dashboardEvents) watch(ctx context.Context, run *dashboardWatch) {
	var changes <-chan fsnotify.Event
	var errors <-chan error
	if run.watcher != nil {
		defer run.watcher.Close()
		changes = run.watcher.Events
		errors = run.watcher.Errors
	}
	fallback := time.NewTicker(dashboardFallbackInterval)
	defer fallback.Stop()
	var delayed *time.Timer
	var pending <-chan time.Time
	last := time.Now()
	defer func() {
		if delayed != nil {
			delayed.Stop()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-changes:
			if !ok {
				changes = nil
				continue
			}
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}
			name := filepath.Base(event.Name)
			if name != events.EventsFile && name != ".feed.jsonl" && name != "rigs.json" && name != "heartbeat.json" && filepath.Dir(event.Name) != filepath.Join(h.root, "settings") {
				continue
			}
			if pending == nil {
				wait := time.Until(last.Add(dashboardEventInterval))
				if wait < 0 {
					wait = 0
				}
				delayed = time.NewTimer(wait)
				pending = delayed.C
			}
		case _, ok := <-errors:
			if !ok {
				errors = nil
			}
			// A slow refresh still runs if the watcher overflows or fails.
		case <-pending:
			pending = nil
			last = time.Now()
			h.publish(run)
		case <-fallback.C:
			if time.Since(last) < dashboardEventInterval {
				continue
			}
			if delayed != nil {
				delayed.Stop()
				pending = nil
			}
			last = time.Now()
			h.publish(run)
		}
	}
}
