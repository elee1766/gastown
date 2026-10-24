package beads

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	beadsdk "github.com/steveyegge/beads"
	"gopkg.in/yaml.v3"
)

const sdkStoreLimit = 8

type sdkStoreEntry struct {
	store    beadsdk.Storage
	users    int
	lastUsed time.Time
}

var sdkStores = struct {
	sync.Mutex
	entries map[string]*sdkStoreEntry
}{entries: make(map[string]*sdkStoreEntry)}

// AcquireSDKStore leases a bounded, shared connection pool for an existing server database.
// The caller must release the lease. GT_BEADS_SDK=off retains the CLI path.
func AcquireSDKStore(ctx context.Context, beadsDir string) (beadsdk.Storage, func(), error) {
	if os.Getenv("GT_BEADS_SDK") == "off" {
		return nil, nil, fmt.Errorf("Beads SDK disabled")
	}
	dir, err := filepath.Abs(beadsDir)
	if err != nil || beadsDir == "" {
		return nil, nil, fmt.Errorf("invalid beads directory")
	}
	metadata, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		return nil, nil, err
	}
	var config struct {
		Mode string `json:"dolt_mode"`
	}
	if err := json.Unmarshal(metadata, &config); err != nil {
		return nil, nil, err
	}
	if config.Mode != "server" {
		return nil, nil, fmt.Errorf("automatic SDK access requires an existing server database")
	}
	settings, err := os.ReadFile(filepath.Join(dir, "config.yaml"))
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	var policy struct {
		Readonly bool   `yaml:"readonly"`
		NoDB     bool   `yaml:"no-db"`
		Actor    string `yaml:"actor"`
	}
	if err := yaml.Unmarshal(settings, &policy); err != nil {
		return nil, nil, err
	}
	if policy.Readonly || policy.NoDB || os.Getenv("BEADS_READONLY") != "" || os.Getenv("BEADS_NO_DB") != "" || (policy.Actor != "" && os.Getenv("BD_ACTOR") == "") {
		return nil, nil, fmt.Errorf("Beads configuration requires CLI policy handling")
	}
	// Configuration and credentials contribute only to a hash, never to logs.
	env := make([]string, 0)
	for _, item := range os.Environ() {
		if strings.HasPrefix(item, "BEADS_") || strings.HasPrefix(item, "BD_") || strings.HasPrefix(item, "DOLT_") || strings.HasPrefix(item, "GT_DOLT_") {
			env = append(env, item)
		}
	}
	sort.Strings(env)
	key := fmt.Sprintf("%s:%x", dir, sha256.Sum256(append(append(metadata, settings...), []byte(strings.Join(env, "\n"))...)))
	sdkStores.Lock()
	defer sdkStores.Unlock()
	entry := sdkStores.entries[key]
	if entry == nil {
		if len(sdkStores.entries) >= sdkStoreLimit {
			var oldestKey string
			var oldest *sdkStoreEntry
			for candidateKey, candidate := range sdkStores.entries {
				if candidate.users == 0 && (oldest == nil || candidate.lastUsed.Before(oldest.lastUsed)) {
					oldestKey, oldest = candidateKey, candidate
				}
			}
			if oldest == nil {
				return nil, nil, fmt.Errorf("Beads SDK pool is busy")
			}
			_ = oldest.store.Close()
			delete(sdkStores.entries, oldestKey)
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		// Unlike OpenFromConfig, this refuses to create a missing database.
		store, err := beadsdk.OpenBestAvailable(ctx, dir)
		if err != nil {
			return nil, nil, err
		}
		if provider, ok := store.(interface{ DB() *sql.DB }); ok {
			provider.DB().SetMaxOpenConns(2)
			provider.DB().SetMaxIdleConns(1)
		}
		entry = &sdkStoreEntry{store: store}
		sdkStores.entries[key] = entry
	}
	entry.users++
	var once sync.Once
	release := func() {
		once.Do(func() {
			sdkStores.Lock()
			entry.users--
			entry.lastUsed = time.Now()
			sdkStores.Unlock()
		})
	}
	return entry.store, release, nil
}

// CloseIdleSDKStores releases unused stores at a command boundary.
func CloseIdleSDKStores() {
	sdkStores.Lock()
	defer sdkStores.Unlock()
	for key, entry := range sdkStores.entries {
		if entry.users == 0 {
			_ = entry.store.Close()
			delete(sdkStores.entries, key)
		}
	}
}

func (b *Beads) sdkDependencyLocal(id string) bool {
	if strings.HasPrefix(id, "external:") {
		return true
	}
	dir := b.getResolvedBeadsDir()
	return filepath.Clean(ResolveBeadsDirForID(dir, id)) == filepath.Clean(dir)
}

func (b *Beads) agentStore(dir string) beadsdk.Storage {
	if filepath.Clean(dir) == filepath.Clean(b.getResolvedBeadsDir()) {
		return b.store
	}
	return nil
}

func (b *Beads) storeCloseCompatible(opts closeOptions, ids []string) (bool, error) {
	ctx, cancel := storeCtx()
	defer cancel()
	for _, id := range ids {
		issue, err := b.store.GetIssue(ctx, id)
		if err != nil {
			return false, err
		}
		if issue.IsTemplate {
			return false, fmt.Errorf("cannot close template %s", id)
		}
		if issue.IssueType == beadsdk.TypeEpic || string(issue.IssueType) == "gate" {
			return false, nil
		}
		if issue.Pinned && !opts.force {
			return false, fmt.Errorf("cannot close pinned issue %s without force", id)
		}
		deps, err := b.store.GetDependenciesWithMetadata(ctx, id)
		if err != nil {
			return false, err
		}
		for _, dep := range deps {
			if dep.DependencyType == beadsdk.DepParentChild {
				return false, nil
			}
		}
		if !opts.force {
			checker, ok := b.store.(interface {
				IsBlocked(context.Context, string) (bool, []string, error)
			})
			if !ok {
				return false, nil
			}
			blocked, _, err := checker.IsBlocked(ctx, id)
			if err != nil {
				return false, err
			}
			if blocked {
				return false, fmt.Errorf("cannot close blocked issue %s without force", id)
			}
		}
	}
	return true, nil
}

func (b *Beads) withSDK() (*Beads, func()) {
	if b.store != nil || b.isolated {
		return b, func() {}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, release, err := AcquireSDKStore(ctx, b.getResolvedBeadsDir())
	if err != nil {
		return b, func() {}
	}
	return &Beads{
		workDir: b.workDir, beadsDir: b.beadsDir, isolated: b.isolated,
		serverPort: b.serverPort, store: store, townRoot: b.getTownRoot(), noRoute: b.noRoute,
	}, release
}
