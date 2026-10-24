# Automatic Beads SDK access

Gas Town uses the pinned Beads Go SDK for its existing issue and mailbox adapters. Normal `Beads` and `Mailbox` constructors acquire a database connection when an SDK-backed method runs. Callers do not need to inject a store.

## Covered operations

| Area | SDK-backed methods |
| --- | --- |
| Issue reads | `List`, `ListIssueStatuses`, `Ready`, `ReadyForMol`, `ReadyWithType`, `Show`, local batches behind `ShowMultiple`, `Blocked`, `Search`, `Comments` |
| Issue writes | `Create`, `Update`, local close behind `Close`/`CloseWithReason`/`ForceCloseWithReason`, `ReleaseWithReason`, `AddDependency`, `RemoveDependency` |
| Delegation | `AddDelegation`, `RemoveDelegation` |
| Mail | directory list/get/close, mark read, acknowledge primary delivery, remove read marker, reopen |

This activates 18 issue/delegation adapters and seven mailbox adapters. It does not replace every `bd` invocation in Gas Town. Initialization, administrative commands, and other unsupported operations still use the CLI.

## Routing and connection ownership

- Automatic access requires an existing server-mode `.beads/metadata.json` database. It does not create missing databases or select embedded Dolt.
- Issue IDs are routed before acquiring a store. Multi-database batches acquire each target separately. Agent-bead operations select the town database.
- Mail operations use the requested directory. Routed message mutations retain the home-database fallback for messages whose prefix points elsewhere.
- Inbox reads include primary and CC recipients and both durable messages and wisps. Primary inbox entries include open/hooked messages; CC entries include open messages.
- Each process caches at most eight stores. Each automatic store has at most two SQL connections, one idle. Active leases cannot be evicted. Idle least-recently-used stores are evicted at capacity.
- Cache identity includes the directory, metadata, local YAML configuration and relevant environment. Secret-bearing configuration contributes only to a hash.
- Short-lived CLI commands close idle stores at exit. Injected stores remain caller-owned.

## Safety and fallbacks

Set `GT_BEADS_SDK=off` to disable automatic SDK acquisition. Existing explicit `SetStore` callers retain their chosen store.

Unavailable databases, malformed configuration, embedded mode, read-only/no-database policy, and a configured actor without a `BD_ACTOR` override retain CLI handling. Once a write has been attempted through the SDK, its failure is returned rather than retried through `bd`.

Local SDK closes reject templates, pinned issues without force, and open blockers. Epic/gate closes and parent-linked issues retain the CLI path so its gate validation and molecule auto-completion behavior are not lost. Cross-database dependency commands also retain CLI routing unless the dependency is already explicitly encoded as external.

Create defaults, query limits, active-status filtering, and durable/wisp selection match the corresponding adapters' CLI behavior.

## Rig identity and polling recovery

The daemon requires a durable `<prefix>-rig-<name>` identity record to verify that a rig is not parked or docked. Missing records fail closed and are reported separately from database errors. Failed lookups back off from 30 seconds to five minutes, with one warning per attempt. Successful status checks are not cached, so persisted park/dock changes remain visible on the next check. Local parked/docked overrides are checked before the retry cache.

`gt rig add`, including adoption, returns an error when identity creation fails. Registration may already have completed, so repair the registered rig rather than deleting or recreating its worktrees. Before repairing an existing identity, inspect local `.beads-wisp/config/<rig>.json` and preserve parked/docked intent and disabled merge queues. The rig type must be durable, not listed in `types.infra`.

The convoy event poller waits for the headquarters store before retaining per-rig stores. This avoids a partial startup that permanently skips convoy lookups after headquarters briefly fails to connect.

## Retained daemon APIs

The daemon borrows its long-lived HQ and rig stores for lifecycle inbox reads/deletes, agent state and hook checks, assigned-work checks, crash notifications, and patrol-step reads/closes. Unavailable stores fail safely instead of starting repeated CLI fallback processes. An empty scheduler queue is checked natively before launching the dispatcher.

Convoy event batches request one scan rather than one subprocess per issue. The scan reads scheduling contexts once per database and resolves raw dependency records against each owning store. The SDK's joined dependency API omits targets absent from the local database, so it must not be used to establish cross-rig completion. Missing work remains unresolved, never complete. Assigned work is left to witness recovery rather than repeatedly slung by the convoy poller. Failed dispatches back off from one to five minutes.

Completed convoys close through the SDK, export durable issue records atomically before notification, and write completion mail through the retained HQ store. A completion marker prevents repeat notification after a successful pass. The startup scan joins shutdown before stores are closed.

This is not a process-free town. Actual worker dispatch still invokes `gt sling`, and a nonempty scheduler queue still enters the existing dispatcher. Formula pouring, administrative commands, Git operations, and agent-authored `bd` commands remain subprocess boundaries. Native patrol closes still perform Beads dependency propagation; eliminating connection setup does not eliminate that database work.

`TestNativeConvoyLifecycle` runs with no CLI on PATH and covers assigned work, unassigned dispatch readiness, cross-store completion, durable export and repeat completion. Polecat health scenarios use disposable real SQL stores, including crash mail and protecting active/assigned work. Set `GT_TEST_SDK_HOST` to a disposable server on port 3306 for those tests.

## Validation

`internal/beads/sdk_acceptance_test.go` exercises the normal constructors against a disposable real Dolt server with `bd` absent from `PATH`. It covers all 25 adapters, routed databases, agent-bead routing, CC/wisp mail, delivery acknowledgment, default filters, blocked closure and read-only configuration. Without SDK activation it fails at the first create because `bd` is unavailable.

Run on a remote runner, never against a live town database:

```sh
GT_TEST_SDK_HOST=<disposable-dolt-host> CGO_ENABLED=0 \
  go test ./internal/beads -run '^TestAutomaticSDK$' -count=1 -v
```

The test creates uniquely named databases on port 3306. Use a disposable server/container and remove it after the run. Regular affected-package tests do not require this variable.
