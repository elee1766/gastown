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

## Validation

`internal/beads/sdk_acceptance_test.go` exercises the normal constructors against a disposable real Dolt server with `bd` absent from `PATH`. It covers all 25 adapters, routed databases, agent-bead routing, CC/wisp mail, delivery acknowledgment, default filters, blocked closure and read-only configuration. Without SDK activation it fails at the first create because `bd` is unavailable.

Run on a remote runner, never against a live town database:

```sh
GT_TEST_SDK_HOST=<disposable-dolt-host> CGO_ENABLED=0 \
  go test ./internal/beads -run '^TestAutomaticSDK$' -count=1 -v
```

The test creates uniquely named databases on port 3306. Use a disposable server/container and remove it after the run. Regular affected-package tests do not require this variable.
