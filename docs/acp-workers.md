# ACP workers

An agent preset or runtime can opt into `"transport": "acp"`. Its `acp` configuration describes how to invoke the agent, not how to control a terminal.

```json
{
  "name": "jcode",
  "command": "/path/to/jcode-worker",
  "args": ["--no-update"],
  "transport": "acp",
  "acp": {"command": "acp"},
  "process_names": ["jcode"],
  "instructions_file": "AGENTS.md"
}
```

The resulting process is `gt acp-worker --initial-prompt <prompt> -- <command> <args> acp`. It speaks newline-delimited ACP JSON-RPC on the agent's standard input/output. It initializes the protocol, creates or loads a session, and sends prompts through `session/prompt`. It does not start an agent TUI.

## Lifecycle and control

The existing tmux session hosts the worker and displays its output. It remains a process host and log viewer, not an input or status protocol. There is no agent GUI to attach to. Use `gt nudge` to send work.

Gas Town records `GT_RUNTIME_TRANSPORT=acp` and `GT_ACP_SOCKET` in the session environment before startup. Readiness, liveness, idle waits, nudges and cancellation use that private Unix socket rather than terminal contents or key injection. A missing ACP endpoint does not fall back to keyboard delivery. The default empty transport retains existing terminal behavior.

The hidden diagnostic CLI works without an attached terminal:

```bash
export GT_ROOT=/path/to/town
gt acp-control status hq-mayor
gt acp-control prompt hq-mayor 'Inspect the assigned work'
gt acp-control receipt hq-mayor <receipt-id>
gt acp-control cancel hq-mayor
```

Cancellation requests cancellation of the current turn. It does not delete queued work or shut down the worker. Normal Gas Town stop/restart commands still own process lifecycle. On Linux, the worker isolates and terminates its owned process group, and the child receives SIGTERM if the worker dies.

## Delivery and recovery

A prompt is acknowledged only after it is saved in the town's `.runtime/acp/` journal. The worker runs one prompt at a time and accepts up to 64 queued messages. Reusing a receipt ID with the same message returns its existing state. A different message with that ID is rejected. The most recent retained receipts are bounded to approximately 1024 entries, so IDs are not a permanent deduplication ledger.

On restart, the worker uses `session/load` with its saved ACP session ID. It fails visibly if the agent cannot load the session, rather than silently replacing the conversation. A fresh startup/recovery instruction runs ahead of pending messages. Previously submitted turns become `interrupted`, with an unknown outcome, and are not blindly replayed. Inspect their effects before issuing another request. Receipt states are `queued`, `running`, `completed`, `cancelled` and `interrupted`.

The journal is scoped to the logical Gas Town session name. Retiring and later reusing that name retains its conversation and pending queue. For a deliberately new conversation, stop the worker and archive its journal before starting it again. Do not remove a journal while its worker is running.

## Safety and limitations

- Socket directories are private and sockets are mode 0600. Journals are written atomically and synced before acknowledgement.
- The ACP client advertises no filesystem or terminal capabilities. Unhandled permission requests are cancelled, not approved automatically.
- The agent command remains responsible for its backend and credentials. A jcode deployment should keep a private backend per worker when per-client environment forwarding is unavailable. Never replace it with a shared backend without verifying tool identity propagation.
- Legacy mail queues and their pollers remain supported. Their idle detection and final injection use ACP for ACP workers.
- Existing tmux lifecycle management remains required for managed workers. Standalone `gt acp-worker` can run without tmux, but it is not automatically registered with the daemon.
- Linux process cleanup and the deployment wrapper's stopped-process watchdog do not establish recovery from every possible deadlock or whole-process-group suspension.
