# raft-go — architecture

This is a living document. It describes the package layout, how the layers fit
together, and what state each piece is in. Update as the project evolves.

## Status legend

| Marker | Meaning |
| ------ | ------- |
| ✅     | Done and tested |
| 🔨     | In progress |
| ⬜     | Stub or not started |

## Package layout

```
raft-go/
├── go.mod · README.md                            ✅
│
├── rsm/                PUBLIC  hashicorp-style   ⬜ stub
├── transport/          PUBLIC  Peer interface + wire types   ✅
│   └── memory/         PUBLIC  in-process mesh   ✅
├── storage/            PUBLIC  Store interface   ✅
│   └── memory/         PUBLIC  in-memory store   ✅
├── server/             PUBLIC  wiring + CLI      ⬜ stub
│
├── internal/
│   ├── consensus/      PRIVATE Raft algorithm    ✅ (unsplit, no tests)
│   ├── dlog/           PRIVATE debug logger      ✅
│   └── testcluster/    PRIVATE cluster harness   ⬜
│
└── tools/dslogs.py     DEV     log parser        ✅
```

## Dependency diagram

```
    ┌──────────────────────────────────────────────┐
    │  Consumer (e.g. sharded-KV repo, separate)   │
    │  ─ implements rsm.StateMachine               │
    │  ─ calls rsm.RSM.Propose(cmd) → Future       │
    └───────────────────────┬──────────────────────┘
                            │
  ━━━━━━━━━━━━━━━━━━━━━━━━━━│━━━ raft-go public API ━━━━━━━━━━━━━━
                            ▼
                    ┌───────────────┐
                    │     rsm       │
                    │  (FSM wrap,   │
                    │   applier,    │
                    │   futures,    │
                    │   snap trig)  │
                    └───────┬───────┘
                            │
  ━━━━━━━━━━━━━━━━━━━━━━━━━━│━━━ raft-go internals ━━━━━━━━━━━━━━━
                            ▼
                  ┌─────────────────────┐
                  │ internal/consensus  │
                  │  (leader election,  │
                  │   log replication,  │
                  │   snapshot install, │
                  │   persistence)      │
                  └──┬─────────────┬───┘
                     │             │
     ┌───────────────┘             └──────────────┐
     ▼                                            ▼
 ┌────────────────────┐                     ┌────────────┐
 │ transport          │                     │ storage    │
 │ .Peer (interface)  │                     │ .Store     │
 │ + wire types       │                     │ (interface)│
 └──┬──────────────┬──┘                     └──┬──────┬──┘
    │              │                           │      │
    ▼              ▼                           ▼      ▼
  ┌──────┐      ┌────┐                      ┌──────┐┌──────┐
  │memory│      │grpc│                      │memory││ file │
  └──────┘      └────┘                      └──────┘└──────┘

Side utilities:
  internal/dlog         tools/dslogs.py
  internal/testcluster  server
```

## Runtime flow — a committed command, happy path

```
user.Propose(cmd)                        ← called on any node, forwarded to leader
    │
    ▼
rsm.RSM.Propose(cmd) → Future
    │
    ▼
consensus.Raft.Start(cmd)                ← leader appends to its log
    │
    ├── storage.Save(state, nil)         ← persist before acking
    │
    ├── transport.Peer.Call("Raft.AppendEntries", ...)  × N peers
    │       (peers mutate reply, reply returns)
    │
    │   once majority ack → commitIndex advances
    │
    └── sends ApplyMsg on applyCh
            │
            ▼
rsm applier loop reads ApplyMsg
    │
    ├── StateMachine.Apply(index, cmd) → response
    │
    └── Future.resolve(response)
            │
            ▼
user sees response via Future.Response()
```

## Layer responsibilities

### `transport` (public)
Two things in one package:

1. The `Peer` interface, with one method: `Call(ctx, method, args, reply) error`.
   Consensus uses this to send Raft RPCs to other peers. Implementations plug in
   behind it — `transport/memory` for tests, `transport/grpc` for production.
2. Hand-written Go structs for the wire messages the interface carries:
   `LogEntry`, `RequestVoteArgs/Reply`, `AppendEntriesArgs/Reply`,
   `InstallSnapshotArgs/Reply`. Any third-party transport implementation
   imports the same types.

### `storage` (public)
A single interface, `Store`, with four methods: `Save`, `ReadState`,
`ReadSnapshot`, `StateSize`. Consensus uses this to persist and recover
durable state. Implementations plug in — `storage/memory` for tests,
`storage/file` for production.

### `internal/consensus` (private)
The Raft algorithm itself: election timers, AppendEntries, log matching,
commit index advancement, snapshot install. Private because we only support
one entrypoint for consumers: `rsm/`.

### `rsm` (public, main entrypoint)
hashicorp/raft-style wrapper. Consumers implement the `StateMachine`
interface (`Apply`, `Snapshot`, `Restore`); rsm runs the applier loop on a
`consensus.Raft` instance, triggers snapshots, and returns `Future` handles
from `Propose`.

### `server` (public)
Glue + a runnable CLI binary that wires consensus + a transport + a store
+ a state machine into a real node.

### `internal/dlog` (private)
Topic-filtered debug logger. Ported verbatim from the MIT course work.

### `internal/testcluster` (private)
Multi-node cluster harness used by consensus tests. Wires N consensus
instances to a shared `transport/memory` mesh and per-node `storage/memory`
stores, and provides assertion helpers (checkOneLeader, one, wait,
checkLogs).

### `tools/dslogs.py` (dev)
Python TUI for coloring and filtering Dlog output into per-peer columns.
