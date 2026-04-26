# raft-go — architecture

This is a living document. It describes the package layout, how the layers fit
together, and what state each piece is in. Update as the project evolves.

## Package layout

```
raft-go/
├── go.mod · README.md
│
├── cmd/
│   ├── raftd/          PUBLIC  daemon binary
│   └── raftcli/        PUBLIC  REPL client
│
├── rsm/                PUBLIC  Submit/StateMachine wrapper
├── transport/          PUBLIC  Peer interface + wire types
│   ├── memory/         PUBLIC  in-process mesh
│   └── grpc/           PUBLIC  gRPC + protobuf
├── storage/            PUBLIC  Store interface
│   └── memory/         PUBLIC  in-memory store
│
├── proto/              PUBLIC  protobuf source
│
├── internal/
│   ├── consensus/      PRIVATE Raft algorithm    (election + replication + persistence + snapshot tests)
│   ├── dlog/           PRIVATE debug logger
│   ├── lincheck/       PRIVATE porcupine adapter (linearizability under concurrent clients)
│   └── testcluster/    PRIVATE cluster harness   (in-memory mesh + fault injection + WithNodeFactory)
│
└── tools/dslogs/       DEV     log parser TUI
```

## Dependency diagram

```
    ┌──────────────────────────────────────────────┐
    │  Consumer (e.g. sharded-KV repo, separate)   │
    │  ─ implements rsm.StateMachine               │
    │  ─ calls rsm.RSM.Submit(req) → (Err, any)    │
    └───────────────────────┬──────────────────────┘
                            │
  ━━━━━━━━━━━━━━━━━━━━━━━━━━│━━━ raft-go public API ━━━━━━━━━━━━━━
                            ▼
                    ┌───────────────┐
                    │     rsm       │
                    │  (FSM wrap,   │
                    │   applier,    │
                    │   Submit,     │
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
 └─────────┬──────────┘                     └──────┬─────┘
           │                                       │
           ▼                                       ▼
   ┌──────┬──────┐                             ┌──────┐
   │memory│ gRPC │                             │memory│
   └──────┴──────┘                             └──────┘
 (on-disk store etc. plug in behind the same interfaces)

Side utilities:
  internal/dlog         tools/dslogs
  internal/testcluster  cmd/raftd, cmd/raftcli
  internal/lincheck
```

## Runtime flow — a committed command, happy path

```
user.Submit(cmd)                          ← called on the leader; followers return ErrWrongLeader
    │
    ▼
rsm.RSM.Submit(req) → blocks until commit
    │  wraps req in an Op{ID, Req} envelope
    ▼
consensus.Node.Start(op)                  ← leader appends Op to its log
    │
    ├── storage.Save(state, nil)          ← persist before acking
    │
    ├── transport.Peer.Call(transport.MethodAppendEntries, ...)  × N peers
    │       (peers mutate reply, reply returns)
    │
    │   once majority ack → commitIndex advances
    │
    └── sends ApplyMsg on applyCh
            │
            ▼
rsm applier loop reads ApplyMsg
    │  matches by (CommandIndex, Op.ID) against in-flight Submits
    │
    ├── StateMachine.DoOp(req) → response
    │
    └── opReqCh <- result                 ← unblocks Submit
            │
            ▼
Submit returns (rsm.OK, response)
```

## Layer responsibilities

### `transport` (public)
Three things in this package tree:

1. The `Peer` interface, with one method: `Call(ctx, method, args, reply) error`.
   Consensus uses this to send Raft RPCs to other peers. Implementations plug in
   behind it — `transport/memory` for tests, `transport/grpc` for production.
2. Hand-written Go structs for the wire messages the interface carries:
   `LogEntry`, `RequestVoteArgs/Reply`, `AppendEntriesArgs/Reply`,
   `InstallSnapshotArgs/Reply`. Any third-party transport implementation
   imports the same types.
3. `transport.Method*` constants — the canonical names consensus passes as the
   `method` argument to `Peer.Call`. Every transport switches on these instead
   of inline string literals.

### `transport/grpc` (public)
Production transport implementation. Builds on `proto/raft/v1/raft.proto` for
the wire format and exposes:

- `Cluster` — one node's view of a Raft cluster (a server plus the outbound
  peer handles); lifecycle is `Build` → `Start(node, addr)` → `Stop`.
- `Peer` — a single outbound `transport.Peer` over a gRPC client connection.
- `ConnectPeer(addr)` — constructs a `*Peer` (the underlying gRPC client is
  lazy and dials on first RPC).

`translate.go` round-trips between the hand-written `transport.*` structs and
the generated `raftpb.*` proto types; `LogEntry.Command` is gob-encoded into
bytes at the boundary so consensus stays proto-agnostic.

### `storage` (public)
A single interface, `Store`, with four methods: `Save`, `ReadState`,
`ReadSnapshot`, `StateSize`. Consensus uses this to persist and recover
durable state. Only `storage/memory` ships today; an on-disk implementation
would slot in behind the same interface.

### `internal/consensus` (private)
The Raft algorithm itself: election timers, AppendEntries, log matching,
commit index advancement, snapshot install. Private because we only support
one entrypoint for consumers: `rsm/`.

### `rsm` (public, main entrypoint)
The replicated-state-machine wrapper. Consumers implement the
`StateMachine` interface (`DoOp`, `Snapshot`, `Restore`) and call
`MakeRSM(peers, me, store, maxraftstate, sm)`. `Submit(req)` proposes
an op through consensus and blocks until it commits, returning
`(OK, result)` on success or `(ErrWrongLeader, nil)` if the local node
is no longer leader. Internally rsm wraps every request in an `Op`
envelope so the applier can match committed entries to in-flight
Submits and detect log overwrite by a new leader. Snapshots fire when
the persisted Raft state exceeds `maxraftstate`.

### `cmd/raftd` and `cmd/raftcli` (public)
The bundled demo: a fault-tolerant, linearizable KV server and its REPL client.

- `raftd` runs one node of a Raft cluster. gRPC for inter-peer RPCs, HTTP for
  client requests (`/put`, `/get`, `/keys`, `/status`). Followers reject writes
  with 503; the kvStore state machine and HTTP handler live in this package
  alongside `main`.
- `raftcli` is a stateful REPL that caches the leader after the first
  successful op, falls back to walking the peer list on 503, and renders
  cluster metadata in the prompt. Mirrors the etcd / TiKV / CockroachDB
  client-side leader-discovery pattern.

### `internal/dlog` (private)
Topic-filtered debug logger used during development. Enabled by setting
`DEBUG_TRACE=1` in the environment. The companion `tools/dslogs` parses
and colorizes its output.

### `internal/testcluster` (private)
Multi-node cluster harness used by consensus and rsm tests. Wires N nodes
to a shared `transport/memory` mesh and per-node `storage/memory` stores,
and provides the assertion helpers (`CheckOneLeader`, `One`, `Wait`,
`NCommitted`, `CheckTerms`, `CheckNoAgreement`, `KillPeer`,
`RestartPeer`, `Disconnect`, `Connect`, `SetReliable`,
`WithSnapshotInterval`, etc.). `WithNodeFactory` lets a higher-level
test harness (rsm) supply its own node constructor while still using the
cluster's mesh, partition, and lifecycle primitives.

### `internal/lincheck` (private)
Adapter around [Porcupine](https://github.com/anishathalye/porcupine) for
linearizability checking. Provides an `OpLog` that records concurrent
client operations with monotonic timestamps and a `Check` helper that
runs a Porcupine model against the recorded history. On a failure verdict
it dumps an interactive HTML visualization to `debug/porcupine/`.

### `tools/dslogs` (dev)
Go TUI for coloring and filtering Dlog output into per-peer columns.
