# raft-go

A Go implementation of the Raft consensus algorithm hand-crafted for learning (highly inspired by MIT 6.5840).

## Quick start

The fastest way to see it work end-to-end is the bundled demo: **raftd** (a
fault-tolerant, linearizable KV server) and **raftcli** (a REPL client).

### Local 3-node cluster

```sh
make run-local-0   # terminal 1
make run-local-1   # terminal 2
make run-local-2   # terminal 3
make cli           # terminal 4
```

```
[Cluster leader: peer 1 (term 2)] > put foo bar
OK
[Cluster leader: peer 1 (term 2)] > put baz qux
OK
[Cluster leader: peer 1 (term 2)] > keys
baz
foo
[Cluster leader: peer 1 (term 2)] > keys b*
baz
[Cluster leader: peer 1 (term 2)] > get foo
bar
[Cluster leader: peer 1 (term 2)] > status
http://localhost:9101: {"id":0,"term":2,"is_leader":false}
http://localhost:9102: {"id":1,"term":2,"is_leader":true}
http://localhost:9103: {"id":2,"term":2,"is_leader":false}
```

Kill `raft-1` (Ctrl-C in terminal 2) and watch a new leader emerge in the prompt.

### Docker

```sh
docker compose up -d --build
make cli-docker
docker compose down
```

## What raftd is

A fault-tolerant, linearizable KV server. Each instance runs one node of a Raft
cluster — HTTP for clients, gRPC for peer-to-peer. The HTTP API is intentionally
minimal:

| Endpoint | Behavior |
| --- | --- |
| `GET /put?key=K&value=V` | Replicate a PUT through Raft; 200 with `{value, leader_id, term}` on the leader, 503 elsewhere |
| `GET /get?key=K`         | Linearizable read of K; same response shape |
| `GET /keys?pattern=P`    | Redis-style glob over the keyspace; 200 with `{keys, leader_id, term}` |
| `GET /status`            | `{id, term, is_leader}` for this node |

Followers reject writes with 503. **raftcli** caches the leader after the first
successful op and falls back to walking the peer list when it hits a 503,
mirroring how etcd, TiKV, and CockroachDB clients work.

## Using raft-go as a library

Implement the `StateMachine` interface and hand it to `MakeRSM`. Each peer in
the cluster runs its own `RSM`; consensus replicates `Submit`'d commands and
calls `DoOp` on every replica in the same order.

```go
import (
    "github.com/conache/raft-go/rsm"
    memstorage "github.com/conache/raft-go/storage/memory"
    memtransport "github.com/conache/raft-go/transport/memory"
)

// 1. Implement StateMachine
type counter struct{ n int }
func (c *counter) DoOp(req any) any { c.n++; return c.n }
func (c *counter) Snapshot() []byte { /* gob-encode state */ }
func (c *counter) Restore([]byte)   { /* gob-decode state */ }

// 2. Construct one RSM per peer (here using the in-process mesh transport)
mesh := memtransport.NewMesh(3)
nodes := make([]*rsm.RSM, 3)
for i := range 3 {
    nodes[i] = rsm.MakeRSM(mesh.Peers(i), i, memstorage.New(), 1<<20, &counter{})
}
defer func() { for _, n := range nodes { n.Kill() } }()

// 3. Submit on the leader; followers return ErrWrongLeader
for _, n := range nodes {
    if status, resp := n.Submit("inc"); status == rsm.OK {
        fmt.Println("counter is now", resp)
        break
    }
}
```

`cmd/raftd` is the canonical worked example — a `kvStore` StateMachine wired
into the gRPC transport and an HTTP API.

## Architecture

```
        ┌──────────────────────────┐
        │   your StateMachine      │   ← you write this
        └──────────────┬───────────┘
                       │  DoOp / Snapshot / Restore
        ┌──────────────▼───────────┐
        │           rsm            │   ← public entrypoint: MakeRSM, Submit
        └──────────────┬───────────┘
                       │
        ┌──────────────▼───────────┐
        │     internal/consensus   │   ← Raft algorithm (private)
        └────┬────────────────┬────┘
             │                │
       ┌─────▼─────┐    ┌─────▼─────┐
       │ transport │    │  storage  │   ← public interfaces
       └──┬─────┬──┘    └─────┬─────┘
          │     │             │
       ┌──▼──┐ ┌▼───┐      ┌──▼───┐
       │ gRPC│ │mem │      │ mem  │   ← in-tree implementations
       └─────┘ └────┘      └──────┘
```

**What's exposed:**

- `rsm.RSM`, `rsm.StateMachine`, `rsm.MakeRSM`, `rsm.Submit` — the API you call
- `transport.Peer` and the wire types — implement this for a custom transport
- `storage.Store` — implement this for persistent on-disk state
- `transport/memory`, `transport/grpc`, `storage/memory` — in-tree implementations
- `proto/raft/v1/raft.proto` — the wire format if you want a non-Go peer

**What's internal** (you don't touch directly):

- `internal/consensus` — the Raft algorithm
- `internal/testcluster`, `internal/lincheck`, `internal/dlog` — test harness, Porcupine linearizability adapter, topic-tagged debug logger

For a deeper layer-by-layer breakdown see [`docs/architecture.md`](docs/architecture.md).

## Properties

- **Fault-tolerant:** survives up to ⌊N/2⌋ node failures
- **Linearizable:** reads and writes both go through Raft (CP in CAP terms)
- **No persistence yet:** the only `storage.Store` implementation is in-memory; surviving full-cluster restarts requires a durable backend (interface is in place)

## Repository layout

```
raft-go/
├── cmd/
│   ├── raftd/        the daemon binary
│   └── raftcli/      the REPL client
├── rsm/              Submit / StateMachine wrapper
├── transport/        Peer interface
│   ├── grpc/         production transport
│   └── memory/       in-process mesh used by tests
├── storage/          Store interface
│   └── memory/       in-memory implementation
├── internal/
│   ├── consensus/    Raft algorithm
│   ├── testcluster/  test harness
│   ├── lincheck/     Porcupine linearizability adapter
│   └── dlog/         topic-tagged debug logger
├── proto/            protobuf source
├── docs/             architecture notes
└── Makefile · Dockerfile · docker-compose.yml
```
