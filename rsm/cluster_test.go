package rsm

import (
	"bytes"
	"encoding/gob"
	"log"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/conache/raft-go/internal/consensus"
	"github.com/conache/raft-go/internal/testcluster"
	"github.com/conache/raft-go/storage"
	"github.com/conache/raft-go/transport"
)

const (
	nServers = 3
	// nSeconds bounds the time onePartition spends searching for a leader
	// that can commit and apply the op
	nSeconds = 10
)

// Counter operations carried through the log
type (
	Inc     struct{}
	IncRep  struct{ N int }
	Null    struct{}
	NullRep struct{}
	Dec     struct{}
)

func init() {
	gob.Register(Op{})
	gob.Register(Inc{})
	gob.Register(IncRep{})
	gob.Register(Null{})
	gob.Register(NullRep{})
	gob.Register(Dec{})
}

// rsmSrv hosts an RSM and its in-process counter state machine for one peer.
// Mirrors the role of the MIT-style server type used in the original tests
type rsmSrv struct {
	me  int
	rsm *RSM

	mu      sync.Mutex
	counter int
}

func (s *rsmSrv) DoOp(req any) any {
	switch req.(type) {
	case Inc:
		s.mu.Lock()
		s.counter += 1
		c := s.counter
		s.mu.Unlock()
		return &IncRep{N: c}

	case Null:
		return &NullRep{}

	case Dec:
		s.mu.Lock()
		s.counter -= 1
		s.mu.Unlock()
		return nil

	default:
		log.Fatalf("rsmSrv.DoOp: unexpected request type %T", req)
	}
	return nil
}

func (s *rsmSrv) Snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s.counter); err != nil {
		log.Fatalf("rsmSrv.Snapshot: encode: %v", err)
	}
	return buf.Bytes()
}

func (s *rsmSrv) Restore(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&s.counter); err != nil {
		log.Fatalf("rsmSrv.Restore: decode: %v", err)
	}
}

// Test wires N rsmSrv replicas to a testcluster.Cluster running in
// node-factory mode, so the cluster owns mesh / stores / partition / kill
// / restart while the RSMs own their apply channels and appliers
type Test struct {
	*testcluster.Cluster

	t            *testing.T
	maxraftstate int
	srvs         []*rsmSrv

	mu     sync.Mutex
	leader int
}

// makeTest builds a Test cluster of n peers.
// maxraftstate=-1 disables snapshotting.
// Any non-negative value becomes the per-peer log size cap before an
// rsm-driven snapshot fires
func makeTest(t *testing.T, n int, maxraftstate int) *Test {
	t.Helper()

	ts := &Test{
		t:            t,
		maxraftstate: maxraftstate,
		srvs:         make([]*rsmSrv, n),
	}

	factory := func(idx int, peers []transport.Peer, store storage.Store) (*consensus.Node, func()) {
		s := &rsmSrv{me: idx}
		s.rsm = MakeRSM(peers, idx, store, maxraftstate, s)
		ts.srvs[idx] = s

		node, ok := s.rsm.Raft().(*consensus.Node)
		if !ok {
			t.Fatalf("peer %d: rsm.Raft() is not *consensus.Node", idx)
		}
		return node, s.rsm.Kill
	}

	ts.Cluster = testcluster.New(t, n, testcluster.WithNodeFactory(factory))
	return ts
}

func (ts *Test) cleanup() {
	ts.Shutdown()
}

// inPartition reports whether s is in p (or len(p)==0 means "no restriction")
func inPartition(s int, p []int) bool {
	return len(p) == 0 || slices.Contains(p, s)
}

// onePartition tries every running, connected peer in partition p in
// round-robin (starting from the cached leader) until one accepts and
// applies req.
// Retries for nSeconds; returns nil on timeout
func (ts *Test) onePartition(p []int, req any) any {
	t0 := time.Now()

	for time.Since(t0).Seconds() < nSeconds {
		ts.mu.Lock()
		idx := ts.leader
		ts.mu.Unlock()

		for range ts.N() {
			if ts.IsRunning(idx) && ts.IsConnected(idx) && inPartition(idx, p) {
				s := ts.srvs[idx]

				if s != nil && s.rsm != nil {
					err, rep := s.rsm.Submit(req)
					if err == OK {
						ts.mu.Lock()
						ts.leader = idx
						ts.mu.Unlock()
						return rep
					}
				}
			}
			idx = (idx + 1) % ts.N()
		}

		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func (ts *Test) oneInc() *IncRep {
	rep := ts.onePartition(nil, Inc{})
	if rep == nil {
		return nil
	}
	return rep.(*IncRep)
}

func (ts *Test) oneNull() *NullRep {
	rep := ts.onePartition(nil, Null{})
	if rep == nil {
		return nil
	}
	return rep.(*NullRep)
}

// countValue returns how many peer FSMs currently report counter==v
func (ts *Test) countValue(v int) int {
	n := 0

	for _, s := range ts.srvs {
		if s == nil {
			continue
		}
		s.mu.Lock()
		if s.counter == v {
			n++
		}
		s.mu.Unlock()
	}
	return n
}

// checkCounter polls until at least nsrv peers have counter==v, with backoff
// Fails the test on timeout
func (ts *Test) checkCounter(v, nsrv int) {
	ts.t.Helper()

	to := 10 * time.Millisecond
	got := 0
	for range 30 {
		got = ts.countValue(v)
		if got >= nsrv {
			return
		}

		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}

	ts.t.Fatalf("checkCounter: only %d peers have counter=%d, want %d", got, v, nsrv)
}

// findLeader returns (true, idx) of any connected, running peer reporting
// leadership; otherwise (false, 0)
func (ts *Test) findLeader() (bool, int) {
	for i := range ts.N() {
		if !ts.IsRunning(i) || !ts.IsConnected(i) {
			continue
		}

		s := ts.srvs[i]
		if s == nil || s.rsm == nil {
			continue
		}

		if _, isLeader := s.rsm.Raft().GetState(); isLeader {
			return true, i
		}
	}
	return false, 0
}

// disconnectLeader isolates the cached leader and returns its index
func (ts *Test) disconnectLeader() int {
	ts.mu.Lock()
	l := ts.leader
	ts.mu.Unlock()

	ts.Disconnect(l)
	return l
}

// makePartition returns (majority, minority) where the minority is the
// single-peer side containing l
func (ts *Test) makePartition(l int) (majority, minority []int) {
	minority = []int{l}

	for i := range ts.N() {
		if i != l {
			majority = append(majority, i)
		}
	}

	return majority, minority
}
