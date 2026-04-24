// Package testcluster wires N consensus.Node instances to a shared in-memory
// mesh and per-node in-memory stores for integration testing. It provides
// the assertion helpers consensus tests rely on.
package testcluster

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conache/raft-go/internal/consensus"
	memstorage "github.com/conache/raft-go/storage/memory"
	memtransport "github.com/conache/raft-go/transport/memory"
)

// CheckOneLeader's retry budget. checkLeaderRetries * checkLeaderInterval
// should comfortably exceed one election timeout plus a vote round-trip so
// that in-flight elections resolve before we give up.
const (
	checkLeaderRetries  = 20
	checkLeaderInterval = 100 * time.Millisecond
)

const (
	oneOuterTimeout = 10 * time.Second
	oneInnerTimeout = 2 * time.Second
	onePollInterval = 20 * time.Millisecond
)

// Cluster is an n-node Raft cluster backed by an in-memory mesh.
type Cluster struct {
	t         *testing.T
	n         int
	mesh      *memtransport.Mesh
	stores    []*memstorage.Store
	nodes     []*consensus.Node
	applyChs  []chan consensus.ApplyMsg
	startedAt time.Time

	mu        sync.Mutex
	connected []bool
	done      chan struct{}
	drainWG   sync.WaitGroup

	// All apply-time state is guarded by appliedMu. When applyFn is set
	// via WithApply, every committed command is fed through it and the
	// returned value stashed in appliedResults, letting linearizability
	// tests retrieve "what did the state machine return for this op".
	appliedMu       sync.Mutex
	appliedCommands []map[int]any
	appliedResults  []map[int]any
	applyFn         func(nodeIdx, index int, cmd any) any

	opsCount atomic.Int64
}

// N returns the cluster size.
func (c *Cluster) N() int { return c.n }

// Option configures a Cluster at construction.
type Option func(*Cluster)

// WithApply hooks a state-machine function into the apply pipeline. Every
// committed command is fed through fn on each node; the returned value is
// stashed per (nodeIdx, index) and retrievable via Result / AnyResult.
// The same function runs on every node — nodes are distinguished via the
// nodeIdx argument.
func WithApply(fn func(nodeIdx, index int, cmd any) any) Option {
	return func(c *Cluster) {
		c.applyFn = fn
		c.appliedResults = make([]map[int]any, c.n)
		for i := range c.n {
			c.appliedResults[i] = make(map[int]any)
		}
	}
}

// New builds and starts an n-node cluster. Caller must defer Shutdown.
func New(t *testing.T, n int, opts ...Option) *Cluster {
	t.Helper()
	c := &Cluster{
		t:               t,
		n:               n,
		mesh:            memtransport.NewMesh(n),
		stores:          make([]*memstorage.Store, n),
		nodes:           make([]*consensus.Node, n),
		applyChs:        make([]chan consensus.ApplyMsg, n),
		connected:       make([]bool, n),
		done:            make(chan struct{}),
		startedAt:       time.Now(),
		appliedCommands: make([]map[int]any, n),
	}
	for i := range n {
		c.appliedCommands[i] = make(map[int]any)
	}

	// Options run before any node is created so applyFn (if provided) is
	// set before trackApply goroutines start consuming commits.
	for _, opt := range opts {
		opt(c)
	}

	// Create all nodes first; register handlers after so no peer can issue
	// an RPC to an unregistered target. The ~250ms election timeout gives
	// us a wide window to complete this setup.
	for i := range n {
		c.stores[i] = memstorage.New()
		c.applyChs[i] = make(chan consensus.ApplyMsg, 256)
		node, err := consensus.Make(c.mesh.Peers(i), i, c.stores[i], c.applyChs[i])
		if err != nil {
			t.Fatalf("consensus.Make peer %d: %v", i, err)
		}
		c.nodes[i] = node
		c.connected[i] = true

		c.drainWG.Add(1)
		go c.trackApply(i)
	}
	for i := range n {
		if err := c.mesh.Register(i, c.nodes[i]); err != nil {
			t.Fatalf("mesh.Register peer %d: %v", i, err)
		}
	}
	return c
}

// Shutdown kills every node, stops the drain goroutines, and prints a
// single-line summary of cluster-level stats for post-test visibility.
func (c *Cluster) Shutdown() {
	elapsed := time.Since(c.startedAt)
	close(c.done)
	for i := range c.n {
		if c.nodes[i] != nil {
			c.nodes[i].Kill()
		}
	}
	c.drainWG.Wait()

	s := c.mesh.Stats()
	status := "PASSED"
	if c.t.Failed() {
		status = "FAILED"
	}
	fmt.Printf(
		"[%s] time=%s #peers=%d #RPCs=%d [%s] #dropped=%d #Ops=%d\n\n",
		status,
		elapsed.Round(time.Millisecond),
		c.n,
		s.Calls,
		formatByMethod(s.ByMethod),
		s.Dropped,
		c.opsCount.Load(),
	)
}

// formatByMethod renders the per-method counts deterministically:
// "MethodA=3 MethodB=7" with keys sorted alphabetically.
func formatByMethod(m map[string]int64) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(' ')
		}
		fmt.Fprintf(&sb, "%s=%d", k, m[k])
	}
	return sb.String()
}

// trackApply records every committed command each peer applies so tests
// can later ask "how many peers committed index N, and what value?". If a
// state-machine function was registered via WithApply, each committed
// command is fed through it and the result stored alongside.
// Snapshot messages are acknowledged (kept draining) but not yet handled
// — snapshot-aware tests will extend this.
func (c *Cluster) trackApply(i int) {
	defer c.drainWG.Done()
	for {
		select {
		case msg := <-c.applyChs[i]:
			if msg.CommandValid {
				c.appliedMu.Lock()
				c.appliedCommands[i][msg.CommandIndex] = msg.Command
				if c.applyFn != nil {
					c.appliedResults[i][msg.CommandIndex] = c.applyFn(i, msg.CommandIndex, msg.Command)
				}
				c.appliedMu.Unlock()
			}
		// NOTE: ignore SnapshotValid messages for now
		case <-c.done:
			return
		}
	}
}

// Result returns the state-machine output recorded for (nodeIdx, index),
// or (nil, false) if that node hasn't applied that index yet.
func (c *Cluster) Result(nodeIdx, index int) (any, bool) {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	if c.appliedResults == nil {
		return nil, false
	}
	r, ok := c.appliedResults[nodeIdx][index]
	return r, ok
}

// AnyResult returns the first recorded state-machine output for index
// across all nodes, or (nil, false) if no node has applied it yet.
func (c *Cluster) AnyResult(index int) (any, bool) {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	if c.appliedResults == nil {
		return nil, false
	}
	for i := range c.n {
		if r, ok := c.appliedResults[i][index]; ok {
			return r, true
		}
	}
	return nil, false
}

// CheckOneLeader confirms exactly one connected peer believes it is the
// leader, retrying up to checkLeaderRetries times at checkLeaderInterval
// spacing so that in-flight elections have time to resolve.
func (c *Cluster) CheckOneLeader() int {
	c.t.Helper()
	for range checkLeaderRetries {
		time.Sleep(checkLeaderInterval)

		leadersByTerm := map[int][]int{}
		for i := range c.n {
			if !c.isConnected(i) {
				continue
			}
			term, isLeader := c.nodes[i].GetState()
			if isLeader {
				leadersByTerm[term] = append(leadersByTerm[term], i)
			}
		}

		highestTerm := -1
		for term := range leadersByTerm {
			if term > highestTerm {
				highestTerm = term
			}
		}
		if highestTerm >= 0 {
			leaders := leadersByTerm[highestTerm]
			if len(leaders) > 1 {
				c.t.Fatalf("term %d has multiple leaders: %v", highestTerm, leaders)
			}
			return leaders[0]
		}
	}
	c.t.Fatalf("expected one leader, got none")
	return -1
}

// CheckNoLeader fails the test if any connected peer claims leadership.
func (c *Cluster) CheckNoLeader() {
	c.t.Helper()
	for i := range c.n {
		if !c.isConnected(i) {
			continue
		}
		if _, isLeader := c.nodes[i].GetState(); isLeader {
			c.t.Fatalf("peer %d is leader but should not be", i)
		}
	}
}

// CheckTerms polls connected peers until they agree on a single term,
// retrying up to checkLeaderRetries times at checkLeaderInterval spacing.
// Returns the agreed term. Fails the test if terms never converge —
// disagreement is often transient (a peer hasn't yet seen the current
// leader's heartbeat after a reconnect), so a hard single-shot check
// races with normal heartbeat propagation.
func (c *Cluster) CheckTerms() int {
	c.t.Helper()
	var lastSeen []int
	for range checkLeaderRetries {
		terms := c.connectedTerms()
		if len(terms) > 0 && allEqual(terms) {
			return terms[0]
		}
		lastSeen = terms
		time.Sleep(checkLeaderInterval)
	}
	c.t.Fatalf("peers never agreed on term (last seen: %v)", lastSeen)
	return -1
}

// connectedTerms returns the current term reported by each connected peer.
func (c *Cluster) connectedTerms() []int {
	terms := make([]int, 0, c.n)
	for i := range c.n {
		if !c.isConnected(i) {
			continue
		}
		peerTerm, _ := c.nodes[i].GetState()
		terms = append(terms, peerTerm)
	}
	return terms
}

func allEqual(xs []int) bool {
	for i := 1; i < len(xs); i++ {
		if xs[i] != xs[0] {
			return false
		}
	}
	return true
}

// NCommitted reports how many peers have applied the command at the given
// log index, and what that command's value is
func (c *Cluster) NCommitted(index int) (int, any) {
	c.t.Helper()
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()

	count := 0
	var committed any
	sawValue := false
	for i := range c.n {
		cmd, ok := c.appliedCommands[i][index]
		if !ok {
			continue
		}
		if sawValue && !reflect.DeepEqual(cmd, committed) {
			c.t.Fatalf(
				"divergent commit at index %d: peer %d has %v, peers earlier have %v",
				index, i, cmd, committed,
			)
		}
		committed = cmd
		sawValue = true
		count++
	}
	return count, committed
}

// One submits cmd to the leader and waits for at least expectedPeers to
// commit it. If retry is true, it re-tries on a new leader when the
// current one loses leadership or fails to commit within the inner
// timeout. Returns the log index where cmd was committed.
func (c *Cluster) One(cmd any, expectedPeers int, retry bool) int {
	c.t.Helper()
	c.opsCount.Add(1)

	deadline := time.Now().Add(oneOuterTimeout)
	for time.Now().Before(deadline) {
		idx := -1
		for i := range c.n {
			if !c.isConnected(i) {
				continue
			}
			proposedIdx, _, isLeader := c.nodes[i].Start(cmd)
			if isLeader {
				idx = proposedIdx
				break
			}
		}
		if idx < 0 {
			// No leader right now; either retry after a short wait or fail.
			if !retry {
				c.t.Fatalf("One: no leader available for cmd %v", cmd)
			}
			time.Sleep(onePollInterval)
			continue
		}

		// Wait for expectedPeers to apply cmd at idx. If a different leader
		// wins and overwrites idx with something else, break out and retry.
		innerDeadline := time.Now().Add(oneInnerTimeout)
		for time.Now().Before(innerDeadline) {
			count, committed := c.NCommitted(idx)
			if count >= expectedPeers && reflect.DeepEqual(committed, cmd) {
				return idx
			}
			time.Sleep(onePollInterval)
		}
		if !retry {
			c.t.Fatalf(
				"One: commit of %v never reached %d peers at idx %d",
				cmd, expectedPeers, idx,
			)
		}
	}
	c.t.Fatalf("One: deadline exceeded for cmd %v", cmd)
	return -1
}

// Disconnect isolates peer i from the rest of the cluster.
func (c *Cluster) Disconnect(i int) {
	c.mu.Lock()
	c.connected[i] = false
	c.mu.Unlock()
	c.mesh.Isolate(i)
}

// Connect reconnects peer i to every other peer.
func (c *Cluster) Connect(i int) {
	c.mu.Lock()
	c.connected[i] = true
	c.mu.Unlock()
	c.mesh.Heal(i)
}

func (c *Cluster) isConnected(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected[i]
}
