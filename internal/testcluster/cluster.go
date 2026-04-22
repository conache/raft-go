// Package testcluster wires N consensus.Node instances to a shared in-memory
// mesh and per-node in-memory stores for integration testing. It provides
// the assertion helpers consensus tests rely on.
package testcluster

import (
	"fmt"
	"sort"
	"strings"
	"sync"
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
}

// New builds and starts an n-node cluster. Caller must defer Shutdown.
func New(t *testing.T, n int) *Cluster {
	t.Helper()
	c := &Cluster{
		t:         t,
		n:         n,
		mesh:      memtransport.NewMesh(n),
		stores:    make([]*memstorage.Store, n),
		nodes:     make([]*consensus.Node, n),
		applyChs:  make([]chan consensus.ApplyMsg, n),
		connected: make([]bool, n),
		done:      make(chan struct{}),
		startedAt: time.Now(),
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
		go c.drainApply(i)
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
// The line is emitted via fmt.Println so it appears under the test name
// without the "file:line" prefix that t.Logf adds.
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
		"[%s] time=%s #peers=%d #RPCs=%d [%s] #dropped=%d #Ops=%d\n",
		status,
		elapsed.Round(time.Millisecond),
		c.n,
		s.Calls,
		formatByMethod(s.ByMethod),
		s.Dropped,
		0,
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

// drainApply pulls from a peer's apply channel to keep Raft unblocked.
// Replication tests will replace this with a tracker that records
// committed commands.
func (c *Cluster) drainApply(i int) {
	defer c.drainWG.Done()
	for {
		select {
		case <-c.applyChs[i]:
		case <-c.done:
			return
		}
	}
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

// CheckTerms asserts all connected peers agree on a single term and
// returns it. Fails the test on disagreement.
func (c *Cluster) CheckTerms() int {
	c.t.Helper()
	term := -1
	for i := range c.n {
		if !c.isConnected(i) {
			continue
		}
		t, _ := c.nodes[i].GetState()
		if term == -1 {
			term = t
		} else if t != term {
			c.t.Fatalf("peers disagree on term: %d vs %d", term, t)
		}
	}
	return term
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
