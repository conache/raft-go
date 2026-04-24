// Package testcluster spins up an in-memory Raft cluster for tests
// and exposes the assertion helpers used by consensus tests
package testcluster

import (
	"bytes"
	"encoding/gob"
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

// CheckOneLeader's retry budget;
// total wait must exceed one election timeout + a vote round-trip
const (
	checkLeaderRetries  = 20
	checkLeaderInterval = 100 * time.Millisecond
)

const (
	// Budget for One() to find a leader and commit; 15s absorbs long
	// log-backup runs under -race without masking real liveness bugs
	oneOuterTimeout = 15 * time.Second
	// Budget for a single commit attempt after a leader accepts the proposal
	oneInnerTimeout = 2 * time.Second
	// Spacing between commit-state polls
	onePollInterval = 20 * time.Millisecond
)

// Cluster is an n-node Raft cluster backed by an in-memory mesh
type Cluster struct {
	t *testing.T
	// Fixed peer count
	n int
	// Shared in-memory transport for all peers
	mesh *memtransport.Mesh
	// Per-peer persistence, indexed by peer id
	stores []*memstorage.Store
	// Per-peer Raft node, indexed by peer id
	nodes []*consensus.Node
	// Per-peer apply channel, drained by trackApply
	applyChs []chan consensus.ApplyMsg
	// Cluster boot time, used for the Shutdown summary line
	startedAt time.Time

	// Guards connected and the drain-goroutine bookkeeping
	mu sync.Mutex
	// Per-peer connection state mirroring the mesh
	connected []bool
	// Closed by Shutdown to stop trackApply goroutines
	done chan struct{}
	// Waits for all trackApply goroutines to exit
	drainWG sync.WaitGroup

	// Guards every apply-state field below
	appliedMu sync.Mutex
	// Committed commands per (peer, log index)
	appliedCommands []map[int]any
	// FSM outputs per (peer, log index) when WithApply is set
	appliedResults []map[int]any
	// Per-commit callback driving appliedResults
	applyFn func(nodeIdx, index int, cmd any) any

	// Count of One/StartOn invocations, printed in the Shutdown summary
	opsCount atomic.Int64

	// Snapshot interval in commits; 0 disables snapshotting entirely
	snapshotInterval int
}

// N returns the cluster size
func (c *Cluster) N() int { return c.n }

// Option configures a Cluster at construction
type Option func(*Cluster)

// WithApply runs fn on every committed command and stores the result
// Retrievable via Result / AnyResult; nodes are distinguished by nodeIdx
func WithApply(fn func(nodeIdx, index int, cmd any) any) Option {
	return func(c *Cluster) {
		c.applyFn = fn
		c.appliedResults = make([]map[int]any, c.n)
		for i := range c.n {
			c.appliedResults[i] = make(map[int]any)
		}
	}
}

// WithSnapshotInterval enables snapshotting every `interval` commits
// After every `interval` applied entries, the harness encodes the applied
// command map and calls Node.Snapshot to trim the log
// Follower snapshots delivered via ApplyMsg.SnapshotValid are decoded and
// rehydrated into appliedCommands so NCommitted queries stay consistent
func WithSnapshotInterval(interval int) Option {
	return func(c *Cluster) { c.snapshotInterval = interval }
}

// New builds and starts an n-node cluster and returns it
// Caller must defer Shutdown
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

	// Apply options before any trackApply goroutine starts consuming commits
	for _, opt := range opts {
		opt(c)
	}

	// Create nodes first, register handlers after, so no peer can dispatch
	// an RPC to an unregistered target before the setup completes
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

// Shutdown kills every node, stops drain goroutines,
// and prints a one-line summary of cluster-level stats
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

// formatByMethod renders per-method counts as "MethodA=3 MethodB=7",
// keys sorted alphabetically for stable output
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

// trackApply drains peer i's apply channel and records committed commands
// If WithApply is set, also feeds each command through applyFn
// If WithSnapshotInterval is set, triggers Node.Snapshot at the interval
// and re-ingests SnapshotValid messages into appliedCommands
func (c *Cluster) trackApply(i int) {
	defer c.drainWG.Done()
	for {
		select {
		case msg := <-c.applyChs[i]:
			switch {
			case msg.CommandValid:
				c.appliedMu.Lock()
				c.appliedCommands[i][msg.CommandIndex] = msg.Command
				if c.applyFn != nil {
					c.appliedResults[i][msg.CommandIndex] = c.applyFn(i, msg.CommandIndex, msg.Command)
				}
				c.appliedMu.Unlock()
				if c.snapshotInterval > 0 && (msg.CommandIndex+1)%c.snapshotInterval == 0 {
					c.triggerSnapshot(i, msg.CommandIndex)
				}
			case msg.SnapshotValid && c.snapshotInterval > 0:
				if err := c.ingestSnapshot(i, msg.Snapshot, msg.SnapshotIndex); err != nil {
					c.t.Errorf("peer %d: ingest snapshot: %v", i, err)
				}
			}
		case <-c.done:
			return
		}
	}
}

// triggerSnapshot encodes peer i's applied commands up to commandIndex
// and asks Node.Snapshot to trim its log
func (c *Cluster) triggerSnapshot(i, commandIndex int) {
	data := c.encodeSnapshot(i, commandIndex)
	if node := c.nodeAt(i); node != nil {
		node.Snapshot(commandIndex, data)
	}
}

// encodeSnapshot gob-encodes (lastIncludedIndex, []any{cmds 0..lastIncludedIndex})
// from peer i's applied commands
func (c *Cluster) encodeSnapshot(i, lastIncludedIndex int) []byte {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	xlog := make([]any, lastIncludedIndex+1)
	for idx, cmd := range c.appliedCommands[i] {
		if idx <= lastIncludedIndex {
			xlog[idx] = cmd
		}
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(lastIncludedIndex); err != nil {
		c.t.Fatalf("encode snapshot index: %v", err)
	}
	if err := enc.Encode(xlog); err != nil {
		c.t.Fatalf("encode snapshot log: %v", err)
	}
	return buf.Bytes()
}

// ingestSnapshot decodes a snapshot produced by encodeSnapshot and replaces
// peer i's applied commands with it
// expectedIndex may be -1 to skip the cross-check used on restart
func (c *Cluster) ingestSnapshot(i int, data []byte, expectedIndex int) error {
	if len(data) == 0 {
		return nil
	}
	dec := gob.NewDecoder(bytes.NewReader(data))
	var lastIncluded int
	if err := dec.Decode(&lastIncluded); err != nil {
		return fmt.Errorf("decode index: %w", err)
	}
	if expectedIndex != -1 && expectedIndex != lastIncluded {
		return fmt.Errorf("snapshot lastIncluded=%d, expected=%d", lastIncluded, expectedIndex)
	}
	var xlog []any
	if err := dec.Decode(&xlog); err != nil {
		return fmt.Errorf("decode log: %w", err)
	}
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	rebuilt := make(map[int]any, len(xlog))
	for idx, cmd := range xlog {
		if cmd != nil {
			rebuilt[idx] = cmd
		}
	}
	c.appliedCommands[i] = rebuilt
	return nil
}

// Result returns the FSM output at (nodeIdx, index),
// or (nil, false) if that node hasn't applied the index yet
func (c *Cluster) Result(nodeIdx, index int) (any, bool) {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	if c.appliedResults == nil {
		return nil, false
	}
	r, ok := c.appliedResults[nodeIdx][index]
	return r, ok
}

// AnyResult returns the first FSM output for index across all nodes,
// or (nil, false) if no node has applied it yet
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

// CheckOneLeader confirms exactly one connected peer claims leadership
// Retries so in-flight elections have time to resolve
// Returns the leader's peer id; fails the test if no leader emerges
func (c *Cluster) CheckOneLeader() int {
	c.t.Helper()
	for range checkLeaderRetries {
		time.Sleep(checkLeaderInterval)

		leadersByTerm := map[int][]int{}
		for i := range c.n {
			node, live := c.liveNode(i)
			if !live {
				continue
			}
			term, isLeader := node.GetState()
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

// CheckNoLeader fails the test if any live peer claims leadership
func (c *Cluster) CheckNoLeader() {
	c.t.Helper()
	for i := range c.n {
		node, live := c.liveNode(i)
		if !live {
			continue
		}
		if _, isLeader := node.GetState(); isLeader {
			c.t.Fatalf("peer %d is leader but should not be", i)
		}
	}
}

// CheckTerms polls connected peers until they agree on a single term
// Returns the agreed term; fails if terms never converge
// Disagreement is often transient (heartbeat hasn't reached a rejoined peer yet)
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

// connectedTerms returns the current term reported by each live peer
func (c *Cluster) connectedTerms() []int {
	terms := make([]int, 0, c.n)
	for i := range c.n {
		node, live := c.liveNode(i)
		if !live {
			continue
		}
		peerTerm, _ := node.GetState()
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

// StartOn proposes cmd directly on peer i
// Lets tests target a specific (possibly disconnected) peer instead of
// letting One() auto-discover the current leader
// Returns Start's (index, term, isLeader) verbatim, or zero values if i is currently killed
func (c *Cluster) StartOn(i int, cmd any) (index, term int, isLeader bool) {
	c.opsCount.Add(1)
	n := c.nodeAt(i)
	if n == nil {
		return 0, 0, false
	}
	return n.Start(cmd)
}

// Term returns peer i's current term, or -1 if the peer is currently killed
func (c *Cluster) Term(i int) int {
	n := c.nodeAt(i)
	if n == nil {
		return -1
	}
	t, _ := n.GetState()
	return t
}

// RPCCount returns total RPCs delivered across the cluster since startup
func (c *Cluster) RPCCount() int64 { return c.mesh.Stats().Calls }

// BytesTotal returns cumulative gob-encoded bytes of RPC args + replies
func (c *Cluster) BytesTotal() int64 { return c.mesh.Stats().Bytes }

// MaxStateSize returns the largest persisted Raft state across all peers
// Snapshot bytes are not counted — use this to verify snapshots effectively
// cap log growth under WithSnapshotInterval
func (c *Cluster) MaxStateSize() int {
	max := 0
	for _, s := range c.stores {
		if sz := s.StateSize(); sz > max {
			max = sz
		}
	}
	return max
}

// Wait polls until expectedPeers have applied a command at index
// Returns the committed value, or -1 if startTerm is passed and any peer advances beyond it
// Fails the test if the index never reaches the expected peer count
func (c *Cluster) Wait(index, expectedPeers, startTerm int) any {
	c.t.Helper()
	to := 10 * time.Millisecond
	for range 30 {
		n, _ := c.NCommitted(index)
		if n >= expectedPeers {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm >= 0 {
			for i := range c.n {
				if c.Term(i) > startTerm {
					return -1
				}
			}
		}
	}
	n, cmd := c.NCommitted(index)
	if n < expectedPeers {
		c.t.Fatalf("Wait: only %d peers committed index %d, want %d", n, index, expectedPeers)
	}
	return cmd
}

// CheckNoAgreement fails the test if any peer has committed at the given index
func (c *Cluster) CheckNoAgreement(index int) {
	c.t.Helper()
	n, _ := c.NCommitted(index)
	if n > 0 {
		c.t.Fatalf("%d peer(s) committed at index %d, expected none", n, index)
	}
}

// NCommitted returns how many peers applied index, and the command value
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

// One submits cmd and waits for expectedPeers to commit it at one index
// Polls for a leader within oneOuterTimeout, then waits oneInnerTimeout per attempt
// retry=true restarts the leader lookup when a commit attempt expires
// Returns the committed log index
func (c *Cluster) One(cmd any, expectedPeers int, retry bool) int {
	c.t.Helper()
	c.opsCount.Add(1)

	deadline := time.Now().Add(oneOuterTimeout)
	for time.Now().Before(deadline) {
		idx := c.proposeOnLeader(cmd)
		if idx < 0 {
			// No leader right now; always transient, retry within the outer deadline
			time.Sleep(onePollInterval)
			continue
		}

		// Wait for expectedPeers to apply cmd at idx
		// If a new leader overwrites idx, bail out and retry
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

// proposeOnLeader tries every connected peer once and returns the log
// index a leader accepted cmd at, or -1 if no peer claimed leadership
func (c *Cluster) proposeOnLeader(cmd any) int {
	for i := range c.n {
		node, live := c.liveNode(i)
		if !live {
			continue
		}
		if idx, _, isLeader := node.Start(cmd); isLeader {
			return idx
		}
	}
	return -1
}

// Disconnect isolates peer i from the rest of the cluster
func (c *Cluster) Disconnect(i int) {
	c.mu.Lock()
	c.connected[i] = false
	c.mu.Unlock()
	c.mesh.Isolate(i)
}

// Connect reconnects peer i to every other peer
func (c *Cluster) Connect(i int) {
	c.mu.Lock()
	c.connected[i] = true
	c.mu.Unlock()
	c.mesh.Heal(i)
}

// KillPeer stops peer i's node
// The store is left in place so RestartPeer can rebuild from persisted state
// Connection state is preserved so the peer is reachable on restart unless Disconnect was called separately
func (c *Cluster) KillPeer(i int) {
	c.mu.Lock()
	n := c.nodes[i]
	c.nodes[i] = nil
	c.mu.Unlock()
	if n != nil {
		n.Kill()
	}
}

// RestartPeer rebuilds peer i from its persisted store
// In-memory state (commitIndex, lastApplied, votedFor resets) is discarded
// currentTerm, votedFor, and the log are rehydrated from the store
// If snapshotting is enabled, the persisted snapshot is re-ingested so
// appliedCommands stays consistent with the log's new base index
func (c *Cluster) RestartPeer(i int) {
	c.mu.Lock()
	old := c.nodes[i]
	c.nodes[i] = nil
	c.mu.Unlock()
	if old != nil {
		old.Kill()
	}
	if c.snapshotInterval > 0 {
		snap, err := c.stores[i].ReadSnapshot()
		if err != nil {
			c.t.Fatalf("peer %d: read snapshot on restart: %v", i, err)
		}
		if len(snap) > 0 {
			if err := c.ingestSnapshot(i, snap, -1); err != nil {
				c.t.Fatalf("peer %d: ingest snapshot on restart: %v", i, err)
			}
		}
	}
	node, err := consensus.Make(c.mesh.Peers(i), i, c.stores[i], c.applyChs[i])
	if err != nil {
		c.t.Fatalf("consensus.Make peer %d on restart: %v", i, err)
	}
	if err := c.mesh.Register(i, node); err != nil {
		c.t.Fatalf("mesh.Register peer %d on restart: %v", i, err)
	}
	c.mu.Lock()
	c.nodes[i] = node
	c.mu.Unlock()
}

// KillAll stops every peer without touching stores or connection state
func (c *Cluster) KillAll() {
	for i := range c.n {
		c.KillPeer(i)
	}
}

// RestartAll rebuilds every peer from its persisted store
func (c *Cluster) RestartAll() {
	for i := range c.n {
		c.RestartPeer(i)
	}
}

// IsRunning reports whether peer i currently has an active node
func (c *Cluster) IsRunning(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i] != nil
}

// defaultDropRate is the per-call drop probability used by SetReliable(false)
const defaultDropRate = 0.1

// SetDropRate sets the mesh's per-call drop probability to rate (in [0, 1])
// Use this when a test wants a drop rate other than the default
func (c *Cluster) SetDropRate(rate float64) { c.mesh.SetDropRate(rate) }

// SetReliable is a convenience wrapper around SetDropRate
// reliable=true disables drops; reliable=false installs defaultDropRate
func (c *Cluster) SetReliable(reliable bool) {
	if reliable {
		c.SetDropRate(0)
	} else {
		c.SetDropRate(defaultDropRate)
	}
}

// liveNode atomically returns peer i's node if it is both running and
// connected, else (nil, false)
// Callers must use this instead of isLive + c.nodes[i] to avoid a TOCTOU
// race with KillPeer/RestartPeer
func (c *Cluster) liveNode(i int) (*consensus.Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nodes[i]
	if n == nil || !c.connected[i] {
		return nil, false
	}
	return n, true
}

// nodeAt returns peer i's current node (or nil if killed) under c.mu
// Used where liveness isn't required (e.g. StartOn on a disconnected peer)
func (c *Cluster) nodeAt(i int) *consensus.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}
