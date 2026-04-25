package consensus_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/conache/raft-go/internal/testcluster"
)

// TestElectionEmergesOnBoot boots a 3-node cluster and confirms that a
// single leader emerges, that every peer agrees on the resulting term, and
// that the leader stays stable when no failures occur.
func TestElectionEmergesOnBoot(t *testing.T) {
	c := testcluster.New(t, 3)
	defer c.Shutdown()

	c.CheckOneLeader()

	// Give stragglers a moment to observe the election and record its term.
	time.Sleep(50 * time.Millisecond)
	term1 := c.CheckTerms()
	if term1 < 1 {
		t.Fatalf("term = %d, want >= 1", term1)
	}

	// Without failures, the leader and term should stay stable across a
	// couple of election-timeout windows.
	// We don't expect the term to change
	time.Sleep(2 * 1000 * time.Millisecond)
	term2 := c.CheckTerms()
	if term1 != term2 {
		t.Logf("warning: term changed %d -> %d with no failures", term1, term2)
	}

	c.CheckOneLeader()
}

// TestElectionReelectsAfterLeaderDisconnect verifies that when the current
// leader is partitioned away, a new leader emerges among the remaining
// connected peers, and that reconnecting the old leader leaves the cluster
// with exactly one leader (the old one must demote).
func TestElectionReelectsAfterLeaderDisconnect(t *testing.T) {
	c := testcluster.New(t, 3)
	defer c.Shutdown()

	leader1 := c.CheckOneLeader()

	// Partition the leader away; the remaining two peers should elect a new one.
	c.Disconnect(leader1)
	leader2 := c.CheckOneLeader()
	if leader2 == leader1 {
		t.Fatalf("new leader (%d) should differ from the disconnected one (%d)", leader2, leader1)
	}

	// Rejoin the old leader.
	// The cluster should still have exactly one leader; the reconnected
	// peer must demote on seeing the higher term.
	c.Connect(leader1)
	c.CheckOneLeader()
}

// TestElectionStallsWithoutQuorum checks that a cluster without a majority
// of connected peers cannot elect a leader, and that restoring quorum lets
// it elect again.
//
// A 5-peer cluster is used instead of 3 so that disconnecting 3 peers
// leaves only 2 connected — which is never a majority of 5 (quorum is 3),
// regardless of how elections race with the Disconnect calls.
// A 3-peer cluster here is racy: the window between the two required
// Disconnects is wide enough for a candidate to win 2 of 3 votes before
// the leader is isolated, producing a spurious leader that CheckNoLeader
// then catches.
func TestElectionStallsWithoutQuorum(t *testing.T) {
	const servers = 5
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	leader1 := c.CheckOneLeader()

	// Disconnect three peers including the leader — only 2 remain, below
	// quorum.
	// Disconnect non-leaders first so any election racing with this sequence
	// still has the leader available to reject stale requests.
	others := []int{(leader1 + 1) % servers, (leader1 + 2) % servers}
	for _, o := range others {
		c.Disconnect(o)
	}
	c.Disconnect(leader1)

	// Wait long enough that any in-flight election times out without a quorum.
	time.Sleep(2 * 1000 * time.Millisecond)
	c.CheckNoLeader()

	// Reconnect one: 3 connected, quorum returns, a leader can be elected.
	c.Connect(others[0])
	c.CheckOneLeader()

	// Reconnect the remaining peers; a rejoining non-leader must not disrupt
	// the cluster.
	c.Connect(others[1])
	c.Connect(leader1)
	c.CheckOneLeader()
}

// TestElectionOldLeaderDemotesAfterRejoin verifies the specific safety
// property that a deposed leader, once reachable again, observes the new
// leader's higher term and steps down — rather than silently believing it
// is still leader of its own (lower) term.
func TestElectionOldLeaderDemotesAfterRejoin(t *testing.T) {
	c := testcluster.New(t, 3)
	defer c.Shutdown()

	leader1 := c.CheckOneLeader()
	c.Disconnect(leader1)
	leader2 := c.CheckOneLeader()
	if leader2 == leader1 {
		t.Fatalf("new leader should differ from the disconnected one")
	}

	// Rejoin the old leader.
	// The new leader's heartbeat (100 ms interval) carries the higher term;
	// the rejoined peer must demote on receiving it.
	c.Connect(leader1)
	time.Sleep(200 * time.Millisecond)

	// If the old leader demoted, every peer agrees on the same term.
	c.CheckTerms()

	leader3 := c.CheckOneLeader()
	if leader3 == leader1 {
		t.Fatalf("reconnected peer %d should have demoted, but is still leader", leader1)
	}
}

// TestElectionEndToEndRecovery chains the full election lifecycle in a
// single scenario: initial election, leader disconnect, re-election, old
// leader rejoins, another disconnect takes the cluster below quorum, then
// peers rejoin one at a time and leadership is restored.
//
// Uses a 5-peer cluster for the no-quorum phase (see
// TestElectionStallsWithoutQuorum for the rationale — 3 peers with 2
// disconnects is structurally racy, 5 peers with 3 disconnects is not).
func TestElectionEndToEndRecovery(t *testing.T) {
	const servers = 5
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	leader1 := c.CheckOneLeader()

	// Leader disconnects; remaining 4 peers elect a new one (quorum 3 of 5).
	c.Disconnect(leader1)
	c.CheckOneLeader()

	// Old leader rejoins; cluster still has exactly one leader.
	c.Connect(leader1)
	leader2 := c.CheckOneLeader()

	// Take 3 peers offline including the current leader — only 2 remain,
	// below quorum of 5.
	// Disconnect non-leaders first to keep the sequential-Disconnect window
	// safe.
	others := []int{(leader2 + 1) % servers, (leader2 + 2) % servers}
	for _, o := range others {
		c.Disconnect(o)
	}
	c.Disconnect(leader2)
	time.Sleep(2 * 1000 * time.Millisecond)
	c.CheckNoLeader()

	// Reconnect one: 3 connected, quorum returns, a leader is elected.
	c.Connect(others[0])
	c.CheckOneLeader()

	// Reconnect the remaining peers: still one leader.
	c.Connect(others[1])
	c.Connect(leader2)
	c.CheckOneLeader()
}

// TestElectionStabilizesUnderChurn repeatedly partitions and heals three
// peers out of seven; a quorum always remains, so each round must stabilize
// to exactly one leader.
func TestElectionStabilizesUnderChurn(t *testing.T) {
	const servers = 7
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.CheckOneLeader()

	const iters = 10
	for range iters {
		i1 := rand.Intn(servers)
		i2 := rand.Intn(servers)
		i3 := rand.Intn(servers)
		c.Disconnect(i1)
		c.Disconnect(i2)
		c.Disconnect(i3)

		// With up to three peers down, at least four remain — quorum holds.
		c.CheckOneLeader()

		c.Connect(i1)
		c.Connect(i2)
		c.Connect(i3)
	}

	c.CheckOneLeader()
}
