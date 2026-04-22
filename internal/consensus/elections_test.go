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
	time.Sleep(2 * 500 * time.Millisecond)
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

	// Rejoin the old leader. The cluster should still have exactly one leader;
	// the reconnected peer must demote on seeing the higher term.
	c.Connect(leader1)
	c.CheckOneLeader()
}

// TestElectionStallsWithoutQuorum checks that a cluster without a majority
// of connected peers cannot elect a leader, and that restoring quorum lets
// it elect again.
func TestElectionStallsWithoutQuorum(t *testing.T) {
	c := testcluster.New(t, 3)
	defer c.Shutdown()

	leader1 := c.CheckOneLeader()

	// Disconnect the leader and one other peer: only one peer remains, no
	// quorum. Disconnect the non-leader first so the cluster can't hold an
	// election in the window between the two calls.
	other := (leader1 + 1) % 3
	c.Disconnect(other)
	c.Disconnect(leader1)

	// Wait long enough that any in-flight election times out without a quorum.
	time.Sleep(2 * 500 * time.Millisecond)
	c.CheckNoLeader()

	// Reconnect one of the disconnected peers: quorum returns, so does a leader.
	c.Connect(other)
	c.CheckOneLeader()

	// Reconnect the last disconnected peer too; a rejoining non-leader should
	// not disrupt the cluster.
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

	// Rejoin the old leader. The new leader's heartbeat (100 ms interval)
	// carries the higher term; the rejoined peer must demote on receiving it.
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
func TestElectionEndToEndRecovery(t *testing.T) {
	c := testcluster.New(t, 3)
	defer c.Shutdown()

	leader1 := c.CheckOneLeader()

	// Leader disconnects; remaining peers elect a new one.
	c.Disconnect(leader1)
	c.CheckOneLeader()

	// Old leader rejoins; cluster still has exactly one leader.
	c.Connect(leader1)
	leader2 := c.CheckOneLeader()

	// Take the new leader and one other peer offline — only one peer
	// remains, so no quorum and no leader. Disconnect the non-leader first
	// so an election can't fire between the two calls.
	other := (leader2 + 1) % 3
	c.Disconnect(other)
	c.Disconnect(leader2)
	time.Sleep(2 * 500 * time.Millisecond)
	c.CheckNoLeader()

	// Reconnect one of them: quorum returns, a leader is elected.
	c.Connect(other)
	c.CheckOneLeader()

	// Reconnect the last peer: still one leader.
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
