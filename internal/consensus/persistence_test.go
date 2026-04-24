package consensus_test

import (
	"testing"

	"github.com/conache/raft-go/internal/testcluster"
)

// TestPersistenceBasic walks the cluster through several crash/restart
// cycles and confirms the persisted state is correctly restored each time
// Covers: full-cluster restart, leader-only restart, a crash during a
// no-quorum-window that rejoins before the next crash
func TestPersistenceBasic(t *testing.T) {
	const servers = 3
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.One(11, servers, true)

	// Full-cluster restart: every peer dies and comes back from its store
	c.KillAll()
	c.RestartAll()

	c.One(12, servers, true)

	// Leader-only restart: the leader dies alone and rejoins
	leader1 := c.CheckOneLeader()
	c.KillPeer(leader1)
	c.RestartPeer(leader1)

	c.One(13, servers, true)

	// Kill the new leader, commit on the remaining quorum, restart the killed peer
	leader2 := c.CheckOneLeader()
	c.KillPeer(leader2)

	c.One(14, servers-1, true)

	c.RestartPeer(leader2)

	// Wait for leader2 to catch up to index 4 before the next kill so the
	// cluster has a consistent view when we move on
	c.Wait(4, servers, -1)

	// Kill a follower, commit without it, then restart it
	i3 := (c.CheckOneLeader() + 1) % servers
	c.KillPeer(i3)

	c.One(15, servers-1, true)

	c.RestartPeer(i3)

	c.One(16, servers, true)
}
