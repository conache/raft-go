package consensus_test

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// TestPersistenceRestartMajority drives a 5-peer cluster through five
// rounds of killing different subsets and bringing them back
// Exercises log replay plus leader election under repeated crash churn
func TestPersistenceRestartMajority(t *testing.T) {
	const (
		servers = 5
		rounds  = 5
	)
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	index := 1
	for range rounds {
		c.One(10+index, servers, true)
		index++

		leader1 := c.CheckOneLeader()

		// Kill two followers; the remaining 3 still form a quorum
		c.KillPeer((leader1 + 1) % servers)
		c.KillPeer((leader1 + 2) % servers)

		c.One(10+index, servers-2, true)
		index++

		// Kill the leader and the other two; cluster is now fully down
		c.KillPeer((leader1 + 0) % servers)
		c.KillPeer((leader1 + 3) % servers)
		c.KillPeer((leader1 + 4) % servers)

		// Restart two of the previously killed followers first
		c.RestartPeer((leader1 + 1) % servers)
		c.RestartPeer((leader1 + 2) % servers)
		time.Sleep(electionTimeout)

		// Restart a third, bringing the live set back to quorum
		c.RestartPeer((leader1 + 3) % servers)

		c.One(10+index, servers-2, true)
		index++

		// Restart the remaining two; cluster is whole again
		c.RestartPeer((leader1 + 4) % servers)
		c.RestartPeer((leader1 + 0) % servers)
	}

	c.One(1000, servers, true)
}

// TestPersistencePartitionedLeaderCrash partitions a leader off, commits on
// the remaining quorum, then crashes the partitioned leader
// Verifies the restarted leader correctly catches up from the surviving peer
func TestPersistencePartitionedLeaderCrash(t *testing.T) {
	const servers = 3
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.One(101, servers, true)

	// Partition one peer out
	leader := c.CheckOneLeader()
	c.Disconnect((leader + 2) % servers)

	c.One(102, 2, true)

	// Crash the leader and its follower, leaving only the isolated peer
	c.KillPeer((leader + 0) % servers)
	c.KillPeer((leader + 1) % servers)

	// Reconnect the isolated peer and restart one of the crashed peers;
	// they need each other to form a quorum
	c.Connect((leader + 2) % servers)
	c.RestartPeer((leader + 0) % servers)

	c.One(103, 2, true)

	c.RestartPeer((leader + 1) % servers)

	c.One(104, servers, true)
}

// TestFigure8Reliable reproduces the classic Raft Figure 8 scenario to
// confirm no committed entry is ever overwritten on a leader change
// Each iteration proposes on whoever claims leadership, kills them
// shortly after, and occasionally restarts a dead peer so quorum survives
func TestFigure8Reliable(t *testing.T) {
	const (
		servers = 5
		iters   = 1000
	)
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.One(rand.Int(), 1, true)

	nup := servers
	for range iters {
		leader := -1
		for i := range servers {
			if !c.IsRunning(i) {
				continue
			}
			if _, _, ok := c.StartOn(i, rand.Int()); ok {
				leader = i
			}
		}

		// Jitter: most iters a short pause, some a long one so elections fire
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Int63n(int64(electionTimeout/time.Millisecond)/2)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Int63n(13)) * time.Millisecond)
		}

		if leader != -1 {
			c.KillPeer(leader)
			nup--
		}

		// Keep at least 3 peers alive so a quorum is always reachable
		if nup < 3 {
			s := rand.Intn(servers)
			if !c.IsRunning(s) {
				c.RestartPeer(s)
				nup++
			}
		}
	}

	// Restart any remaining dead peers and confirm the cluster can still commit
	for i := range servers {
		if !c.IsRunning(i) {
			c.RestartPeer(i)
		}
	}

	c.One(rand.Int(), servers, true)
}

// TestAgreeUnreliable exercises concurrent proposals under a 10%
// per-call drop rate so AppendEntries retries and term changes drive log-backup paths;
// flipping back to reliable at the end forces a final full-cluster agreement to verify log convergence
func TestAgreeUnreliable(t *testing.T) {
	const (
		servers = 5
		iters   = 50
	)
	c := testcluster.New(t, servers)
	defer c.Shutdown()
	c.SetReliable(false)

	var wg sync.WaitGroup
	for i := 1; i < iters; i++ {
		for j := range 4 {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				c.One((100*i)+j, 1, true)
			}(i, j)
		}
		c.One(i, 1, true)
	}

	c.SetReliable(true)
	wg.Wait()

	c.One(100, servers, true)
}

// TestFigure8Unreliable is the Figure 8 scenario under a 10% drop rate
// No crashes here, just partitions, but the same leader-flip-with-uncommitted-tail
// dynamics exercise log-backup under dropped Append RPCs
func TestFigure8Unreliable(t *testing.T) {
	const (
		servers = 5
		iters   = 1000
	)
	c := testcluster.New(t, servers)
	defer c.Shutdown()
	c.SetReliable(false)

	c.One(rand.Intn(10_000), 1, true)

	nup := servers
	for range iters {
		leader := -1
		for i := range servers {
			cmd := rand.Intn(10_000)
			if _, _, ok := c.StartOn(i, cmd); ok && c.IsRunning(i) {
				// Only treat i as leader if it's still reachable;
				// StartOn on a disconnected peer can still succeed locally
				leader = i
			}
		}

		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Int63n(int64(electionTimeout/time.Millisecond)/2)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Int63n(13)) * time.Millisecond)
		}

		// Occasionally partition off the current leader
		if leader != -1 && rand.Intn(1000) < int(electionTimeout/time.Millisecond)/2 {
			c.Disconnect(leader)
			nup--
		}

		// Keep at least 3 peers reachable
		if nup < 3 {
			s := rand.Intn(servers)
			// Reconnect the first not-connected peer we find
			c.Connect(s)
			nup++
		}
	}

	for i := range servers {
		c.Connect(i)
	}

	c.One(rand.Intn(10_000), servers, true)
}

// TestChurnReliable and TestChurnUnreliable hammer the cluster with
// concurrent client goroutines while the main goroutine randomly
// disconnects, crashes, and restarts peers
// At the end, every value a client saw committed must still be present in
// the log, checked via a full replay of indices 1..lastIndex
func TestChurnReliable(t *testing.T)   { churn(t, true) }
func TestChurnUnreliable(t *testing.T) { churn(t, false) }

func churn(t *testing.T, reliable bool) {
	const (
		servers = 5
		clients = 3
		iters   = 20
	)
	c := testcluster.New(t, servers)
	defer c.Shutdown()
	c.SetReliable(reliable)

	var stop atomic.Bool
	results := make([]chan []int, clients)
	for i := range clients {
		results[i] = make(chan []int, 1)
		go churnClient(t, c, i, servers, &stop, results[i])
	}

	for range iters {
		if rand.Intn(1000) < 200 {
			c.Disconnect(rand.Intn(servers))
		}
		if rand.Intn(1000) < 500 {
			i := rand.Intn(servers)
			if !c.IsRunning(i) {
				c.RestartPeer(i)
			}
			c.Connect(i)
		}
		if rand.Intn(1000) < 200 {
			i := rand.Intn(servers)
			if c.IsRunning(i) {
				c.KillPeer(i)
			}
		}

		// Sleep less than an election timeout so peers keep churning
		// without letting things settle fully
		time.Sleep((electionTimeout * 7) / 10)
	}

	time.Sleep(electionTimeout)
	c.SetReliable(true)
	for i := range servers {
		if !c.IsRunning(i) {
			c.RestartPeer(i)
		}
		c.Connect(i)
	}

	stop.Store(true)

	values := []int{}
	for i := range clients {
		vv := <-results[i]
		if vv == nil {
			t.Fatalf("client %d failed", i)
		}
		values = append(values, vv...)
	}

	time.Sleep(electionTimeout)
	lastIndex := c.One(rand.Int(), servers, true)

	committed := make([]int, 0, lastIndex)
	for idx := 1; idx <= lastIndex; idx++ {
		v := c.Wait(idx, servers, -1)
		vi, ok := v.(int)
		if !ok {
			t.Fatalf("index %d: committed value %v is not an int", idx, v)
		}
		committed = append(committed, vi)
	}

	// Every value a client observed must still be in the final committed log
	for _, v := range values {
		found := false
		for _, c := range committed {
			if v == c {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("client-observed value %d is missing from the committed log", v)
		}
	}
}

// churnClient submits random ints as long as stop is false
// Records only values the cluster confirmed committed at the expected index
func churnClient(t *testing.T, c *testcluster.Cluster, me, servers int, stop *atomic.Bool, out chan<- []int) {
	var values []int
	defer func() { out <- values }()

	for !stop.Load() {
		x := rand.Int()
		index := -1
		ok := false
		for i := range servers {
			idx, _, ok1 := c.StartOn(i, x)
			if ok1 {
				index = idx
				ok = ok1
			}
		}
		if !ok {
			// No leader accepted; back off by a client-unique interval
			time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			continue
		}

		// Poll for commit with a bounded escalation so a slow leader
		// doesn't block the client forever
		for _, to := range []int{10, 20, 50, 100, 200} {
			n, cmd := c.NCommitted(index)
			if n > 0 {
				if xx, isInt := cmd.(int); isInt {
					if xx == x {
						values = append(values, x)
					}
				} else {
					t.Errorf("churnClient: committed value at %d is %T, want int", index, cmd)
					values = nil
					return
				}
				break
			}
			time.Sleep(time.Duration(to) * time.Millisecond)
		}
	}
}
