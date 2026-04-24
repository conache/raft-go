package consensus_test

import (
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conache/raft-go/internal/lincheck"
	"github.com/conache/raft-go/internal/testcluster"
)

// sleep uint used by tests to wait for an election to succeed or to timeout
const electionTimeout = 1000 * time.Millisecond

// TestReplicationBasicAgreement proposes three commands in sequence on a
// healthy 3-node cluster and confirms each one commits at the expected
// index on every peer.
func TestReplicationBasicAgreement(t *testing.T) {
	c := testcluster.New(t, 3)
	defer c.Shutdown()

	// Make sure a leader exists before proposing anything — otherwise One()
	// would spin on "no leader" until the first election finishes anyway.
	c.CheckOneLeader()

	const commands = 3
	for i := range commands {
		cmd := 100 * (i + 1)
		expectedIdx := i + 1
		committedIdx := c.One(cmd, c.N(), false)
		if committedIdx != expectedIdx {
			t.Fatalf("command %d committed at index %d, want %d", cmd, committedIdx, expectedIdx)
		}
	}
}

// TestReplicationRegisterLinearizable runs three concurrent client
// goroutines against a 3-node cluster, each issuing 20 mixed Put/Get
// operations on a shared single-register state machine. The resulting
// history is fed to Porcupine, which verifies it is linearizable.
func TestReplicationRegisterLinearizable(t *testing.T) {
	const (
		peers        = 3
		clients      = 3
		opsPerClient = 20
		writeBias    = 7 // ~70% puts, ~30% gets
	)

	// Per-node register FSMs, allocated up front so the apply hook can
	// route each node's commits to its own FSM. Raft delivers applies in
	// log order, so the per-node FSMs converge to the same sequence.
	fsms := make([]*lincheck.RegisterFSM, peers)
	for i := range fsms {
		fsms[i] = lincheck.NewRegisterFSM()
	}

	c := testcluster.New(t, peers, testcluster.WithApply(
		func(nodeIdx, _ int, cmd any) any {
			return fsms[nodeIdx].Apply(cmd.(lincheck.RegOp))
		},
	))
	defer c.Shutdown()

	c.CheckOneLeader() // warm-up: make sure a leader exists

	log := lincheck.NewOpLog()
	var wg sync.WaitGroup
	for clientID := range clients {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id) + 1))

			for range opsPerClient {
				// decide operation
				var op lincheck.RegOp
				if rng.Intn(10) < writeBias {
					op = lincheck.RegOp{Kind: lincheck.RegOpPut, Value: rng.Intn(1000)}
				} else {
					op = lincheck.RegOp{Kind: lincheck.RegOpGet}
				}
				// append opperation to the porcupine log
				log.Op(id, op, func() any {
					idx := c.One(op, (c.N()/2)+1, true)
					r, _ := c.AnyResult(idx)
					return r
				})
			}
		}(clientID)

	}
	wg.Wait()

	lincheck.Check(t, lincheck.RegisterModel, log, 5*time.Second)
}

// TestReplicationRPCBytesBounded commits 10 * 5KB commands on a healthy
// 3-node cluster and asserts that the total RPC byte volume stays close
// to the theoretical lower bound of servers * sum(cmd sizes)
// this validates that each  command crosses the wire to each peer exactly once,
// with a fixed slack for gob-encoding overhead, heartbeats, and ack bookkeeping.
func TestReplicationRPCBytesBounded(t *testing.T) {
	const (
		servers  = 3
		iters    = 10
		cmdSize  = 5000
		overhead = 50_000
		firstCmd = 99
		startIdx = 2
	)
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.One(firstCmd, servers, false)
	bytes0 := c.BytesTotal()

	cmd := strings.Repeat("a", cmdSize)
	var sent int64
	for index := startIdx; index < iters+startIdx; index++ {
		got := c.One(cmd, servers, false)
		if got != index {
			t.Fatalf("committed at index %d, want %d", got, index)
		}
		sent += int64(len(cmd))
	}

	used := c.BytesTotal() - bytes0
	bound := int64(servers)*sent + overhead
	if used > bound {
		t.Fatalf("replicated %d bytes of payload in %d RPC bytes, want <= %d", sent, used, bound)
	}
}

// TestReplicationFollowerFailure walks the cluster through progressively worse failure scenarios:
//   - With one follower down a healthy leader and the remaining follower still form a quorum and can commits
//   - With two followers down the isolated leader can still Start() a command locally
//     but it must never commit, since the cluster is below quorum.
func TestReplicationFollowerFailure(t *testing.T) {
	const servers = 3
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.One(101, servers, false)

	// Disconnect one follower. Leader + remaining follower = 2/3 quorum,
	// so new proposals must still commit at N-1 peers.
	leader1 := c.CheckOneLeader()
	c.Disconnect((leader1 + 1) % servers)

	c.One(102, servers-1, false)
	c.One(103, servers-1, false)

	// Disconnect the remaining follower. The leader is now alone.
	leader2 := c.CheckOneLeader()
	c.Disconnect((leader2 + 1) % servers)
	c.Disconnect((leader2 + 2) % servers)

	// Leader still accepts Start() locally — it doesn't know yet that it's
	// lost quorum — but the entry must not commit.
	index, _, isLeader := c.StartOn(leader2, 104)
	if !isLeader {
		t.Fatalf("peer %d rejected Start but was still the leader", leader2)
	}
	if index != 4 {
		t.Fatalf("Start returned index %d, want 4", index)
	}

	time.Sleep(2 * electionTimeout)
	c.CheckNoAgreement(index)
}

// TestReplicationLeaderFailure disconnects the current leader twice in a row.
// After the first disconnect the remaining peers must elect a new leader and keep committing.
// After the second disconnect too many peers are gone for any quorum,
// so proposals sent to every peer must all fail to commit.
func TestReplicationLeaderFailure(t *testing.T) {
	const servers = 3
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.One(101, servers, false)

	// Disconnect the first leader; the rest must elect a new one.
	leader1 := c.CheckOneLeader()
	c.Disconnect(leader1)

	c.One(102, servers-1, false)
	c.One(103, servers-1, false)

	// Disconnect the new leader too. No peer can commit anything now.
	leader2 := c.CheckOneLeader()
	c.Disconnect(leader2)

	// Submit on every peer; none should be able to commit at index 4
	// because the cluster is below quorum.
	for i := range servers {
		c.StartOn(i, 104)
	}

	c.CheckNoAgreement(4)
}

// TestReplicationFailAgree verifies that a follower which was offline can
// rejoin and immediately participate in agreement again, including
// catching up on entries it missed while disconnected
func TestReplicationFailAgree(t *testing.T) {
	const servers = 3
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.One(101, servers, false)

	// Disconnect one follower
	// Quorum remains, proposals still commit
	leader := c.CheckOneLeader()
	follower := (leader + 1) % servers
	c.Disconnect(follower)

	c.One(102, servers-1, false)
	c.One(103, servers-1, false)
	c.One(104, servers-1, false)
	c.One(105, servers-1, false)

	// Rejoin the follower. The cluster must still be able to commit, and
	// the follower must catch up so new proposals commit at all servers
	c.Connect(follower)
	time.Sleep(electionTimeout)
	c.One(106, servers, true)
	c.One(107, servers, true)
}

// TestReplicationFailNoAgree puts 3 of 5 peers offline so the remaining
// 2 are below quorum and the leader's Start() must not commit
// When the disconnected peers rejoin (they may have elected their own leader in
// the meantime and advanced their logs), the cluster must converge back
// to a single leader and resume agreement
func TestReplicationFailNoAgree(t *testing.T) {
	const servers = 5
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.One(10, servers, false)

	// Disconnect 3 of 5 followers — leader is left with one peer, below
	// the quorum of 3.
	leader := c.CheckOneLeader()
	c.Disconnect((leader + 1) % servers)
	c.Disconnect((leader + 2) % servers)
	c.Disconnect((leader + 3) % servers)

	index, _, isLeader := c.StartOn(leader, 20)
	if !isLeader {
		t.Fatalf("peer %d rejected Start but was still the leader", leader)
	}
	if index != 2 {
		t.Fatalf("Start returned index %d, want 2", index)
	}

	if n, _ := c.NCommitted(index); n > 0 {
		t.Fatalf("%d peer(s) committed at index %d without a majority", n, index)
	}

	// Heal the partition
	// The formerly-disconnected majority may have
	// elected its own leader and committed at index 2 first, which would
	// overwrite the original leader's uncommitted entry.
	c.Connect((leader + 1) % servers)
	c.Connect((leader + 2) % servers)
	c.Connect((leader + 3) % servers)

	leader2 := c.CheckOneLeader()
	index2, _, ok := c.StartOn(leader2, 30)
	if !ok {
		t.Fatalf("peer %d rejected Start but was the leader", leader2)
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("Start returned index %d, want 2 or 3", index2)
	}

	c.One(1000, servers, true)
}

// TestReplicationConcurrentProposes fires five parallel Start() calls
// against the same leader during the same term and confirms every one of them commits
// If leadership flips mid-test (term advances), the attempt
// is rerun — up to 5 tries — since a term change invalidates the
// preconditions for the checker
func TestReplicationConcurrentProposes(t *testing.T) {
	const (
		servers = 3
		iters   = 5
		retries = 5
	)
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	for try := range retries {
		if try > 0 {
			time.Sleep(3 * time.Second)
		}

		leader := c.CheckOneLeader()
		_, term, ok := c.StartOn(leader, 1)
		if !ok {
			continue // leader stepped down before we could start
		}

		var wg sync.WaitGroup
		indices := make(chan int, iters)
		for ii := range iters {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				idx, t, ok := c.StartOn(leader, 100+i)
				if !ok || t != term {
					return
				}
				indices <- idx
			}(ii)
		}
		wg.Wait()
		close(indices)

		if termAdvanced(c, term) {
			continue
		}

		committedValues, termChanged := collectCommitted(c, indices, term)
		if termChanged {
			continue
		}

		// Confirm every value we proposed (100..104) shows up.
		for ii := range iters {
			want := 100 + ii
			if !slices.Contains(committedValues, want) {
				t.Fatalf("command %d missing from committed set %v", want, committedValues)
			}
		}
		return
	}
	t.Fatalf("term changed too often; could not complete concurrent-propose check in %d attempts", retries)
}

// termAdvanced reports whether any peer has moved past startTerm.
func termAdvanced(c *testcluster.Cluster, startTerm int) bool {
	for i := range c.N() {
		if c.Term(i) > startTerm {
			return true
		}
	}
	return false
}

// collectCommitted drains indices, waiting for each to commit at startTerm
// on all peers. If term advances mid-wait, returns termChanged=true.
func collectCommitted(c *testcluster.Cluster, indices <-chan int, startTerm int) (values []int, termChanged bool) {
	for idx := range indices {
		cmd := c.Wait(idx, c.N(), startTerm)
		if v, ok := cmd.(int); ok {
			if v == -1 {
				// Drain the rest so the producer goroutines don't leak.
				go func() {
					for range indices {
					}
				}()
				return values, true
			}
			values = append(values, v)
		}
	}
	return values, false
}

// TestReplicationRejoinPartitionedLeader exercises the scenario where a
// partitioned leader accumulates uncommitted entries in its local log
// while the rest of the cluster elects a new leader and moves on
// When the old leader rejoins, its divergent entries must be overwritten by
// the new leader's log
func TestReplicationRejoinPartitionedLeader(t *testing.T) {
	const servers = 3
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.One(101, servers, true)

	// Partition the leader. It'll keep accepting Start()s locally but
	// can't commit them.
	leader1 := c.CheckOneLeader()
	c.Disconnect(leader1)

	c.StartOn(leader1, 102)
	c.StartOn(leader1, 103)
	c.StartOn(leader1, 104)

	// The remaining two peers elect a new leader and commit at index 2
	c.One(103, 2, true)

	// Partition the new leader too; bring the old leader back.
	// The third peer still has the committed log from leader2, so the old leader
	// must overwrite its divergent entries to re-join agreement
	leader2 := c.CheckOneLeader()
	c.Disconnect(leader2)
	c.Connect(leader1)

	c.One(104, 2, true)

	// Everyone back in. Final commit must reach all three peers.
	c.Connect(leader2)
	c.One(105, servers, true)
}

// TestReplicationFastBackup stresses the leader's log-backup path:
// a minority partition builds up many uncommitted entries, then the
// partitions swap so a different group commits a long run, then the
// original leader rejoins and must back up through the divergent tail
// quickly enough that the cluster keeps progressing
// Uses 5 peers so a 3/2 partition is always decisive
func TestReplicationFastBackup(t *testing.T) {
	const servers = 5
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.One(rand.Int(), servers, true)

	// Partition A: leader1 + one follower; Partition B: the other three
	leader1 := c.CheckOneLeader()
	c.Disconnect((leader1 + 2) % servers)
	c.Disconnect((leader1 + 3) % servers)
	c.Disconnect((leader1 + 4) % servers)

	// Partition A has no quorum — these 50 proposals won't commit
	for range 50 {
		c.StartOn(leader1, rand.Int())
	}
	time.Sleep(electionTimeout / 2)

	// Swap partitions: A offline, B online with quorum 3/5
	c.Disconnect((leader1 + 0) % servers)
	c.Disconnect((leader1 + 1) % servers)
	c.Connect((leader1 + 2) % servers)
	c.Connect((leader1 + 3) % servers)
	c.Connect((leader1 + 4) % servers)

	for range 50 {
		c.One(rand.Int(), 3, true)
	}

	// Now isolate one of B's peers and let the new leader build up another
	// divergent tail.
	leader2 := c.CheckOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	c.Disconnect(other)

	for range 50 {
		c.StartOn(leader2, rand.Int())
	}

	// Drop every peer, then reconnect the original partition's two
	// members plus the isolated peer — that group must elect, catch up,
	// and commit 50 new entries
	for i := range servers {
		c.Disconnect(i)
	}
	c.Connect((leader1 + 0) % servers)
	c.Connect((leader1 + 1) % servers)
	c.Connect(other)

	for range 50 {
		c.One(rand.Int(), 3, true)
	}

	// Heal completely and confirm final agreement across all peers.
	for i := range servers {
		c.Connect(i)
	}
	c.One(rand.Int(), servers, true)
}

// TestReplicationRPCCountBounded asserts that the implementation doesn't
// burn excessive RPCs: the initial election should finish in a small
// constant, 10 agreement entries should cost O(entries * peers) RPCs,
// and a quiet 1-second idle period should see only heartbeat traffic
func TestReplicationRPCCountBounded(t *testing.T) {
	const (
		servers      = 3
		iters        = 10
		retries      = 5
		maxElection  = 30 // RPCs allowed for the initial election
		idleMaxHB    = 3 * 20
		idleDuration = electionTimeout
	)
	c := testcluster.New(t, servers)
	defer c.Shutdown()

	c.CheckOneLeader()

	total1 := c.RPCCount()
	if total1 > maxElection || total1 < 1 {
		t.Fatalf("initial election used %d RPCs, want in [1, %d]", total1, maxElection)
	}

	var total2 int64
	success := false
	for try := range retries {
		if try > 0 {
			time.Sleep(3 * time.Second)
		}

		leader := c.CheckOneLeader()
		total1 = c.RPCCount()

		starti, term, ok := c.StartOn(leader, 1)
		if !ok {
			continue
		}

		cmds := make([]int, 0, iters+1)
		termChanged := false
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			idx, tm, ok := c.StartOn(leader, x)
			if !ok || tm != term {
				termChanged = true
				break
			}
			if starti+i != idx {
				t.Fatalf("leader %d placed command at index %d, want %d", leader, idx, starti+i)
			}
		}
		if termChanged {
			continue
		}

		for i := 1; i < iters+1; i++ {
			cmd := c.Wait(starti+i, servers, term)
			v, ok := cmd.(int)
			if !ok || v != cmds[i-1] {
				if v == -1 {
					termChanged = true
					break
				}
				t.Fatalf("index %d: got %v, want %d", starti+i, cmd, cmds[i-1])
			}
		}
		if termChanged {
			continue
		}

		// Recheck terms before judging RPC counts — a term bump during
		// the run inflates the count for reasons unrelated to the bound
		// we're trying to verify.
		stillSameTerm := true
		for i := range servers {
			if c.Term(i) != term {
				stillSameTerm = false
				break
			}
		}
		total2 = c.RPCCount()
		if !stillSameTerm {
			continue
		}

		maxForAgreement := int64((iters + 1 + 3) * servers)
		if total2-total1 > maxForAgreement {
			t.Fatalf("used %d RPCs for %d entries, want <= %d", total2-total1, iters, maxForAgreement)
		}
		success = true
		break
	}
	if !success {
		t.Fatalf("term changed too often; could not verify RPC bound in %d attempts", retries)
	}

	time.Sleep(idleDuration)
	total3 := c.RPCCount()
	if total3-total2 > idleMaxHB {
		t.Fatalf("used %d RPCs during %v of idle, want <= %d", total3-total2, idleDuration, idleMaxHB)
	}
}
