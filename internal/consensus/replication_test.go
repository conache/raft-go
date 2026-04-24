package consensus_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/conache/raft-go/internal/lincheck"
	"github.com/conache/raft-go/internal/testcluster"
)

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
