package consensus_test

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/conache/raft-go/internal/testcluster"
)

// regOp is the single-register command type. "put" writes Value; "get"
// reads the current register value. Registered with gob so it can round-
// trip through the in-memory transport's serialization.
type regOp struct {
	Kind  string
	Value int
}

func init() { gob.Register(regOp{}) }

// registerSM is the per-node state machine — a single int register.
// Every cluster node holds its own copy; because Raft delivers applies in
// log order, all copies converge to the same sequence of states.
type registerSM struct {
	mu  sync.Mutex
	val int
}

func (s *registerSM) apply(op regOp) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if op.Kind == "put" {
		s.val = op.Value
		return op.Value
	}
	return s.val
}

// registerModel is the sequential specification Porcupine checks the
// observed history against. For a single-int register:
//   - put(v) returns v and transitions state → v
//   - get() returns current state and leaves it unchanged
var registerModel = porcupine.Model{
	Init: func() any { return 0 },
	Step: func(state, input, output any) (bool, any) {
		s := state.(int)
		op := input.(regOp)
		got := output.(int)
		if op.Kind == "put" {
			return got == op.Value, op.Value
		}
		return got == s, s
	},
	Equal: func(a, b any) bool { return a.(int) == b.(int) },
	DescribeOperation: func(input, output any) string {
		op := input.(regOp)
		ret := output.(int)
		if op.Kind == "put" {
			return fmt.Sprintf("put(%d) -> %d", op.Value, ret)
		}
		return fmt.Sprintf("get() -> %d", ret)
	},
}

// opHistory is a goroutine-safe buffer of Porcupine operations recorded
// across all client goroutines.
type opHistory struct {
	mu  sync.Mutex
	ops []porcupine.Operation
}

func (h *opHistory) append(op porcupine.Operation) {
	h.mu.Lock()
	h.ops = append(h.ops, op)
	h.mu.Unlock()
}

func (h *opHistory) snapshot() []porcupine.Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]porcupine.Operation, len(h.ops))
	copy(out, h.ops)
	return out
}

// TestReplicationRegisterLinearizable runs three concurrent client
// goroutines against a 3-node cluster, each issuing 20 mixed Put/Get
// operations on a shared single-register state machine. The resulting
// history is fed to Porcupine, which verifies it is linearizable.
func TestReplicationRegisterLinearizable(t *testing.T) {
	const (
		clients      = 3
		opsPerClient = 20
		writeBias    = 7 // ~70% puts, ~30% gets
	)

	c := testcluster.New(t, 3)
	defer c.Shutdown()

	// One state machine per cluster node; they should all converge because
	// Raft delivers commands in the same order everywhere.
	sms := make([]*registerSM, c.N())
	for i := range sms {
		sms[i] = &registerSM{}
	}
	c.SetApply(func(nodeIdx, _ int, cmd any) any {
		return sms[nodeIdx].apply(cmd.(regOp))
	})

	c.CheckOneLeader() // warm-up: make sure a leader exists

	history := &opHistory{}
	var wg sync.WaitGroup
	for clientID := range clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Each goroutine gets its own RNG so clients are independent.
			rng := rand.New(rand.NewSource(int64(id) + 1))
			for range opsPerClient {
				var op regOp
				if rng.Intn(10) < writeBias {
					op = regOp{Kind: "put", Value: rng.Intn(1000)}
				} else {
					op = regOp{Kind: "get"}
				}

				callTime := time.Now().UnixNano()
				// Wait for a majority — safe to read the result from any of them.
				idx := c.One(op, (c.N()/2)+1, true)

				// Pull the state-machine return value for this commit.
				var ret int
				if r, ok := c.AnyResult(idx); ok {
					ret = r.(int)
				} else {
					t.Errorf("client %d: no result for idx %d", id, idx)
					return
				}
				returnTime := time.Now().UnixNano()

				history.append(porcupine.Operation{
					ClientId: id,
					Input:    op,
					Output:   ret,
					Call:     callTime,
					Return:   returnTime,
				})
			}
		}(clientID)
	}
	wg.Wait()

	// Porcupine's checker can be slow on pathological histories; bound it.
	result, _ := porcupine.CheckOperationsVerbose(registerModel, history.snapshot(), 5*time.Second)
	switch result {
	case porcupine.Ok:
		// Linearizable — pass.
	case porcupine.Illegal:
		t.Fatalf("history is not linearizable")
	case porcupine.Unknown:
		t.Fatalf("linearizability check timed out")
	}
}
