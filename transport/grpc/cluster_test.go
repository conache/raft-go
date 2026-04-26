package grpc_test

import (
	"encoding/gob"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/conache/raft-go/rsm"
	memstorage "github.com/conache/raft-go/storage/memory"
	tgrpc "github.com/conache/raft-go/transport/grpc"
)

// counter is a tiny rsm.StateMachine used to verify the gRPC transport end-to-end
// Each Submit increments and returns the new value
type counter struct {
	mu sync.Mutex
	n  int
}

func (c *counter) DoOp(any) any     { c.mu.Lock(); c.n++; r := c.n; c.mu.Unlock(); return r }
func (c *counter) Snapshot() []byte { return nil }
func (c *counter) Restore([]byte)   {}

type incOp struct{}

func init() { gob.Register(incOp{}) }

// pickPorts returns n free TCP ports by binding then releasing them
func pickPorts(t *testing.T, n int) []string {
	t.Helper()

	addrs := make([]string, n)
	for i := range addrs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}

		addrs[i] = l.Addr().String()
		_ = l.Close()
	}

	return addrs
}

// TestGrpcEndToEnd brings up 3 RSMs over real gRPC connections and
// verifies that two consecutive Submits land on the leader and the
// counter advances 1 → 2 across the cluster
func TestGrpcEndToEnd(t *testing.T) {
	const n = 3
	addrs := pickPorts(t, n)

	clusters := make([]*tgrpc.Cluster, n)
	rsms := make([]*rsm.RSM, n)

	defer func() {
		for _, r := range rsms {
			if r != nil {
				r.Kill()
			}
		}
		for _, c := range clusters {
			if c != nil {
				c.Stop()
			}
		}
	}()

	for i := range n {
		c, err := tgrpc.Build(i, addrs)
		if err != nil {
			t.Fatalf("Build peer %d: %v", i, err)
		}

		clusters[i] = c
	}

	for i := range n {
		rsms[i] = rsm.MakeRSM(clusters[i].Peers, i, memstorage.New(), -1, &counter{})

		if err := clusters[i].Start(rsms[i].Raft(), addrs[i]); err != nil {
			t.Fatalf("Start peer %d: %v", i, err)
		}
	}

	for round := 1; round <= 2; round++ {
		deadline := time.Now().Add(5 * time.Second)

		var got any
		for time.Now().Before(deadline) {
			for _, r := range rsms {
				status, resp := r.Submit(incOp{})
				if status == rsm.OK {
					got = resp
					break
				}
			}
			if got != nil {
				break
			}

			time.Sleep(50 * time.Millisecond)
		}

		if got != round {
			t.Fatalf("round %d: got %v, want %d", round, got, round)
		}
	}
}
