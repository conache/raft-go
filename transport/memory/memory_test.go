package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conache/raft-go/transport"
)

// Compile-time check: *Peer satisfies transport.Peer.
var _ transport.Peer = (*Peer)(nil)

type (
	echoArgs  struct{ Value string }
	echoReply struct{ Value string }
)

type echoHandler struct {
	mu    sync.Mutex
	calls int
}

func (e *echoHandler) Echo(args *echoArgs, reply *echoReply) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	reply.Value = args.Value
}

type mutatingHandler struct{}

func (h *mutatingHandler) Mutate(args *echoArgs, reply *echoReply) {
	args.Value = "MUTATED"
	reply.Value = "ok"
}

type panicHandler struct{}

func (h *panicHandler) Boom(args *echoArgs, reply *echoReply) {
	panic("boom")
}

type sleepyHandler struct{}

func (h *sleepyHandler) Sleep(args *echoArgs, reply *echoReply) {
	time.Sleep(100 * time.Millisecond)
	reply.Value = args.Value
}

func TestHappyPath(t *testing.T) {
	m := NewMesh(2)
	_ = m.Register(0, &echoHandler{})
	_ = m.Register(1, &echoHandler{})

	reply := &echoReply{}
	if err := m.Peers(0)[1].Call(context.Background(), "Echo.Echo", &echoArgs{Value: "hi"}, reply); err != nil {
		t.Fatal(err)
	}
	if reply.Value != "hi" {
		t.Errorf("reply.Value = %q, want hi", reply.Value)
	}
}

func TestGobIsolationProtectsArgs(t *testing.T) {
	m := NewMesh(2)
	_ = m.Register(1, &mutatingHandler{})

	args := &echoArgs{Value: "original"}
	if err := m.Peers(0)[1].Call(context.Background(), "Mutate", args, &echoReply{}); err != nil {
		t.Fatal(err)
	}
	if args.Value != "original" {
		t.Errorf("caller's args mutated by handler: %q", args.Value)
	}
}

func TestDropRateAlwaysDrops(t *testing.T) {
	m := NewMesh(2, WithDropRate(1.0))
	_ = m.Register(1, &echoHandler{})

	err := m.Peers(0)[1].Call(context.Background(), "Echo", &echoArgs{}, &echoReply{})
	if !errors.Is(err, ErrDropped) {
		t.Errorf("err = %v, want ErrDropped", err)
	}
}

func TestDropRateNeverDrops(t *testing.T) {
	m := NewMesh(2)
	_ = m.Register(1, &echoHandler{})

	for range 100 {
		if err := m.Peers(0)[1].Call(context.Background(), "Echo", &echoArgs{}, &echoReply{}); err != nil {
			t.Fatalf("drop rate 0 but got: %v", err)
		}
	}
}

func TestDropRateApproximatelyHalf(t *testing.T) {
	m := NewMesh(2, WithDropRate(0.5), WithSeed(42))
	_ = m.Register(1, &echoHandler{})

	const n = 400
	dropped := 0
	for range n {
		err := m.Peers(0)[1].Call(context.Background(), "Echo", &echoArgs{}, &echoReply{})
		if errors.Is(err, ErrDropped) {
			dropped++
		}
	}
	if dropped < n*30/100 || dropped > n*70/100 {
		t.Errorf("dropped = %d of %d, expected roughly half", dropped, n)
	}
}

func TestDisconnectIsBidirectional(t *testing.T) {
	m := NewMesh(3)
	for i := range 3 {
		_ = m.Register(i, &echoHandler{})
	}
	peers0 := m.Peers(0)
	peers1 := m.Peers(1)

	m.Disconnect(0, 1)

	if err := peers0[1].Call(context.Background(), "Echo", &echoArgs{}, &echoReply{}); !errors.Is(
		err,
		ErrDisconnected,
	) {
		t.Errorf("0->1: err = %v, want ErrDisconnected", err)
	}
	if err := peers1[0].Call(context.Background(), "Echo", &echoArgs{}, &echoReply{}); !errors.Is(
		err,
		ErrDisconnected,
	) {
		t.Errorf("1->0: err = %v, want ErrDisconnected", err)
	}
	if err := peers0[2].Call(context.Background(), "Echo", &echoArgs{}, &echoReply{}); err != nil {
		t.Errorf("0->2 unaffected: %v", err)
	}

	m.Connect(0, 1)
	if err := peers0[1].Call(context.Background(), "Echo", &echoArgs{}, &echoReply{}); err != nil {
		t.Errorf("0->1 after Connect: %v", err)
	}
}

func TestIsolateAndHeal(t *testing.T) {
	m := NewMesh(4)
	for i := range 4 {
		_ = m.Register(i, &echoHandler{})
	}

	m.Isolate(0)
	peers0 := m.Peers(0)
	for to := 1; to < 4; to++ {
		if err := peers0[to].Call(context.Background(), "Echo", &echoArgs{}, &echoReply{}); !errors.Is(
			err,
			ErrDisconnected,
		) {
			t.Errorf("0->%d after Isolate: err = %v", to, err)
		}
	}
	if err := m.Peers(1)[2].Call(context.Background(), "Echo", &echoArgs{}, &echoReply{}); err != nil {
		t.Errorf("1->2 should still work after Isolate(0): %v", err)
	}

	m.Heal(0)
	for to := 1; to < 4; to++ {
		if err := peers0[to].Call(context.Background(), "Echo", &echoArgs{}, &echoReply{}); err != nil {
			t.Errorf("0->%d after Heal: %v", to, err)
		}
	}
}

func TestContextCancellation(t *testing.T) {
	m := NewMesh(2)
	_ = m.Register(1, &sleepyHandler{})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := m.Peers(0)[1].Call(ctx, "Sleep", &echoArgs{}, &echoReply{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
}

func TestUnknownMethodReturnsError(t *testing.T) {
	m := NewMesh(2)
	_ = m.Register(1, &echoHandler{})

	err := m.Peers(0)[1].Call(context.Background(), "NoSuchMethod", &echoArgs{}, &echoReply{})
	if !errors.Is(err, ErrBadMethod) {
		t.Errorf("err = %v, want ErrBadMethod", err)
	}
}

func TestUnregisteredPeerReturnsError(t *testing.T) {
	m := NewMesh(2)
	_ = m.Register(0, &echoHandler{})

	err := m.Peers(0)[1].Call(context.Background(), "Echo", &echoArgs{}, &echoReply{})
	if !errors.Is(err, ErrNoHandler) {
		t.Errorf("err = %v, want ErrNoHandler", err)
	}
}

func TestHandlerPanicSurfaces(t *testing.T) {
	m := NewMesh(2)
	_ = m.Register(1, &panicHandler{})

	err := m.Peers(0)[1].Call(context.Background(), "Boom", &echoArgs{}, &echoReply{})
	if err == nil {
		t.Fatal("expected error from panicking handler")
	}
}

func TestConcurrentCalls(t *testing.T) {
	m := NewMesh(3)
	for i := range 3 {
		_ = m.Register(i, &echoHandler{})
	}

	var wg sync.WaitGroup
	var successes atomic.Int64
	for range 50 {
		for src := range 3 {
			for dst := range 3 {
				if src == dst {
					continue
				}
				wg.Add(1)
				go func(s, d int) {
					defer wg.Done()
					if err := m.Peers(s)[d].Call(context.Background(), "Echo", &echoArgs{Value: "x"}, &echoReply{}); err == nil {
						successes.Add(1)
					}
				}(src, dst)
			}
		}
	}
	wg.Wait()
	if successes.Load() == 0 {
		t.Fatal("no successful concurrent calls")
	}
}
