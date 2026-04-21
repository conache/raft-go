// Package memory provides an in-process transport that satisfies
// transport.Peer. It is deterministic, supports partition and drop
// fault injection, and is intended both for consensus tests and for
// downstream projects that want to exercise raft-backed code without
// real networking.
package memory

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"

	"github.com/conache/raft-go/transport"
)

var (
	ErrOutOfRange   = errors.New("memory: peer id out of range")
	ErrDisconnected = errors.New("memory: peer unreachable")
	ErrDropped      = errors.New("memory: call dropped")
	ErrNoHandler    = errors.New("memory: no handler registered for target")
	ErrBadMethod    = errors.New("memory: no such method on handler")
	ErrBadSignature = errors.New("memory: handler method has wrong signature")
)

// Mesh is an in-process network of n peers that dispatch calls among
// themselves. A zero-value Mesh is unusable; construct with NewMesh.
type Mesh struct {
	mu           sync.Mutex
	n            int
	handlers     map[int]any
	disconnected map[[2]int]bool
	dropRate     float64
	rng          *rand.Rand
}

// Option configures a Mesh at construction.
type Option func(*Mesh)

// WithDropRate sets the per-call drop probability in [0, 1]. Default is 0.
func WithDropRate(rate float64) Option {
	return func(m *Mesh) { m.dropRate = rate }
}

// WithSeed seeds the internal RNG used for drop decisions. Default seed is
// 1, making tests reproducible by default.
func WithSeed(seed int64) Option {
	return func(m *Mesh) { m.rng = rand.New(rand.NewSource(seed)) }
}

// NewMesh returns an n-peer mesh. All peers start connected; no calls drop.
// n is fixed at construction and cannot change at runtime.
func NewMesh(n int, opts ...Option) *Mesh {
	m := &Mesh{
		n:            n,
		handlers:     make(map[int]any),
		disconnected: make(map[[2]int]bool),
		rng:          rand.New(rand.NewSource(1)),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Register sets the handler for peer id. Exported methods on the handler
// become callable via Peer.Call. Typical handler: a *consensus.Raft instance.
func (m *Mesh) Register(id int, handler any) error {
	if id < 0 || id >= m.n {
		return fmt.Errorf("%w: %d not in [0, %d)", ErrOutOfRange, id, m.n)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[id] = handler
	return nil
}

// Peers returns a slice of length n with the peer handles to use from node
// `from`. peers[i] dispatches to peer i.
func (m *Mesh) Peers(from int) []transport.Peer {
	out := make([]transport.Peer, m.n)
	for i := range m.n {
		out[i] = &Peer{mesh: m, from: from, to: i}
	}
	return out
}

// Connect restores the bidirectional link between i and j. No-op if i == j.
func (m *Mesh) Connect(i, j int) {
	if i == j {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.disconnected, pair(i, j))
}

// Disconnect breaks the bidirectional link between i and j. No-op if i == j.
func (m *Mesh) Disconnect(i, j int) {
	if i == j {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disconnected[pair(i, j)] = true
}

// Isolate disconnects i from all other peers.
func (m *Mesh) Isolate(i int) {
	for j := range m.n {
		if i != j {
			m.Disconnect(i, j)
		}
	}
}

// Heal reconnects i to all other peers.
func (m *Mesh) Heal(i int) {
	for j := range m.n {
		if i != j {
			m.Connect(i, j)
		}
	}
}

// pair returns a canonical [min, max] pair used as a map key so a single
// entry captures both directions.
func pair(i, j int) [2]int {
	if i < j {
		return [2]int{i, j}
	}
	return [2]int{j, i}
}

func (m *Mesh) isConnected(i, j int) bool {
	if i == j {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.disconnected[pair(i, j)]
}

func (m *Mesh) shouldDrop() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dropRate <= 0 {
		return false
	}
	return m.rng.Float64() < m.dropRate
}

func (m *Mesh) handlerFor(id int) any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.handlers[id]
}

// Peer is a handle for sending RPCs from one node to another.
// It implements transport.Peer.
type Peer struct {
	mesh *Mesh
	from int
	to   int
}

// Call implements transport.Peer.Call. It serializes args via gob,
// dispatches to the target handler's method in a separate goroutine so a
// held mutex on the callee cannot deadlock the caller, and deserializes the
// reply back. The caller's ctx is honored for cancellation.
func (p *Peer) Call(ctx context.Context, method string, args, reply any) error {
	m := p.mesh

	if p.to < 0 || p.to >= m.n {
		return fmt.Errorf("%w: %d", ErrOutOfRange, p.to)
	}
	if !m.isConnected(p.from, p.to) {
		return ErrDisconnected
	}
	if m.shouldDrop() {
		return ErrDropped
	}

	handler := m.handlerFor(p.to)
	if handler == nil {
		return ErrNoHandler
	}

	methodName := method
	if i := strings.LastIndex(method, "."); i >= 0 {
		methodName = method[i+1:]
	}

	methodValue := reflect.ValueOf(handler).MethodByName(methodName)
	if !methodValue.IsValid() {
		return fmt.Errorf("%w: %s", ErrBadMethod, methodName)
	}

	mt := methodValue.Type()
	if mt.NumIn() != 2 {
		return fmt.Errorf("%w: %s: want 2 params, got %d", ErrBadSignature, methodName, mt.NumIn())
	}
	if mt.In(0).Kind() != reflect.Ptr || mt.In(1).Kind() != reflect.Ptr {
		return fmt.Errorf("%w: %s: params must be pointers", ErrBadSignature, methodName)
	}

	// Round-trip args into a fresh instance so the handler cannot mutate the
	// caller's struct. Mirrors wire serialization.
	argCopy := reflect.New(mt.In(0).Elem())
	if err := gobRoundTrip(args, argCopy.Interface()); err != nil {
		return fmt.Errorf("memory: encoding args: %w", err)
	}
	replyCopy := reflect.New(mt.In(1).Elem())

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("memory: handler panicked: %v", r)
			}
		}()
		methodValue.Call([]reflect.Value{argCopy, replyCopy})
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := gobRoundTrip(replyCopy.Interface(), reply); err != nil {
		return fmt.Errorf("memory: decoding reply: %w", err)
	}
	return nil
}

// gobRoundTrip encodes src and decodes into dst through a single buffer.
// Used so handler mutations don't alias back to the caller's structs.
func gobRoundTrip(src, dst any) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(&buf).Decode(dst)
}
