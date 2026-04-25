// Package memory is an in-process transport.Peer implementation
// Deterministic, supports partition and drop fault injection
// Used by consensus tests and by downstream projects that want to
// exercise raft-backed code without real networking
package memory

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

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

// Mesh is an in-process network of n peers dispatching calls to each other
// A zero-value Mesh is unusable; construct with NewMesh
type Mesh struct {
	// Guards handlers, disconnected, and rng
	mu sync.Mutex
	// Fixed peer count set at construction
	n int
	// Per-peer RPC targets registered via Register; nil slot = no handler yet
	handlers map[int]any
	// Canonical-pair keyset of currently severed links, see pair()
	disconnected map[[2]int]bool
	// Per-call drop probability in [0, 1]; 0 means never drop
	dropRate float64
	// RNG consulted by shouldDrop, seeded by WithSeed for determinism
	rng *rand.Rand

	// Cumulative count of dispatched calls
	totalCalls atomic.Int64
	// Gob-encoded sizes of args and replies for every dispatched call
	totalBytes atomic.Int64
	// Cumulative count of calls the drop-rate filter discarded
	dropped atomic.Int64
	// Guards methodCounts
	methodMu sync.Mutex
	// Per-method call counters, keyed by whatever method name the caller
	// passes so the mesh stays agnostic to the service on top of it
	methodCounts map[string]int64
}

// Stats is a snapshot of a mesh's cumulative call counters
type Stats struct {
	// Total dispatched calls since the mesh was created
	Calls int64
	// Total gob-encoded arg + reply bytes across all dispatched calls
	Bytes int64
	// Calls that the drop-rate filter discarded
	Dropped int64
	// Per-method dispatched-call counts, same keys the caller used
	ByMethod map[string]int64
}

// Stats returns a snapshot of the mesh's counters
// ByMethod is a copy safe for the caller to mutate or retain
func (m *Mesh) Stats() Stats {
	m.methodMu.Lock()
	defer m.methodMu.Unlock()

	byMethod := maps.Clone(m.methodCounts)
	return Stats{
		Calls:    m.totalCalls.Load(),
		Bytes:    m.totalBytes.Load(),
		Dropped:  m.dropped.Load(),
		ByMethod: byMethod,
	}
}

// Option configures a Mesh at construction
type Option func(*Mesh)

// WithDropRate sets the per-call drop probability in [0, 1]; default 0
func WithDropRate(rate float64) Option {
	return func(m *Mesh) { m.dropRate = rate }
}

// SetDropRate changes the per-call drop probability at runtime
// Use it to toggle reliable/unreliable modes mid-test
func (m *Mesh) SetDropRate(rate float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropRate = rate
}

// WithSeed seeds the RNG used for drop decisions; default 1 for reproducibility
func WithSeed(seed int64) Option {
	return func(m *Mesh) { m.rng = rand.New(rand.NewSource(seed)) }
}

// NewMesh returns an n-peer mesh with all peers connected and no drops
// n is fixed at construction
func NewMesh(n int, opts ...Option) *Mesh {
	m := &Mesh{
		n:            n,
		handlers:     make(map[int]any),
		disconnected: make(map[[2]int]bool),
		rng:          rand.New(rand.NewSource(1)),
		methodCounts: make(map[string]int64),
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

// Register sets the handler for peer id
// Exported methods on the handler become callable via Peer.Call
func (m *Mesh) Register(id int, handler any) error {
	if id < 0 || id >= m.n {
		return fmt.Errorf("%w: %d not in [0, %d)", ErrOutOfRange, id, m.n)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.handlers[id] = handler
	return nil
}

// Peers returns peer handles indexed by target id, for use from node `from`
func (m *Mesh) Peers(from int) []transport.Peer {
	out := make([]transport.Peer, m.n)
	for i := range m.n {
		out[i] = &Peer{mesh: m, from: from, to: i}
	}
	return out
}

// Connect restores the bidirectional link between i and j; no-op if i == j
func (m *Mesh) Connect(i, j int) {
	if i == j {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.disconnected, pair(i, j))
}

// Disconnect breaks the bidirectional link between i and j; no-op if i == j
func (m *Mesh) Disconnect(i, j int) {
	if i == j {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disconnected[pair(i, j)] = true
}

// Isolate disconnects i from all other peers
func (m *Mesh) Isolate(i int) {
	for j := range m.n {
		if i != j {
			m.Disconnect(i, j)
		}
	}
}

// Heal reconnects i to all other peers
func (m *Mesh) Heal(i int) {
	for j := range m.n {
		if i != j {
			m.Connect(i, j)
		}
	}
}

// pair returns a canonical [min, max] key so one map entry covers both directions
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

// Peer is a handle for sending RPCs from one node to another;
// it implements transport.Peer
type Peer struct {
	// Back-reference to the mesh the call is dispatched through
	mesh *Mesh
	// Caller peer id, used to look up the (from, to) link state
	from int
	// Target peer id; Call dispatches to the handler registered at this id
	to int
}

// Call implements transport.Peer.Call
// Serializes args via gob, dispatches in a goroutine so a held callee mutex
// can't deadlock the caller, then deserializes the reply back
// Honors ctx for cancellation
func (p *Peer) Call(ctx context.Context, method string, args, reply any) error {
	m := p.mesh

	// Validate target reachability and apply fault injection
	if p.to < 0 || p.to >= m.n {
		return fmt.Errorf("%w: %d", ErrOutOfRange, p.to)
	}
	if !m.isConnected(p.from, p.to) {
		return ErrDisconnected
	}
	if m.shouldDrop() {
		m.dropped.Add(1)
		return ErrDropped
	}

	// Resolve handler and target method by reflection
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

	// Record the dispatch; mesh stays service-agnostic by keying on the caller's name
	m.totalCalls.Add(1)
	m.methodMu.Lock()
	m.methodCounts[methodName]++
	m.methodMu.Unlock()

	// Validate handler signature: (*ArgT, *ReplyT)
	mt := methodValue.Type()
	if mt.NumIn() != 2 {
		return fmt.Errorf("%w: %s: want 2 params, got %d", ErrBadSignature, methodName, mt.NumIn())
	}
	if mt.In(0).Kind() != reflect.Ptr || mt.In(1).Kind() != reflect.Ptr {
		return fmt.Errorf("%w: %s: params must be pointers", ErrBadSignature, methodName)
	}

	// Round-trip args into a fresh instance so handler mutations don't alias back
	argCopy := reflect.New(mt.In(0).Elem())
	argBytes, err := gobRoundTrip(args, argCopy.Interface())
	if err != nil {
		return fmt.Errorf("memory: encoding args: %w", err)
	}
	replyCopy := reflect.New(mt.In(1).Elem())

	// Dispatch in a goroutine so a callee mutex can't deadlock the caller;
	// honor ctx for cancellation
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

	// Round-trip reply back into the caller's buffer
	replyBytes, err := gobRoundTrip(replyCopy.Interface(), reply)
	if err != nil {
		return fmt.Errorf("memory: decoding reply: %w", err)
	}

	m.totalBytes.Add(int64(argBytes + replyBytes))
	return nil
}

// gobRoundTrip encodes src and decodes into dst through a single buffer
// Returns the encoded size so callers can account for wire cost
func gobRoundTrip(src, dst any) (int, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return 0, err
	}

	n := buf.Len()
	if err := gob.NewDecoder(&buf).Decode(dst); err != nil {
		return n, err
	}
	return n, nil
}
