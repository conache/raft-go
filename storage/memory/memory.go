package memory

import "sync"

// Store is an in-memory implementation of storage.Store.
// It holds Raft state and snapshot bytes behind a mutex. State does not
// survive process restarts; use Clone to simulate a node restart in tests.
type Store struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

// New returns an empty Store.
func New() *Store {
	return &Store{}
}

// Save atomically replaces the persisted state. Passing nil for snapshot
// leaves the existing snapshot untouched; passing a non-nil byte slice
// (including a zero-length one) replaces it.
func (s *Store) Save(state, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = cloneBytes(state)
	if snapshot != nil {
		s.snapshot = cloneBytes(snapshot)
	}
	return nil
}

// ReadState returns a copy of the persisted state, or nil if nothing has
// been saved.
func (s *Store) ReadState() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneBytes(s.state), nil
}

// ReadSnapshot returns a copy of the persisted snapshot, or nil if nothing
// has been saved.
func (s *Store) ReadSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneBytes(s.snapshot), nil
}

// StateSize returns the length of the persisted state in bytes. Snapshot
// bytes are not counted.
func (s *Store) StateSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.state)
}

// Clone returns a deep copy. Useful for simulating a node restart: kill the
// node, create a new one backed by the clone, and it sees the same persisted
// history.
func (s *Store) Clone() *Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &Store{
		state:    cloneBytes(s.state),
		snapshot: cloneBytes(s.snapshot),
	}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
