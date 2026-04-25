package memory

import "sync"

// Store is an in-memory implementation of storage.Store
// Holds Raft state and snapshot bytes behind a mutex
// Nothing survives process restarts; use Clone to simulate a node restart
type Store struct {
	// Guards state and snapshot
	mu sync.Mutex
	// Persisted Raft state (term, votedFor, log), last value passed to Save
	state []byte
	// Persisted snapshot blob, last value passed to Save (nil preserves prior)
	snapshot []byte
}

// New returns an empty Store
func New() *Store {
	return &Store{}
}

// Save atomically replaces the persisted state
// A nil snapshot leaves the existing one untouched; non-nil (even empty) replaces it
func (s *Store) Save(state, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = cloneBytes(state)
	if snapshot != nil {
		s.snapshot = cloneBytes(snapshot)
	}
	return nil
}

// ReadState returns a copy of the persisted state, or nil if none was saved
func (s *Store) ReadState() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneBytes(s.state), nil
}

// ReadSnapshot returns a copy of the persisted snapshot, or nil if none was saved
func (s *Store) ReadSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneBytes(s.snapshot), nil
}

// StateSize returns the length of the persisted state; snapshot bytes excluded
func (s *Store) StateSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.state)
}

// Clone returns a deep copy
// Use it to simulate a node restart: kill the node, start a new one
// against the clone, and it sees the same persisted history
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
