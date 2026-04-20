package storage

// Store persists Raft's durable state and snapshots. Consensus calls Save
// whenever persistent state changes (term, vote, log entries), and reads back
// both state and snapshot on startup to resume.
//
// Save is expected to be atomic: either both the state blob and snapshot blob
// are durable, or neither. Passing nil for snapshot means "leave the existing
// snapshot untouched".
type Store interface {
	Save(state, snapshot []byte) error
	ReadState() ([]byte, error)
	ReadSnapshot() ([]byte, error)
	StateSize() int
}
