package transport

import "context"

// Peer is a handle to a remote Raft node. Consensus uses it to send one of
// three RPCs — "Raft.RequestVote", "Raft.AppendEntries", "Raft.InstallSnapshot"
// — and waits for the reply struct to be populated.
//
// Implementations are responsible for serialization, retries (if any), and
// enforcing the caller's context deadline. A returned error indicates the call
// failed to complete; a nil error means reply has been populated.
type Peer interface {
	Call(ctx context.Context, method string, args, reply any) error
}
