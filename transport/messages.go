package transport

// LogEntry is a single entry in the Raft replicated log.
// Command is opaque to consensus; it is whatever the state machine expects.
type LogEntry struct {
	Term    int
	Command any
}

// RequestVoteArgs is sent by a candidate to solicit votes.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is returned by a voter.
type RequestVoteReply struct {
	Term    int
	Success bool
}

// AppendEntriesArgs is sent by a leader to replicate log entries or to
// send a heartbeat (Entries may be empty).
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply is returned by a follower. On a conflict, EntryTerm and
// EntryTermStartIndex let the leader skip past conflicting entries faster.
type AppendEntriesReply struct {
	Term    int
	Success bool
	// Actual entry's term in case of conflicting entries
	EntryTerm int
	// First entry's idx with the conflicting term in the follower's log
	EntryTermStartIndex int
}

// InstallSnapshotArgs is sent by a leader to bring a lagging follower up to
// date with a snapshot instead of replaying log entries.
type InstallSnapshotArgs struct {
	// leader's term
	Term int
	// leader id
	LeaderID int
	// last included log index in the snapshot
	LastIncludedIndex int
	// last included term
	LastIncludedTerm int
	// snapshot data
	Data []byte
}

// InstallSnapshotReply is returned by a follower after (attempting to)
// install the snapshot.
type InstallSnapshotReply struct {
	// follower's current term
	Term int
}
