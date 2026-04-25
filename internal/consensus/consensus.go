package consensus

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conache/raft-go/internal/dlog"
	"github.com/conache/raft-go/storage"
	"github.com/conache/raft-go/transport"
)

// Timing parameters. rpcTimeout must be less than electionTimeoutMin so
// that a failed RPC can be retried (or abandoned) before an election
// timeout fires elsewhere in the cluster.
//
// Election timeouts are intentionally generous (500-1000ms) so the
// integration tests stay stable under `go test -race`, which can slow
// goroutine scheduling enough that heartbeats occasionally arrive past a
// tighter timeout and trigger spurious re-elections.
const (
	heartbeatInterval  = 100 * time.Millisecond
	rpcTimeout         = 150 * time.Millisecond
	electionTimeoutMin = 500 * time.Millisecond
	electionTimeoutMax = 1000 * time.Millisecond
)

type nodeRole string

const (
	rFollower  nodeRole = "FOLLOWER"
	rCandidate nodeRole = "CANDIDATE"
	rLeader    nodeRole = "LEADER"
)

// ApplyMsg is delivered on applyCh to the consumer (rsm or a custom applier)
// CommandValid is set for a newly committed log entry;
// the Snapshot* fields are set when the leader pushed a snapshot that the follower just installed.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int

	// SnapshotValid is set to true when
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Node implements a single peer in the Raft consensus cluster.
type Node struct {
	mu    sync.Mutex       // lock to protect shared access to this peer's state
	peers []transport.Peer // RPC end points of all peers
	store storage.Store    // persistent state store for this peer
	me    int              // this peer's index into peers[]
	dead  int32            // set by Kill()

	// conditional variable to track conditions met
	// for updating the state of the node
	stateUpdateCond *sync.Cond

	// current node role
	currentRole nodeRole
	// current term of the node
	currentTerm int
	// peer this node voted for in the current term (-1 if none)
	votedFor int
	// the server's log entries
	log []transport.LogEntry

	// index of the next log entry
	// to be sent to the node
	nextIndex []int

	// index of highest log entry known to be replicated
	// on the server
	matchIndex []int

	// log entries indices
	// index of the highest log entry known to be committed
	commitIndex int

	// last heartbeat time
	lastHeartbeat time.Time

	// latest snapshot
	snapshot []byte
	// last included entry index in the latest snapshot
	snapshotLastIncludedIndex int
	// last included term in the latest snapshot
	snapshotLastIncludedTerm int

	// apply state ch
	applyCh chan ApplyMsg
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (n *Node) GetState() (int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.currentTerm, n.currentRole == rLeader
}

// persist saves durable state (term, vote, log, snapshot metadata) to stable storage.
// Must be called before acking any RPC that depends on the
// state being persistent, per Raft's safety guarantees.
// See Figure 2 of the Raft paper for the full list of required persistent state.
func (n *Node) persist() {
	blob := new(bytes.Buffer)
	e := gob.NewEncoder(blob)
	e.Encode(n.currentTerm)
	e.Encode(n.votedFor)
	e.Encode(n.snapshotLastIncludedIndex)
	e.Encode(n.snapshotLastIncludedTerm)
	e.Encode(n.log)
	_ = n.store.Save(blob.Bytes(), n.snapshot)
}

// readPersist restores previously persisted state into n.
func (n *Node) readPersist(data []byte) error {
	if n.killed() {
		return nil
	}

	if len(data) < 1 { // bootstrap without any state?
		return nil
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var snapshotLastIncludedIndex int
	var snapshotLastIncludedTerm int
	var logEntries []transport.LogEntry

	if err := d.Decode(&currentTerm); err != nil {
		return err
	}
	if err := d.Decode(&votedFor); err != nil {
		return err
	}
	if err := d.Decode(&snapshotLastIncludedIndex); err != nil {
		return err
	}
	if err := d.Decode(&snapshotLastIncludedTerm); err != nil {
		return err
	}
	if err := d.Decode(&logEntries); err != nil {
		return err
	}

	n.currentTerm = currentTerm
	n.votedFor = votedFor
	n.snapshotLastIncludedIndex = snapshotLastIncludedIndex
	n.snapshotLastIncludedTerm = snapshotLastIncludedTerm
	n.log = logEntries
	snap, err := n.store.ReadSnapshot()
	if err != nil {
		return err
	}
	n.snapshot = snap
	return nil
}

// PersistBytes returns how many bytes are in Raft's persisted log.
func (n *Node) PersistBytes() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.StateSize()
}

// Snapshot is called by the state machine once it has captured its state
// up to the specified index. Raft then trims its log through that index.
func (n *Node) Snapshot(index int, snapshot []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	// determine node log's (local) index from the global index

	if index <= n.snapshotLastIncludedIndex {
		dlog.Dlog(
			dlog.DSnap,
			"S%d; T%d; R(%s) - Received command to snapshot until idx %d, but already snapshotted until %d",
			n.me,
			n.currentTerm,
			n.currentRole,
			index,
			n.snapshotLastIncludedIndex,
		)
		return
	}

	localLogIndex := index - n.snapshotLastIncludedIndex

	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - SNAPSHOT. Snapshot node's state until index '%d'(local); '%d'(global)",
		n.me,
		n.currentTerm,
		n.currentRole,
		localLogIndex,
		index,
	)

	n.snapshotLastIncludedTerm = n.log[localLogIndex].Term
	n.snapshotLastIncludedIndex = index
	n.snapshot = snapshot
	n.log = n.log[localLogIndex:]
	n.log[0] = transport.LogEntry{Term: n.snapshotLastIncludedTerm}

	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - SNAPSHOT. Snapshot done - new log array: %v",
		n.me,
		n.currentTerm,
		n.currentRole,
		n.log,
	)

	n.persist()
}

// RequestVote handles vote requests from candidates during elections.
func (n *Node) RequestVote(args *transport.RequestVoteArgs, reply *transport.RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.killed() {
		*reply = transport.RequestVoteReply{
			Success: false,
			Term:    n.currentTerm,
		}
		return
	}

	if args.Term < n.currentTerm {
		// the election is for a past term,
		// so the node doesn't grant the vote
		// and sends its current term to the requesting candidate
		*reply = transport.RequestVoteReply{
			Success: false,
			Term:    n.currentTerm,
		}
		return
	}

	if args.Term > n.currentTerm {
		// the candidate node competes in a future term,
		// so our node should be a follower in that term
		n.setNodeTerm(args.Term)
	}

	if n.votedFor != -1 && n.votedFor != args.CandidateID {
		// vote already granted to another candidate
		*reply = transport.RequestVoteReply{
			Success: false,
			Term:    n.currentTerm,
		}
	} else {
		// With dummy entry at index 0, log is never empty
		latestLogIndexNorm := len(n.log) - 1
		latestLogIndexGlobal := latestLogIndexNorm + n.snapshotLastIncludedIndex
		lastLogTerm := n.log[latestLogIndexNorm].Term

		if args.LastLogTerm > lastLogTerm {
			// the candidate definitely has more up-to-date data
			// than the receiver, which makes it worth having the vote
			// from this peer
			n.votedFor = args.CandidateID
			// votedFor changed
			n.persist()

			*reply = transport.RequestVoteReply{
				Success: true,
				Term:    n.currentTerm,
			}
			return
		}

		// candidate latest log term <= receiver's latest log term
		if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= latestLogIndexGlobal {
			// the candidate's data is at least as up-to-date
			// as the receiver's data, so we can grant it
			// the peer's vote
			n.votedFor = args.CandidateID
			// votedFor changed
			n.persist()
			*reply = transport.RequestVoteReply{
				Success: true,
				Term:    n.currentTerm,
			}
			return
		}

		// none of the above, so the candidate's data makes it
		// not fit for receiving a supporting vote
		*reply = transport.RequestVoteReply{
			Success: false,
			Term:    n.currentTerm,
		}
	}
}

// AppendEntries handles log-replication RPCs from the leader. It doubles as
// a heartbeat when Entries is empty.
func (n *Node) AppendEntries(args *transport.AppendEntriesArgs, reply *transport.AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()
	dlog.Dlog(
		dlog.DInfo,
		"S%d; T%d; R(%s) - AppendEntries called by S%d.",
		n.me,
		n.currentTerm,
		n.currentRole,
		args.LeaderID,
	)

	logNextIndexGlobal := len(n.log) + n.snapshotLastIncludedIndex
	if n.killed() {
		*reply = transport.AppendEntriesReply{
			Term:                n.currentTerm,
			Success:             false,
			EntryTerm:           -1,
			EntryTermStartIndex: logNextIndexGlobal,
		}
		return
	}

	n.lastHeartbeat = time.Now()

	if n.currentRole == rLeader && args.Term == n.currentTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) - DROP. The node is a leader in this term and no longer accepts append entries",
			n.me,
			n.currentTerm,
			n.currentRole,
		)
		return
	}

	dlog.Dlog(
		dlog.DInfo,
		"S%d; T%d; R(%s) - handle AppendEntries call from S%d",
		n.me,
		n.currentTerm,
		n.currentRole,
		args.LeaderID,
	)
	if args.Term < n.currentTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) - DROP. leader's term (S%d;T%d) is less than this nodes' term",
			n.me,
			n.currentTerm,
			n.currentRole,
			args.LeaderID,
			args.Term,
		)
		*reply = transport.AppendEntriesReply{
			Term:                n.currentTerm,
			Success:             false,
			EntryTerm:           -1,
			EntryTermStartIndex: logNextIndexGlobal,
		}
		return
	}

	if args.Term > n.currentTerm {
		// caller's term > current term
		// ensure the node has the up-to-date term
		// and is in a follower state
		n.setNodeTerm(args.Term)
	} else if n.currentRole != rFollower {
		// we don't call setNodeTerm here,
		// because we don't want to change
		// the votedFor state variable
		n.currentRole = rFollower
	}

	// if the node already compressed its previous log in a snapshot
	// we need to account for this and normalize the prevLogIndex value internally
	prevLogIndexNorm := args.PrevLogIndex - n.snapshotLastIncludedIndex
	if prevLogIndexNorm < 0 {
		// prevLogIndex entry was already included in the latest snapshot
		*reply = transport.AppendEntriesReply{
			Term:                n.currentTerm,
			Success:             false,
			EntryTerm:           n.snapshotLastIncludedTerm,
			EntryTermStartIndex: n.snapshotLastIncludedIndex + 1,
		}
		return
	}

	// Check if we have the entry at prevLogIndexNorm
	if prevLogIndexNorm >= len(n.log) {
		// We don't have the entry at prevLogIndexNorm
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) - DROP. Log too short, prevLogIndexNorm %d >= len(log) %d",
			n.me,
			n.currentTerm,
			n.currentRole,
			prevLogIndexNorm,
			len(n.log),
		)
		*reply = transport.AppendEntriesReply{
			Term:                n.currentTerm,
			Success:             false,
			EntryTerm:           -1,
			EntryTermStartIndex: logNextIndexGlobal,
		}
		return
	}

	if prevLogIndexNorm >= 0 && n.log[prevLogIndexNorm].Term != args.PrevLogTerm {
		// prevLogIndex term doesn't match
		// the follower's log entries are outdated
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) - DROP. Conflicting prev log index entries at idx %d: '%d'(existing) <> '%d'(received)",
			n.me,
			n.currentTerm,
			n.currentRole,
			prevLogIndexNorm,
			n.log[prevLogIndexNorm].Term,
			args.PrevLogTerm,
		)
		// find the index of the first entry in the term
		termFirstIdx := 0
		for i := prevLogIndexNorm; i > 0; i-- {
			if n.log[i].Term == n.log[prevLogIndexNorm].Term {
				termFirstIdx = i
			}
		}

		*reply = transport.AppendEntriesReply{
			Term:                n.currentTerm,
			Success:             false,
			EntryTerm:           n.log[prevLogIndexNorm].Term,
			EntryTermStartIndex: termFirstIdx + n.snapshotLastIncludedIndex,
		}
		return
	}

	if len(args.Entries) != 0 {
		// the request is not just a heartbeat

		// search for a conflicting entry
		// in the already existing follower logs
		entriesConflictIdx := -1
		// new log array length (after update) is either:
		//  - more than the existing, case in which the log array expands
		//  - less than or equal the existing, case in which the log array has the same length
		newEntriesEndIdx := prevLogIndexNorm + len(args.Entries) + 1
		entriesMergeEndIdx := min(len(n.log), newEntriesEndIdx)
		offset := prevLogIndexNorm + 1
		for i := offset; i < entriesMergeEndIdx && entriesConflictIdx == -1; i++ {
			if n.log[i].Term != args.Entries[i-offset].Term {
				entriesConflictIdx = i
			}
		}

		if entriesConflictIdx > -1 {
			// conflict detected
			// slice out the log part starting at the conflict index
			n.log = n.log[:entriesConflictIdx]
			// append the remaining slice
			newLogEntries := args.Entries[(entriesConflictIdx - offset):]
			n.log = append(n.log, newLogEntries...)
		} else {
			// conflict not detected
			// update log accordingly
			// special case:
			if newEntriesEndIdx > len(n.log) {
				dlog.Dlog(dlog.DInfo, "S%d; T%d; R(%s) - no entries conflict. Adding entries starting with %d idx: %s", n.me, n.currentTerm, n.currentRole, args.PrevLogIndex+1, dlog.ToTruncatedArrayString(args.Entries))
				dlog.Dlog(dlog.DLog1, "S%d; T%d; R(%s) - New entries idx: %d vs n.log: %d", n.me, n.currentTerm, n.currentRole, newEntriesEndIdx, len(n.log))
				// there are new entries to append
				newLogEntries := args.Entries[(entriesMergeEndIdx - offset):]
				n.log = append(n.log, newLogEntries...)
				dlog.Dlog(dlog.DLog1, "S%d; T%d; R(%s) - New entries added: %.50v", n.me, n.currentTerm, n.currentRole, dlog.ToTruncatedArrayString(newLogEntries))
			}
		}

		// log changed, persist the change
		n.persist()
	}

	if args.LeaderCommit > n.commitIndex {
		dlog.Dlog(
			dlog.DInfo,
			"S%d; T%d; R(%s) - Leader commit index (%d) vs follower commit index (%d)",
			n.me,
			n.currentTerm,
			n.currentRole,
			args.LeaderCommit,
			n.commitIndex,
		)
		var lastVerifiedIndexGlobal int
		if len(args.Entries) > 0 {
			// we received new entries,
			// we store the last entry idx known to be received
			// from the leader
			lastVerifiedIndexGlobal = args.PrevLogIndex + len(args.Entries)
		} else {
			// no new entries
			// we know for sure that the entry at args.PrevLogIndex
			// is matching the leader's term and is a commit index candidate
			lastVerifiedIndexGlobal = args.PrevLogIndex
		}
		n.commitIndex = min(args.LeaderCommit, lastVerifiedIndexGlobal)
		dlog.Dlog(
			dlog.DLog2,
			"S%d; T%d; R(%s) - Updated commit index on the follower to '%d'",
			n.me,
			n.currentTerm,
			n.currentRole,
			n.commitIndex,
		)
		n.stateUpdateCond.Broadcast()
	}

	*reply = transport.AppendEntriesReply{
		Term:                n.currentTerm,
		Success:             true,
		EntryTerm:           n.currentTerm,
		EntryTermStartIndex: args.PrevLogIndex,
	}

	if len(args.Entries) != 0 {
		dlog.Dlog(
			dlog.DLog1,
			"S%d; T%d; R(%s) - Success response - current entries: %v",
			n.me,
			n.currentTerm,
			n.currentRole,
			n.log,
		)
	} else {
		dlog.Dlog(dlog.DHeartbit, "S%d; T%d; R(%s) - Heartbeat success", n.me, n.currentTerm, n.currentRole)
	}
}

// InstallSnapshot accepts a snapshot pushed by the leader when this follower
// is too far behind to catch up via normal log replication.
func (n *Node) InstallSnapshot(args *transport.InstallSnapshotArgs, reply *transport.InstallSnapshotReply) {
	n.mu.Lock()
	defer n.mu.Unlock()
	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - InstallSnapshot from S%d: LastIncludedIndex=%d, LastIncludedTerm=%d, DataLen=%d",
		n.me,
		n.currentTerm,
		n.currentRole,
		args.LeaderID,
		args.LastIncludedIndex,
		args.LastIncludedTerm,
		len(args.Data),
	)

	// default reply, returned when the execution of the handler is ended
	*reply = transport.InstallSnapshotReply{
		Term: n.currentTerm,
	}

	if n.killed() {
		// node already
		dlog.Dlog(
			dlog.DSnap,
			"S%d; T%d; R(%s) - Handling InstallSnapshot call",
			n.me,
			n.currentTerm,
			n.currentRole,
		)
		return
	}

	if n.currentRole == rLeader && args.Term == n.currentTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) InstallSnapshot(): The node is a leader in this term and no longer accepts these requests",
			n.me,
			n.currentTerm,
			n.currentRole,
		)
		return
	}
	// refresh last heartbit since InstallSnapshot() can only be called by the leader
	n.lastHeartbeat = time.Now()

	if args.Term < n.currentTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) InstallSnapshot(): leader's term (S%d;T%d) is less than this nodes' term",
			n.me,
			n.currentTerm,
			n.currentRole,
			args.LeaderID,
			args.Term,
		)
		return
	}

	// if the snapshot doesn't cover the existing commit index, discard it
	// this is an invalid snapshot from the perspective of the follower
	if args.LastIncludedIndex < n.commitIndex {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) InstallSnapshot(): discarding request because LastIncludedIndex < commitIndex",
			n.me,
			n.currentTerm,
			n.currentRole,
			args.LeaderID,
			args.Term,
		)
		return
	}

	if args.Term > n.currentTerm {
		// caller's term > current term
		// ensure the node has the up-to-date term
		// and is in a follower state
		n.setNodeTerm(args.Term)
	} else if n.currentRole != rFollower {
		// we don't call setNodeTerm here,
		// because we don't want to change
		// the votedFor state variable
		n.currentRole = rFollower
	}

	// if snapshot already taken, then skip
	if n.snapshotLastIncludedIndex == args.LastIncludedIndex && n.snapshotLastIncludedTerm == args.LastIncludedTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) - InstallSnapshot(): Snapshot already up to date. Suggested index: %d(global); Suggested term: %d(global)",
			n.me,
			n.currentTerm,
			n.currentRole,
			args.LastIncludedIndex,
			args.LastIncludedTerm,
		)

		return
	}

	// keep entries past LastIncludedIndex if the term at that index matches
	// otherwise the local log diverges
	// before the snapshot boundary and must be discarded entirely
	localLast := args.LastIncludedIndex - n.snapshotLastIncludedIndex
	if localLast >= 0 && localLast < len(n.log) && n.log[localLast].Term == args.LastIncludedTerm {
		n.log = append([]transport.LogEntry{{Term: args.LastIncludedTerm}}, n.log[localLast+1:]...)
	} else {
		n.log = []transport.LogEntry{{Term: args.LastIncludedTerm}}
	}
	n.snapshotLastIncludedIndex = args.LastIncludedIndex
	n.snapshotLastIncludedTerm = args.LastIncludedTerm
	n.snapshot = args.Data
	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - InstallSnapshot() - log trimmed to snapshot boundary %d; remaining entries=%d",
		n.me,
		n.currentTerm,
		n.currentRole,
		args.LastIncludedIndex,
		len(n.log)-1,
	)

	// persist the node's state updates
	n.persist()
	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - InstallSnapshot() - state updated. SnapshotIndex='%d'(global)",
		n.me,
		n.currentTerm,
		n.currentRole,
		args.LastIncludedIndex,
	)

	snapshotUpdateMsg := ApplyMsg{
		CommandValid: false,
		Command:      0,
		CommandIndex: 0,

		SnapshotValid: true,
		Snapshot:      n.snapshot,
		SnapshotTerm:  n.snapshotLastIncludedTerm,
		SnapshotIndex: n.snapshotLastIncludedIndex,
	}

	// NOTE: we don't want to hold locks when sending to the channels
	// because that might cause a deadlock

	// explicitly unlock to send the snapshotUpdateMsg to the channel
	n.mu.Unlock()

	// send snapshot to applyCh
	n.applyCh <- snapshotUpdateMsg
	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - InstallSnapshot() - state transmitted for SnapshotIndex='%d'(global)",
		n.me,
		n.currentTerm,
		n.currentRole,
		args.LastIncludedIndex,
	)

	// Update commit index
	// explicitly acquire lock to check
	n.mu.Lock()
	if n.snapshotLastIncludedIndex > n.commitIndex {
		dlog.Dlog(
			dlog.DSnap,
			"S%d; T%d; R(%s) - InstallSnapshot(): updated commit index on follower: %d(old) > %d(new)",
			n.me,
			n.currentTerm,
			n.currentRole,
			n.commitIndex,
			n.snapshotLastIncludedIndex,
		)
		n.commitIndex = n.snapshotLastIncludedIndex
		n.stateUpdateCond.Broadcast()
	}
	// NOTE: the lock is released by the 'defer' used at the top of the function
}

func (n *Node) callWithTimeout(fn func() bool) bool {
	done := make(chan bool)
	cancelled := make(chan struct{})

	go func() {
		var result bool
		if n.killed() {
			result = false
		} else {
			result = fn()
		}
		select {
		case done <- result:
		case <-cancelled:
		}
	}()

	select {
	case result := <-done:
		return result
	case <-time.After(rpcTimeout):
		close(cancelled)
		return false
	}
}

// sendRequestVote sends a RequestVote RPC to a peer. It returns true once
// reply has been populated; false on timeout, dropped call, or unreachable
// peer. The caller should retry at a higher level if a false return would
// be meaningful.
func (n *Node) sendRequestVote(server int, args *transport.RequestVoteArgs, reply *transport.RequestVoteReply) bool {
	return n.callWithTimeout(
		func() bool {
			return n.peers[server].Call(context.Background(), "Raft.RequestVote", args, reply) == nil
		},
	)
}

func (n *Node) sendAppendEntries(
	server int,
	args *transport.AppendEntriesArgs,
	reply *transport.AppendEntriesReply,
) bool {
	return n.callWithTimeout(
		func() bool {
			return n.peers[server].Call(context.Background(), "Raft.AppendEntries", args, reply) == nil
		},
	)
}

func (n *Node) sendInstallSnapshot(
	server int,
	args *transport.InstallSnapshotArgs,
	reply *transport.InstallSnapshotReply,
) bool {
	return n.callWithTimeout(
		func() bool {
			return n.peers[server].Call(context.Background(), "Raft.InstallSnapshot", args, reply) == nil
		},
	)
}

func (n *Node) getNewCommitIndex() int {
	// calculate new (possible) commit index
	matchIndexDesc := make([]int, len(n.matchIndex))
	copy(matchIndexDesc, n.matchIndex)
	sort.Slice(matchIndexDesc, func(i, j int) bool {
		return matchIndexDesc[i] > matchIndexDesc[j]
	})

	majorityMatchIndex := len(n.peers) / 2
	// the index at the middle + 1 of matchIndexDesc is present on the majority of the nodes
	newCommitIndex := matchIndexDesc[majorityMatchIndex]
	newCommitIndexNorm := newCommitIndex - n.snapshotLastIncludedIndex

	if newCommitIndex > 0 && newCommitIndex > n.commitIndex &&
		n.log[newCommitIndexNorm].Term == n.currentTerm {
		// we can have a new commit index
		// the entry is replicated on a majority of followers
		return newCommitIndex
	}

	return n.commitIndex
}

func (n *Node) applyLogEntriesToState(
	applyCh chan ApplyMsg,
	offset int,
	entriesToApply []transport.LogEntry,
) {
	dlog.Dlog(dlog.DTrace, "S%d; - APPLY LOG ENTRIES TO STATE", n.me)

	for i := 0; i < len(entriesToApply) && !n.killed(); i++ {
		// Assuming this works with no error and is not blocking
		applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entriesToApply[i].Command,
			CommandIndex: i + offset,

			SnapshotValid: false,
			Snapshot:      []byte{},
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
	}
}

// update commit index
// and apply newly committed entries to the node's state
func (n *Node) monitorAndUpdateState() {
	n.mu.Lock()
	dlog.Dlog(dlog.DWarn, "S%d; T%d; R(%s) - monitorAndUpdateState", n.me, n.currentTerm, n.currentRole)
	// Important: set the applier cursor at the snapshot boundary, not commitIndex
	// otherwise, an AppendEntries handler can bump commitIndex before this goroutine is scheduled,
	// which would silently skip entries past snapshotLastIncludedIndex
	previousCommitIndex := n.snapshotLastIncludedIndex
	n.mu.Unlock()

	for !n.killed() {
		// acquire lock to be able to call wait
		n.mu.Lock()
		for n.commitIndex == previousCommitIndex && !n.killed() {
			// wait for broadcast
			dlog.Dlog(
				dlog.DCommit,
				"S%d; T%d; R(%s) - Listening for the commit index change...Current: %d",
				n.me,
				n.currentTerm,
				n.currentRole,
				n.commitIndex,
			)
			n.stateUpdateCond.Wait()
		}

		dlog.Dlog(
			dlog.DSuccess,
			"S%d; T%d; R(%s) - Commit index changed to '%d'!",
			n.me,
			n.currentTerm,
			n.currentRole,
			n.commitIndex,
		)

		newCommitIndex := n.commitIndex
		newCommitIndexNorm := max(newCommitIndex-n.snapshotLastIncludedIndex, 0)
		// NOTE: previousCommitIndex can be smaller than snapshotLastIncludedIndex
		// if the previousCommitIndex's value corresponds to a log index that was included
		// in the latest snapshot
		previousCommitIndexNorm := max(previousCommitIndex-n.snapshotLastIncludedIndex, 0)
		dlog.Dlog(
			dlog.DCommit,
			"S%d; T%d; R(%s) - previousCommitIndexNorm=%d(global:'%d');newCommitIndexNorm=%d(global:'%d')",
			n.me,
			n.currentTerm,
			n.currentRole,
			previousCommitIndexNorm,
			previousCommitIndex,
			newCommitIndexNorm,
			newCommitIndex,
		)

		newEntriesRef := n.log[previousCommitIndexNorm+1 : newCommitIndexNorm+1]
		newEntries := make([]transport.LogEntry, len(newEntriesRef))
		copy(newEntries, newEntriesRef)

		applyOffset := previousCommitIndexNorm + n.snapshotLastIncludedIndex + 1
		nodeKilled := n.killed()

		n.mu.Unlock()

		if !nodeKilled {
			n.applyLogEntriesToState(
				n.applyCh,
				applyOffset,
				newEntries,
			)
			previousCommitIndex = newCommitIndex
		} else {
			dlog.Dlog(dlog.DSuccess, "S%d; T%d; R(%s) - THE NODE WAS KILLED", n.me, n.currentTerm, n.currentRole)
		}

	}
}

func (n *Node) appendEntriesOnPeer(
	peerIdx int,
	nodeTerm int,
	leaderCommitIdx int,
	nextIndexCopy []int,
	logCopy []transport.LogEntry,
	snapshotLastIncludedIdx int,
	isHeartbeat bool,
) {
	if n.killed() {
		return
	}

	logTopic := dlog.DLeader
	if isHeartbeat {
		logTopic = dlog.DHeartbit
	}

	// Use nodeTerm + rLeader (captured/known at call entry) in log prefix
	// instead of live n.currentTerm / n.currentRole, which would race with
	// concurrent writers in setNodeTerm on other goroutines.
	dlog.Dlog(
		logTopic,
		"S%d; T%d; R(%s) - sendAppendEntries call to S%d",
		n.me,
		nodeTerm,
		rLeader,
		peerIdx,
	)

	var prevLogTerm int
	operationFinished := false

	for !operationFinished && !n.killed() {
		// node not killed, operation not finished,
		// ensure that the node is still in LEADER role in the INITIAL term before sending the request
		// Its term and its role can be concurrently changed during a new election
		// while this goroutine is still in execution
		n.mu.Lock()
		if n.currentRole != rLeader || n.currentTerm != nodeTerm {
			dlog.Dlog(
				dlog.DInfo,
				"S%d; T%d; R(%s) - Node not a leader or initial term finished. Aborting sendAppendEntries call to S%d",
				n.me,
				n.currentTerm,
				n.currentRole,
				peerIdx,
			)
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		// while the leader figures out which logs slice should be sent to the follower
		prevLogIndex := nextIndexCopy[peerIdx] - 1

		// normalize prev log index if snapshot/s already happened
		// we still need to communicate the unnormalized value with the other nodes
		prevLogIndexNorm := prevLogIndex - snapshotLastIncludedIdx
		if prevLogIndexNorm >= len(logCopy) {
			// NOTE: discard the entire append entries operation
			// this is an edge case that happened because the currently-executing goroutine
			// was scheduled very late in the execution, so late that the node already did multiple snapshots
			// (case in which norm(prevLogInedx) index is outside of the log array
			// the work will be or already has been picked up by another goroutine
			return
		}

		if prevLogIndexNorm >= 0 {
			prevLogTerm = logCopy[prevLogIndexNorm].Term
		} else {
			// TODO: consider separating in its own function
			// prevLogIndex < snapshot
			// this means that the leader is trying to send entries that are already snapshotted
			// and no longer stored in the node's log
			n.mu.Lock()
			installSnapshotReq := &transport.InstallSnapshotArgs{
				Term:              nodeTerm,
				LeaderID:          n.me,
				LastIncludedIndex: n.snapshotLastIncludedIndex,
				LastIncludedTerm:  n.snapshotLastIncludedTerm,
				Data:              n.snapshot,
			}
			n.mu.Unlock()

			dlog.Dlog(dlog.DSnap,
				"S%d; T%d; R(%s) - requesting 'InstallSnapshot' to S%d; lastIncludedIndex=%d, lastIncludedTerm=%d",
				n.me,
				nodeTerm,
				rLeader,
				peerIdx,
				installSnapshotReq.LastIncludedIndex,
				installSnapshotReq.LastIncludedTerm,
			)
			installSnapshotReply := &transport.InstallSnapshotReply{}
			success := n.sendInstallSnapshot(peerIdx, installSnapshotReq, installSnapshotReply)
			dlog.Dlog(dlog.DSnap, "S%d; T%d; R(%s) - 'InstallSnapshot' to S%d finished", n.me, nodeTerm, rLeader, peerIdx)

			if !success {
				// stop InstallSnapshot request due to network unreliability
				dlog.Dlog(
					dlog.DDrop,
					"S%d; T%d; R(%s) - InstallSnapshot request to S%d failed due to network issue. Operation marked as finished.",
					n.me,
					installSnapshotReq.Term,
					rLeader,
					peerIdx,
				)
				operationFinished = true
				// execution is interrupted here
				continue
			}

			n.mu.Lock()
			if installSnapshotReply.Term > nodeTerm {
				// the leader's term is outdated
				// so we need to set the received term on the node
				// and convert it back to follower role
				dlog.Dlog(dlog.DError, "S%d; T%d; R(%s) - InstallSnapshot call: Received higher term from S%d(T%d); converting to follower", n.me, n.currentTerm, n.currentRole, peerIdx, installSnapshotReply.Term)
				n.setNodeTerm(installSnapshotReply.Term)
				n.mu.Unlock()
				continue
			} else {
				// update matchIndex and nextIndex
				// max() guards against regressing values that a concurrent
				// goroutine has already advanced past this snapshot
				n.nextIndex[peerIdx] = max(installSnapshotReq.LastIncludedIndex+1, n.nextIndex[peerIdx])
				n.matchIndex[peerIdx] = max(installSnapshotReq.LastIncludedIndex, n.matchIndex[peerIdx])

				// match index and  index updated
				// for performance reasons, we can check here if the commit index of the leader changed
				newCommitIndex := n.getNewCommitIndex()
				if n.commitIndex < newCommitIndex {
					// commit index on the leader CAN change => we have a majority of followers that have the entries until the commit index
					// send an appendEntries request to these nodes, so that the majority update their commit index
					// This is a performance improvement, because they don't need to wait until the next heartbeat execution

					// can already change the commit index on the leader
					// since we have a majority on the followers
					n.commitIndex = newCommitIndex
					dlog.Dlog(dlog.DCommit, "S%d; T%d; R(%s) - refreshed commit index to '%d' after installing snapshot", n.me, n.currentTerm, n.currentRole, n.commitIndex)
					n.stateUpdateCond.Broadcast()

					// propagate commit index change
					// to the followers
					go n.sendHeartbeat()
				}

				// InstallSnapshot finished, continue executing entries replication
				// with a fresh parameter set
				operationFinished = true
				n.mu.Unlock()
				go n.replicateEntries()
				continue
			}
		}

		// add support to recursively send the entries to followers
		entriesSlice := logCopy[prevLogIndexNorm+1:]
		appendReq := &transport.AppendEntriesArgs{
			Term:         nodeTerm,
			LeaderID:     n.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: leaderCommitIdx,
			Entries:      entriesSlice,
		}
		reply := &transport.AppendEntriesReply{}

		dlog.Dlog(
			dlog.DInfo,
			"S%d; T%d; R(%s) - requesting 'AppendEntries' to S%d",
			n.me,
			appendReq.Term,
			rLeader,
			peerIdx,
		)
		reqSuccess := n.sendAppendEntries(peerIdx, appendReq, reply)

		dlog.Dlog(
			dlog.DInfo,
			"S%d; T%d; R(%s) - finished 'AppendEntries' request to S%d - success: %v",
			n.me,
			appendReq.Term,
			rLeader,
			peerIdx,
			reqSuccess,
		)

		// request completed, take lock to the node state
		n.mu.Lock()
		if !reqSuccess {
			// the append entries request failes due to network issues, after multiple retries
			// therefore, we're "dropping" this request
			dlog.Dlog(
				dlog.DDrop,
				"S%d; T%d; R(%s) - AppendEntries request to S%d failed. Operation marked as finished.",
				n.me,
				appendReq.Term,
				n.currentRole,
				peerIdx,
			)
			operationFinished = true
		} else {
			// the request succeeded on the network (we've got a response)
			if reply.Success {
				// the log entries were replicated successfully on the follower node

				// update matchIndex and nextIndex
				// we want to use max() here, because these vaules can be
				// also updated from other replicateEntries goroutines that
				// could've come AFTER this current replicateEntries execution
				maxReplicatedEntryIndex := prevLogIndex + len(entriesSlice)
				peerNextIndex := maxReplicatedEntryIndex + 1
				n.nextIndex[peerIdx] = max(peerNextIndex, n.nextIndex[peerIdx])
				n.matchIndex[peerIdx] = max(maxReplicatedEntryIndex, n.matchIndex[peerIdx])

				// match index and  index updated
				// for performance reasons, we can check here if the commit index of the leader changed
				newCommitIndex := n.getNewCommitIndex()
				if n.commitIndex < newCommitIndex {
					// commit index on the leader CAN change => we have a majority of followers that have the entries until the commit index
					// send an appendEntries request to these nodes, so that the majority update their commit index
					// This is a performance improvement, because they don't need to wait until the next heartbeat execution

					// can already change the commit index on the leader
					// since we have a majority on the followers
					n.commitIndex = newCommitIndex
					dlog.Dlog(dlog.DCommit, "S%d; T%d; R(%s) - refreshed commit index to '%d'", n.me, n.currentTerm, n.currentRole, n.commitIndex)
					n.stateUpdateCond.Broadcast()

					// propagate commit index change
					// to the followers
					go n.sendHeartbeat()
				}

				dlog.Dlog(logTopic, "S%d; T%d; R(%s) - Slice appended to S%d", n.me, appendReq.Term, n.currentRole, peerIdx)
				operationFinished = true
			} else if reply.Term > n.currentTerm {
				// the leader's term is outdated
				// so we need to set the received term on the node
				// and convert it back to follower role
				dlog.Dlog(dlog.DError, "S%d; T%d; R(%s) - Received higher term from S%d(T%d); converting to follower", n.me, n.currentTerm, n.currentRole, peerIdx, reply.Term)
				n.setNodeTerm(reply.Term)
				operationFinished = true
			} else {
				// the follower's node doesn't contain an entry at prevLogIndex
				// whose term matches prevLogTerm

				nextIndexCopy[peerIdx] = min(reply.EntryTermStartIndex, len(logCopy)+n.snapshotLastIncludedIndex)
				dlog.Dlog(logTopic, "S%d; T%d; R(%s) - Slice not appended to S%d. Changed nextIndexCopy of peer to %d", n.me, appendReq.Term, n.currentRole, peerIdx, nextIndexCopy[peerIdx])
			}
		}
		n.mu.Unlock()
	}
}

func (n *Node) replicateEntries() {
	n.mu.Lock()
	// copy the critical values that the goroutines need
	// because these values can be modified concurrently by other replicateEntries goroutines

	// copy nextIndex
	nextIndexCopy := make([]int, len(n.nextIndex))
	copy(nextIndexCopy, n.nextIndex)
	// copy leader's log
	logCopy := make([]transport.LogEntry, len(n.log))
	copy(logCopy, n.log)

	// copy current values (for accurate log)
	leaderCommitIdx := n.commitIndex
	nodeTerm := n.currentTerm
	snapshotLatestIncludedIdx := n.snapshotLastIncludedIndex
	n.mu.Unlock()

	for peerIdx := range n.peers {
		if n.me == peerIdx {
			// already applied entry on the leader
			continue
		}

		go n.appendEntriesOnPeer(
			peerIdx,
			nodeTerm,
			leaderCommitIdx,
			nextIndexCopy,
			logCopy,
			snapshotLatestIncludedIdx,
			false,
		)

	}
}

// Start requests that command be appended to the replicated log. If this
// peer is not the leader, Start returns immediately with isLeader=false. On
// the leader, it appends to the local log and kicks off replication; there
// is no guarantee the command will be committed, because the leader may
// fail or lose its term. Start also returns gracefully after Kill().
//
// Returns (index, term, isLeader): the index the command will occupy if it
// commits, the current term, and this peer's view of its own leadership.
func (n *Node) Start(command any) (int, int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	nextGlobalLogIdx := len(n.log) + n.snapshotLastIncludedIndex
	term := n.currentTerm
	isLeader := n.currentRole == rLeader

	if !isLeader || n.killed() {
		return nextGlobalLogIdx, term, isLeader
	}
	dlog.Dlog(
		dlog.DClient,
		"S%d; T%d; R(%s) - leader received new entry: %v. Appending to existing log: %v",
		n.me,
		n.currentTerm,
		n.currentRole,
		command,
		n.log,
	)
	// append new command to the leader's log
	n.log = append(n.log, transport.LogEntry{Term: term, Command: command})
	// log changed, persist
	n.persist()
	// update followers' state arrays
	n.matchIndex[n.me] = nextGlobalLogIdx
	n.nextIndex[n.me] = nextGlobalLogIdx + 1

	dlog.Dlog(
		dlog.DClient,
		"S%d; T%d; R(%s) - Entry appended (%v). Replicating on followers",
		n.me,
		n.currentTerm,
		n.currentRole,
		dlog.ToTruncatedArrayItem(command),
	)
	go n.replicateEntries()

	return nextGlobalLogIdx, term, isLeader
}

// Kill halts the node. Long-running goroutines use killed() to
// check whether they should stop. Use of atomic avoids the need for a lock.
func (n *Node) Kill() {
	atomic.StoreInt32(&n.dead, 1)
	// ensure we check again if we have to kill the state monitoring loop
	n.stateUpdateCond.Broadcast()
}

func (n *Node) killed() bool {
	z := atomic.LoadInt32(&n.dead)
	return z == 1
}

// set term for the node
// when this method is called, the node automatically becomes a follower
func (n *Node) setNodeTerm(term int) {
	if n.currentTerm > term {
		// current term can't be of a higher value
		// than the newly set term
		// ignoring request
		return
	}

	if n.killed() {
		// panic("Can't set term on a killed node")
		// Can't set term on a killed node
		return
	}

	n.currentRole = rFollower
	n.currentTerm = term
	n.votedFor = -1
	// Persist change
	n.persist()
}

// increments term
// the node becomes a candidate
// votes on self
func (n *Node) markSelfAsCandidate() {
	n.currentRole = rCandidate
	n.currentTerm += 1
	n.votedFor = n.me
	// persist state change
	n.persist()
}

func (n *Node) electLeader() {
	if n.killed() {
		// node killed
		return
	}

	// staring election for new cluster Leader
	n.mu.Lock()
	n.markSelfAsCandidate()
	// track the term where the election begun
	electionTerm := n.currentTerm
	// supporting votes received during current election
	votesReceived := 1
	// total of votes requests finished
	votesRequestsFinished := 0
	// setup log-related state variables before requesting the votes
	lastLogIdxNorm := len(n.log) - 1
	lastLogIndexGlobal := lastLogIdxNorm + n.snapshotLastIncludedIndex
	lastLogTerm := n.log[lastLogIdxNorm].Term

	n.mu.Unlock()

	dlog.Dlog(dlog.DInfo, "S%d; T%d - leader election started", n.me, n.currentTerm)

	// ask for vote from the other candidates
	c := sync.NewCond(&n.mu)
	for peerIdx := range n.peers {
		if n.me == peerIdx {
			// already voted for self
			continue
		}

		// Q: what could happen concurrently during the votes gathering,
		// so that this current's election state is affected?
		// - election term might end (election timeout was hit)? how's this election "invalidated"?
		// - the node might've received a heartbeat:
		// 		- another candidate won the election having term >= node's term, so the candidate
		// 		should become a follower

		// request votes concurrently
		go func() {
			var voteReply transport.RequestVoteReply

			if n.killed() {
				dlog.Dlog(
					dlog.DDrop,
					"S%d; T%d - Node killed. Aborting requestVote call",
					n.me,
					n.currentTerm,
				)
				return
			}

			// With dummy entry at index 0, log is never empty
			// request vote from the peer
			voteReq := transport.RequestVoteArgs{
				Term:         electionTerm,
				CandidateID:  n.me,
				LastLogIndex: lastLogIndexGlobal,
				LastLogTerm:  lastLogTerm,
			}

			reqSuccess := n.sendRequestVote(peerIdx, &voteReq, &voteReply)

			n.mu.Lock()
			votesRequestsFinished += 1
			if !reqSuccess {
				// request failed, do nothing
				dlog.Dlog(dlog.DDrop, "S%d; T%d - Request to S%d failed", n.me, voteReq.Term, peerIdx)
			} else {
				dlog.Dlog(dlog.DInfo, "S%d; T%d - Request to S%d succeeded", n.me, voteReq.Term, peerIdx)
				if voteReply.Success {
					// rpc request suceeded
					// the term might've been changed meanwhile, so we need
					// to check if the returned term is the same as the election term
					if voteReply.Term == electionTerm {
						votesReceived += 1
					}
				} else {
					// the candidate didn't receive the vote:
					// (a) the candidate's term might be behind
					// (b) the peer already voted for another candidate
					if voteReply.Term > n.currentTerm {
						// if the current's candidate term is behind
						// update the node's term and convert the node back to follower role
						n.setNodeTerm(voteReply.Term)
					}
				}
			}
			c.Broadcast()
			n.mu.Unlock()
		}()
	}

	// the goroutine code will try to write the same shared data read
	// in the for condition ('votesReceived' and 'votesRequestsFinished')
	// to prevent race conditions, we should use a lock
	n.mu.Lock()
	defer n.mu.Unlock()
	expectedVoteRequests := len(n.peers) - 1
	quorum := len(n.peers)/2 + 1
	// we keep the same quorum size, regardless if there are network partitions or not
	for votesReceived < quorum && votesRequestsFinished != expectedVoteRequests {
		c.Wait()
	}

	if votesReceived < quorum {
		dlog.Dlog(dlog.DDrop, "Candidate %v not elected", n.me)
		return
	}

	if n.currentRole != rCandidate || n.currentTerm != electionTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d - Node no longer candidate (role - %s) OR current term changed (term - %d)",
			n.me,
			n.currentRole,
			n.currentTerm,
		)
		return
	}

	dlog.Dlog(dlog.DVote, "S%d; T%d - elected as leader", n.me, n.currentTerm)
	n.currentRole = rLeader
	dlog.Dlog(
		dlog.DLeader,
		"S%d; T%d; R(%s) - init leader-specific state",
		n.me,
		n.currentTerm,
		n.currentRole,
	)
	n.initFollowersArrays()
	dlog.Dlog(
		dlog.DLeader,
		"S%d; T%d; R(%s) - sending first heartbeats as leader",
		n.me,
		n.currentTerm,
		n.currentRole,
	)
	go n.sendHeartbeat()
}

func (n *Node) initFollowersArrays() {
	n.nextIndex = make([]int, len(n.peers))
	n.matchIndex = make([]int, len(n.peers))

	for peerIdx := range n.peers {
		// note: we initialize the entries at the current node's index as well
		// but we're not gonna actually use it in the implementation,
		// because we have the actual node's state that we can use

		// initially: [1, 1, 1, ... ] (pointing past the dummy entry)
		n.nextIndex[peerIdx] = len(n.log) + n.snapshotLastIncludedIndex
		// initially: [0, 0, 0, ...] (dummy entry is "replicated" everywhere)
		n.matchIndex[peerIdx] = 0
	}
}

func (n *Node) sendHeartbeat() {
	// send heartbeat to the peers in the cluster
	// note that here the append entries method
	// also sends any missing entries to the follower
	// having a double role: both sending a heartbeat,
	// but also replicating the entries missing on the follower node

	n.mu.Lock()

	if n.currentRole != rLeader {
		dlog.Dlog(
			dlog.DHeartbit,
			"S%d; T%d; R(%s) - sendHeartbeat - NODE NOT A LEADER ANYMORE, DROPPING REQUEST",
			n.me,
			n.currentTerm,
			n.currentRole,
		)
		// release the lock
		n.mu.Unlock()
		// finish the execution
		return
	}

	// adding here for race conditions protection
	dlog.Dlog(
		dlog.DHeartbit,
		"S%d; T%d; R(%s) - sendHeartbeat - LOCK ACQUIRED",
		n.me,
		n.currentTerm,
		n.currentRole,
	)
	// copy the critical values that the goroutines need
	// because these values can be modified concurrently by other replicateEntries goroutines

	// copy nextIndex
	nextIndexCopy := make([]int, len(n.nextIndex))
	copy(nextIndexCopy, n.nextIndex)
	// copy leader's log
	logCopy := make([]transport.LogEntry, len(n.log))
	copy(logCopy, n.log)

	leaderCommitIdx := n.commitIndex
	nodeTerm := n.currentTerm
	snapshotLatestIncludedIdx := n.snapshotLastIncludedIndex

	// adding here for race conditions protection
	dlog.Dlog(
		dlog.DHeartbit,
		"S%d; T%d; R(%s) - sendHeartbeat - LOCK RELEASED",
		n.me,
		n.currentTerm,
		n.currentRole,
	)
	n.mu.Unlock()

	for peerIdx := range n.peers {
		if n.me == peerIdx {
			continue
		}

		go n.appendEntriesOnPeer(
			peerIdx,
			nodeTerm,
			leaderCommitIdx,
			nextIndexCopy,
			logCopy,
			snapshotLatestIncludedIdx,
			true,
		)
	}
}

func (n *Node) ticker() {
	// stop loop when server gets killed
	for !n.killed() {
		n.mu.Lock()
		isLeader := n.currentRole == rLeader
		n.mu.Unlock()

		if isLeader {
			// send heartbeat to the peers
			// wait until sending the next heartbeat
			time.Sleep(heartbeatInterval)
			go n.sendHeartbeat()
			continue
		}

		// the node is not a leader
		// check if the it received the heartbeat from the leader in the expected interval
		// if not, start leader election
		// i.e, the node becomes a candidate and executes the candidate logic

		// pause for a random amount of time in [electionTimeoutMin, electionTimeoutMax)
		spread := electionTimeoutMax - electionTimeoutMin
		electionTimeout := electionTimeoutMin + time.Duration(rand.Int63n(int64(spread)))

		time.Sleep(electionTimeout)

		// check latest node state
		n.mu.Lock()
		timeSinceLastHeartbeat := time.Since(n.lastHeartbeat)
		isLeader = n.currentRole == rLeader

		dlog.Dlog(
			dlog.DInfo,
			"S%d; T%d; R(%s) - time since last hearbeat: %d ms",
			n.me,
			n.currentTerm,
			n.currentRole,
			timeSinceLastHeartbeat.Milliseconds(),
		)
		n.mu.Unlock()

		if isLeader {
			dlog.Dlog(
				dlog.DLeader,
				"S%d; T%d; R(%s) - Node became leader meanwhile",
				n.me,
				n.currentTerm,
				n.currentRole,
			)
			// the node became a leader during the election timeout
			// so no need to start an election
		} else if timeSinceLastHeartbeat > electionTimeout {
			dlog.Dlog(dlog.DWarn, "S%d; T%d; R(%s) - tshb(%d ms) > electionTimeout (%d ms)", n.me, n.currentTerm, n.currentRole, timeSinceLastHeartbeat.Milliseconds(), electionTimeout.Milliseconds())
			// 1. Follower node: if the election time passed without a heartbeat
			// then we need to start a new election

			// 2. Candidate node: the election time passed without
			// - the node becoming a leader (already checked above)
			// - the node receiving a heartbeat (a new leader was chosen)
			dlog.Dlog(dlog.DInfo, "S%d; T%d; R(%s) - election timeout crossed, starting a leader election", n.me, n.currentTerm, n.currentRole)
			go n.electLeader()
		}
	}
	dlog.Dlog(dlog.DDrop, "S%d; T%d; R(%s) - Node killed", n.me, n.currentTerm, n.currentRole)
}

// Make creates a Raft node. The full set of peers in the cluster is given
// in peers[] and this node's own index in that slice is me. store persists
// the node's durable state across restarts; applyCh is the channel on which
// the caller expects the node to send ApplyMsg messages.
// Make() returns quickly; long-running work is started as goroutines.
func Make(peers []transport.Peer, me int,
	store storage.Store, applyCh chan ApplyMsg,
) (*Node, error) {
	// Callers are responsible for calling Kill() when done so that the
	// long-running goroutines spawned below shut down cleanly;
	// Each goroutine is guarded with killed() to make sure they shut down correctly

	n := &Node{}
	n.peers = peers
	n.store = store
	n.me = me
	n.applyCh = applyCh

	dlog.Dlog(dlog.DTrace, "S%d; - NODE STARTED", n.me)

	// Initialization
	n.stateUpdateCond = sync.NewCond(&n.mu)
	n.currentRole = rFollower
	n.currentTerm = 0
	n.votedFor = -1

	// add dummy entry, to make sure that the implementation 1-based
	n.log = []transport.LogEntry{{Term: 0}}
	n.commitIndex = 0
	n.lastHeartbeat = time.Now()

	// initialize from state persisted before (a potential) crash
	// TODO: remove this initialization
	// n.initFollowersArrays()

	state, err := store.ReadState()
	if err != nil {
		return nil, fmt.Errorf("consensus: reading persisted state: %w", err)
	}
	if err := n.readPersist(state); err != nil {
		return nil, fmt.Errorf("consensus: decoding persisted state: %w", err)
	}
	dlog.Dlog(dlog.DTrace, "S%d; - SERVER JUST STARTED. Node role is '%d'", n.me, n.currentRole)

	// start monitor and state update goroutine
	go n.monitorAndUpdateState()

	// start ticker goroutine to start elections
	go n.ticker()

	return n, nil
}
