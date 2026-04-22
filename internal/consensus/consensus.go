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
const (
	heartbeatInterval  = 100 * time.Millisecond
	rpcTimeout         = 150 * time.Millisecond
	electionTimeoutMin = 250 * time.Millisecond
	electionTimeoutMax = 500 * time.Millisecond
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

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft implements a single peer in the Raft consensus cluster.
type Raft struct {
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
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.currentRole == rLeader
}

// persist saves durable state (term, vote, log, snapshot metadata) to stable storage.
// Must be called before acking any RPC that depends on the
// state being persistent, per Raft's safety guarantees.
// See Figure 2 of the Raft paper for the full list of required persistent state.
func (rf *Raft) persist() {
	blob := new(bytes.Buffer)
	e := gob.NewEncoder(blob)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.snapshotLastIncludedIndex)
	e.Encode(rf.snapshotLastIncludedTerm)
	e.Encode(rf.log)
	_ = rf.store.Save(blob.Bytes(), rf.snapshot)
}

// readPersist restores previously persisted state into rf.
func (rf *Raft) readPersist(data []byte) error {
	if rf.killed() {
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

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.snapshotLastIncludedIndex = snapshotLastIncludedIndex
	rf.snapshotLastIncludedTerm = snapshotLastIncludedTerm
	rf.log = logEntries
	snap, err := rf.store.ReadSnapshot()
	if err != nil {
		return err
	}
	rf.snapshot = snap
	return nil
}

// PersistBytes returns how many bytes are in Raft's persisted log.
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.store.StateSize()
}

// Snapshot is called by the state machine once it has captured its state
// up to the specified index. Raft then trims its log through that index.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// determine node log's (local) index from the global index

	if index <= rf.snapshotLastIncludedIndex {
		dlog.Dlog(
			dlog.DSnap,
			"S%d; T%d; R(%s) - Received command to snapshot until idx %d, but already snapshotted until %d",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			index,
			rf.snapshotLastIncludedIndex,
		)
		return
	}

	localLogIndex := index - rf.snapshotLastIncludedIndex

	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - SNAPSHOT. Snapshot node's state until index '%d'(local); '%d'(global)",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
		localLogIndex,
		index,
	)

	rf.snapshotLastIncludedTerm = rf.log[localLogIndex].Term
	rf.snapshotLastIncludedIndex = index
	rf.snapshot = snapshot
	rf.log = rf.log[localLogIndex:]
	rf.log[0] = transport.LogEntry{Term: rf.snapshotLastIncludedTerm}

	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - SNAPSHOT. Snapshot done - new log array: %v",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
		rf.log,
	)

	rf.persist()
}

// RequestVote handles vote requests from candidates during elections.
func (rf *Raft) RequestVote(args *transport.RequestVoteArgs, reply *transport.RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		*reply = transport.RequestVoteReply{
			Success: false,
			Term:    rf.currentTerm,
		}
		return
	}

	if args.Term < rf.currentTerm {
		// the election is for a past term,
		// so the node doesn't grant the vote
		// and sends its current term to the requesting candidate
		*reply = transport.RequestVoteReply{
			Success: false,
			Term:    rf.currentTerm,
		}
		return
	}

	if args.Term > rf.currentTerm {
		// the candidate node competes in a future term,
		// so our node should be a follower in that term
		rf.setNodeTerm(args.Term)
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		// vote already granted to another candidate
		*reply = transport.RequestVoteReply{
			Success: false,
			Term:    rf.currentTerm,
		}
	} else {
		// With dummy entry at index 0, log is never empty
		latestLogIndexNorm := len(rf.log) - 1
		latestLogIndexGlobal := latestLogIndexNorm + rf.snapshotLastIncludedIndex
		lastLogTerm := rf.log[latestLogIndexNorm].Term

		if args.LastLogTerm > lastLogTerm {
			// the candidate definitely has more up-to-date data
			// than the receiver, which makes it worth having the vote
			// from this peer
			rf.votedFor = args.CandidateID
			// votedFor changed
			rf.persist()

			*reply = transport.RequestVoteReply{
				Success: true,
				Term:    rf.currentTerm,
			}
			return
		}

		// candidate latest log term <= receiver's latest log term
		if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= latestLogIndexGlobal {
			// the candidate's data is at least as up-to-date
			// as the receiver's data, so we can grant it
			// the peer's vote
			rf.votedFor = args.CandidateID
			// votedFor changed
			rf.persist()
			*reply = transport.RequestVoteReply{
				Success: true,
				Term:    rf.currentTerm,
			}
			return
		}

		// none of the above, so the candidate's data makes it
		// not fit for receiving a supporting vote
		*reply = transport.RequestVoteReply{
			Success: false,
			Term:    rf.currentTerm,
		}
	}
}

// AppendEntries handles log-replication RPCs from the leader. It doubles as
// a heartbeat when Entries is empty.
func (rf *Raft) AppendEntries(args *transport.AppendEntriesArgs, reply *transport.AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	dlog.Dlog(
		dlog.DInfo,
		"S%d; T%d; R(%s) - AppendEntries called by S%d.",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
		args.LeaderID,
	)

	logNextIndexGlobal := len(rf.log) + rf.snapshotLastIncludedIndex
	if rf.killed() {
		*reply = transport.AppendEntriesReply{
			Term:                rf.currentTerm,
			Success:             false,
			EntryTerm:           -1,
			EntryTermStartIndex: logNextIndexGlobal,
		}
		return
	}

	rf.lastHeartbeat = time.Now()

	if rf.currentRole == rLeader && args.Term == rf.currentTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) - DROP. The node is a leader in this term and no longer accepts append entries",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
		)
		return
	}

	dlog.Dlog(
		dlog.DInfo,
		"S%d; T%d; R(%s) - handle AppendEntries call from S%d",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
		args.LeaderID,
	)
	if args.Term < rf.currentTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) - DROP. leader's term (S%d;T%d) is less than this nodes' term",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			args.LeaderID,
			args.Term,
		)
		*reply = transport.AppendEntriesReply{
			Term:                rf.currentTerm,
			Success:             false,
			EntryTerm:           -1,
			EntryTermStartIndex: logNextIndexGlobal,
		}
		return
	}

	if args.Term > rf.currentTerm {
		// caller's term > current term
		// ensure the node has the up-to-date term
		// and is in a follower state
		rf.setNodeTerm(args.Term)
	} else if rf.currentRole != rFollower {
		// we don't call setNodeTerm here,
		// because we don't want to change
		// the votedFor state variable
		rf.currentRole = rFollower
	}

	// if the node already compressed its previous log in a snapshot
	// we need to account for this and normalize the prevLogIndex value internally
	prevLogIndexNorm := args.PrevLogIndex - rf.snapshotLastIncludedIndex
	if prevLogIndexNorm < 0 {
		// prevLogIndex entry was already included in the latest snapshot
		*reply = transport.AppendEntriesReply{
			Term:                rf.currentTerm,
			Success:             false,
			EntryTerm:           rf.snapshotLastIncludedTerm,
			EntryTermStartIndex: rf.snapshotLastIncludedIndex + 1,
		}
		return
	}

	// Check if we have the entry at prevLogIndexNorm
	if prevLogIndexNorm >= len(rf.log) {
		// We don't have the entry at prevLogIndexNorm
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) - DROP. Log too short, prevLogIndexNorm %d >= len(log) %d",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			prevLogIndexNorm,
			len(rf.log),
		)
		*reply = transport.AppendEntriesReply{
			Term:                rf.currentTerm,
			Success:             false,
			EntryTerm:           -1,
			EntryTermStartIndex: logNextIndexGlobal,
		}
		return
	}

	if prevLogIndexNorm >= 0 && rf.log[prevLogIndexNorm].Term != args.PrevLogTerm {
		// prevLogIndex term doesn't match
		// the follower's log entries are outdated
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) - DROP. Conflicting prev log index entries at idx %d: '%d'(existing) <> '%d'(received)",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			prevLogIndexNorm,
			rf.log[prevLogIndexNorm].Term,
			args.PrevLogTerm,
		)
		// find the index of the first entry in the term
		termFirstIdx := 0
		for i := prevLogIndexNorm; i > 0; i-- {
			if rf.log[i].Term == rf.log[prevLogIndexNorm].Term {
				termFirstIdx = i
			}
		}

		*reply = transport.AppendEntriesReply{
			Term:                rf.currentTerm,
			Success:             false,
			EntryTerm:           rf.log[prevLogIndexNorm].Term,
			EntryTermStartIndex: termFirstIdx + rf.snapshotLastIncludedIndex,
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
		entriesMergeEndIdx := min(len(rf.log), newEntriesEndIdx)
		offset := prevLogIndexNorm + 1
		for i := offset; i < entriesMergeEndIdx && entriesConflictIdx == -1; i++ {
			if rf.log[i].Term != args.Entries[i-offset].Term {
				entriesConflictIdx = i
			}
		}

		if entriesConflictIdx > -1 {
			// conflict detected
			// slice out the log part starting at the conflict index
			rf.log = rf.log[:entriesConflictIdx]
			// append the remaining slice
			newLogEntries := args.Entries[(entriesConflictIdx - offset):]
			rf.log = append(rf.log, newLogEntries...)
		} else {
			// conflict not detected
			// update log accordingly
			// special case:
			if newEntriesEndIdx > len(rf.log) {
				dlog.Dlog(dlog.DInfo, "S%d; T%d; R(%s) - no entries conflict. Adding entries starting with %d idx: %s", rf.me, rf.currentTerm, rf.currentRole, args.PrevLogIndex+1, dlog.ToTruncatedArrayString(args.Entries))
				dlog.Dlog(dlog.DLog1, "S%d; T%d; R(%s) - New entries idx: %d vs rf.log: %d", rf.me, rf.currentTerm, rf.currentRole, newEntriesEndIdx, len(rf.log))
				// there are new entries to append
				newLogEntries := args.Entries[(entriesMergeEndIdx - offset):]
				rf.log = append(rf.log, newLogEntries...)
				dlog.Dlog(dlog.DLog1, "S%d; T%d; R(%s) - New entries added: %.50v", rf.me, rf.currentTerm, rf.currentRole, dlog.ToTruncatedArrayString(newLogEntries))
			}
		}

		// log changed, persist the change
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		dlog.Dlog(
			dlog.DInfo,
			"S%d; T%d; R(%s) - Leader commit index (%d) vs follower commit index (%d)",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			args.LeaderCommit,
			rf.commitIndex,
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
		rf.commitIndex = min(args.LeaderCommit, lastVerifiedIndexGlobal)
		dlog.Dlog(
			dlog.DLog2,
			"S%d; T%d; R(%s) - Updated commit index on the follower to '%d'",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			rf.commitIndex,
		)
		rf.stateUpdateCond.Broadcast()
	}

	*reply = transport.AppendEntriesReply{
		Term:                rf.currentTerm,
		Success:             true,
		EntryTerm:           rf.currentTerm,
		EntryTermStartIndex: args.PrevLogIndex,
	}

	if len(args.Entries) != 0 {
		dlog.Dlog(
			dlog.DLog1,
			"S%d; T%d; R(%s) - Success response - current entries: %v",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			rf.log,
		)
	} else {
		dlog.Dlog(dlog.DHeartbit, "S%d; T%d; R(%s) - Heartbeat success", rf.me, rf.currentTerm, rf.currentRole)
	}
}

// InstallSnapshot accepts a snapshot pushed by the leader when this follower
// is too far behind to catch up via normal log replication.
func (rf *Raft) InstallSnapshot(args *transport.InstallSnapshotArgs, reply *transport.InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - InstallSnapshot from S%d: LastIncludedIndex=%d, LastIncludedTerm=%d, DataLen=%d",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
		args.LeaderID,
		args.LastIncludedIndex,
		args.LastIncludedTerm,
		len(args.Data),
	)

	// default reply, returned when the execution of the handler is ended
	*reply = transport.InstallSnapshotReply{
		Term: rf.currentTerm,
	}

	if rf.killed() {
		// node already
		dlog.Dlog(
			dlog.DSnap,
			"S%d; T%d; R(%s) - Handling InstallSnapshot call",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
		)
		return
	}

	if rf.currentRole == rLeader && args.Term == rf.currentTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) InstallSnapshot(): The node is a leader in this term and no longer accepts these requests",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
		)
		return
	}
	// refresh last heartbit since InstallSnapshot() can only be called by the leader
	rf.lastHeartbeat = time.Now()

	if args.Term < rf.currentTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) InstallSnapshot(): leader's term (S%d;T%d) is less than this nodes' term",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			args.LeaderID,
			args.Term,
		)
		return
	}

	// if the snapshot doesn't cover the existing commit index, discard it
	// this is an invalid snapshot from the perspective of the follower
	if args.LastIncludedIndex < rf.commitIndex {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) InstallSnapshot(): discarding request because LastIncludedIndex > commitIndex",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			args.LeaderID,
			args.Term,
		)
		return
	}

	if args.Term > rf.currentTerm {
		// caller's term > current term
		// ensure the node has the up-to-date term
		// and is in a follower state
		rf.setNodeTerm(args.Term)
	} else if rf.currentRole != rFollower {
		// we don't call setNodeTerm here,
		// because we don't want to change
		// the votedFor state variable
		rf.currentRole = rFollower
	}

	// if snapshot already taken, then skip
	if rf.snapshotLastIncludedIndex == args.LastIncludedIndex && rf.snapshotLastIncludedTerm == args.LastIncludedTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d; T%d; R(%s) - InstallSnapshot(): Snapshot already up to date. Suggested index: %d(global); Suggested term: %d(global)",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			args.LastIncludedIndex,
			args.LastIncludedTerm,
		)

		return
	}

	rf.snapshotLastIncludedIndex = args.LastIncludedIndex
	rf.snapshotLastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.log = []transport.LogEntry{{Term: args.LastIncludedTerm}}
	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - InstallSnapshot() - log updated to only include dummy for SnapshotIndex='%d'(global)",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
		args.LastIncludedIndex,
	)

	// persist the node's state updates
	rf.persist()
	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - InstallSnapshot() - state updated. SnapshotIndex='%d'(global)",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
		args.LastIncludedIndex,
	)

	snapshotUpdateMsg := ApplyMsg{
		CommandValid: false,
		Command:      0,
		CommandIndex: 0,

		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.snapshotLastIncludedTerm,
		SnapshotIndex: rf.snapshotLastIncludedIndex,
	}

	// NOTE: we don't want to hold locks when sending to the channels
	// because that might cause a deadlock

	// explicitly unlock to send the snapshotUpdateMsg to the channel
	rf.mu.Unlock()

	// send snapshot to applyCh
	rf.applyCh <- snapshotUpdateMsg
	dlog.Dlog(
		dlog.DSnap,
		"S%d; T%d; R(%s) - InstallSnapshot() - state transmitted for SnapshotIndex='%d'(global)",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
		args.LastIncludedIndex,
	)

	// Update commit index
	// explicitly acquire lock to check
	rf.mu.Lock()
	if rf.snapshotLastIncludedIndex > rf.commitIndex {
		dlog.Dlog(
			dlog.DSnap,
			"S%d; T%d; R(%s) - InstallSnapshot(): updated commit index on follower: %d(old) > %d(new)",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			rf.commitIndex,
			rf.snapshotLastIncludedIndex,
		)
		rf.commitIndex = rf.snapshotLastIncludedIndex
		rf.stateUpdateCond.Broadcast()
	}
	// NOTE: the lock is released by the 'defer' used at the top of the function
}

func (rf *Raft) callWithTimeout(fn func() bool) bool {
	done := make(chan bool)
	cancelled := make(chan struct{})

	go func() {
		var result bool
		if rf.killed() {
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
func (rf *Raft) sendRequestVote(server int, args *transport.RequestVoteArgs, reply *transport.RequestVoteReply) bool {
	return rf.callWithTimeout(
		func() bool {
			return rf.peers[server].Call(context.Background(), "Raft.RequestVote", args, reply) == nil
		},
	)
}

func (rf *Raft) sendAppendEntries(
	server int,
	args *transport.AppendEntriesArgs,
	reply *transport.AppendEntriesReply,
) bool {
	return rf.callWithTimeout(
		func() bool {
			return rf.peers[server].Call(context.Background(), "Raft.AppendEntries", args, reply) == nil
		},
	)
}

func (rf *Raft) sendInstallSnapshot(
	server int,
	args *transport.InstallSnapshotArgs,
	reply *transport.InstallSnapshotReply,
) bool {
	return rf.callWithTimeout(
		func() bool {
			return rf.peers[server].Call(context.Background(), "Raft.InstallSnapshot", args, reply) == nil
		},
	)
}

func (rf *Raft) getNewCommitIndex() int {
	// calculate new (possible) commit index
	matchIndexDesc := make([]int, len(rf.matchIndex))
	copy(matchIndexDesc, rf.matchIndex)
	sort.Slice(matchIndexDesc, func(i, j int) bool {
		return matchIndexDesc[i] > matchIndexDesc[j]
	})

	majorityMatchIndex := len(rf.peers) / 2
	// the index at the middle + 1 of matchIndexDesc is present on the majority of the nodes
	newCommitIndex := matchIndexDesc[majorityMatchIndex]
	newCommitIndexNorm := newCommitIndex - rf.snapshotLastIncludedIndex

	if newCommitIndex > 0 && newCommitIndex > rf.commitIndex &&
		rf.log[newCommitIndexNorm].Term == rf.currentTerm {
		// we can have a new commit index
		// the entry is replicated on a majority of followers
		return newCommitIndex
	}

	return rf.commitIndex
}

func (rf *Raft) applyLogEntriesToState(
	applyCh chan ApplyMsg,
	offset int,
	entriesToApply []transport.LogEntry,
) {
	dlog.Dlog(dlog.DTrace, "S%d; - APPLY LOG ENTRIES TO STATE", rf.me)

	for i := 0; i < len(entriesToApply) && !rf.killed(); i++ {
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
func (rf *Raft) monitorAndUpdateState() {
	dlog.Dlog(dlog.DWarn, "S%d; T%d; R(%s) - monitorAndUpdateState", rf.me, rf.currentTerm, rf.currentRole)
	// take the lock to correctly initialize the latest applied commit index
	rf.mu.Lock()
	previousCommitIndex := rf.commitIndex
	rf.mu.Unlock()

	for !rf.killed() {
		// acquire lock to be able to call wait
		rf.mu.Lock()
		for rf.commitIndex == previousCommitIndex && !rf.killed() {
			// wait for broadcast
			dlog.Dlog(
				dlog.DCommit,
				"S%d; T%d; R(%s) - Listening for the commit index change...Current: %d",
				rf.me,
				rf.currentTerm,
				rf.currentRole,
				rf.commitIndex,
			)
			rf.stateUpdateCond.Wait()
		}

		dlog.Dlog(
			dlog.DSuccess,
			"S%d; T%d; R(%s) - Commit index changed to '%d'!",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			rf.commitIndex,
		)

		newCommitIndex := rf.commitIndex
		newCommitIndexNorm := max(newCommitIndex-rf.snapshotLastIncludedIndex, 0)
		// NOTE: previousCommitIndex can be smaller than snapshotLastIncludedIndex
		// if the previousCommitIndex's value corresponds to a log index that was included
		// in the latest snapshot
		previousCommitIndexNorm := max(previousCommitIndex-rf.snapshotLastIncludedIndex, 0)
		dlog.Dlog(
			dlog.DCommit,
			"S%d; T%d; R(%s) - previousCommitIndexNorm=%d(global:'%d');newCommitIndexNorm=%d(global:'%d')",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			previousCommitIndexNorm,
			previousCommitIndex,
			newCommitIndexNorm,
			newCommitIndex,
		)

		newEntriesRef := rf.log[previousCommitIndexNorm+1 : newCommitIndexNorm+1]
		newEntries := make([]transport.LogEntry, len(newEntriesRef))
		copy(newEntries, newEntriesRef)

		applyOffset := previousCommitIndexNorm + rf.snapshotLastIncludedIndex + 1
		nodeKilled := rf.killed()

		rf.mu.Unlock()

		if !nodeKilled {
			rf.applyLogEntriesToState(
				rf.applyCh,
				applyOffset,
				newEntries,
			)
			previousCommitIndex = newCommitIndex
		} else {
			dlog.Dlog(dlog.DSuccess, "S%d; T%d; R(%s) - THE NODE WAS KILLED", rf.me, rf.currentTerm, rf.currentRole)
		}

	}
}

func (rf *Raft) appendEntriesOnPeer(
	peerIdx int,
	nodeTerm int,
	leaderCommitIdx int,
	nextIndexCopy []int,
	logCopy []transport.LogEntry,
	snapshotLastIncludedIdx int,
	isHeartbeat bool,
) {
	if rf.killed() {
		return
	}

	logTopic := dlog.DLeader
	if isHeartbeat {
		logTopic = dlog.DHeartbit
	}

	dlog.Dlog(
		logTopic,
		"S%d; T%d; R(%s) - sendAppendEntries call to S%d",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
		peerIdx,
	)

	var prevLogTerm int
	operationFinished := false

	for !operationFinished && !rf.killed() {
		// node not killed, operation not finished,
		// ensure that the node is still in LEADER role in the INITIAL term before sending the request
		// Its term and its role can be concurrently changed during a new election
		// while this goroutine is still in execution
		rf.mu.Lock()
		if rf.currentRole != rLeader || rf.currentTerm != nodeTerm {
			dlog.Dlog(
				dlog.DInfo,
				"S%d; T%d; R(%s) - Node not a leader or initial term finished. Aborting sendAppendEntries call to S%d",
				rf.me,
				rf.currentTerm,
				rf.currentRole,
				peerIdx,
			)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

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
			rf.mu.Lock()
			installSnapshotReq := &transport.InstallSnapshotArgs{
				Term:              nodeTerm,
				LeaderID:          rf.me,
				LastIncludedIndex: rf.snapshotLastIncludedIndex,
				LastIncludedTerm:  rf.snapshotLastIncludedTerm,
				Data:              rf.snapshot,
			}
			rf.mu.Unlock()

			dlog.Dlog(dlog.DSnap,
				"S%d; T%d; R(%s) - requesting 'InstallSnapshot' to S%d; lastIncludedIndex=%d, lastIncludedTerm=%d",
				rf.me,
				nodeTerm,
				rf.currentRole,
				peerIdx,
				installSnapshotReq.LastIncludedIndex,
				installSnapshotReq.LastIncludedTerm,
			)
			installSnapshotReply := &transport.InstallSnapshotReply{}
			success := rf.sendInstallSnapshot(peerIdx, installSnapshotReq, installSnapshotReply)
			dlog.Dlog(dlog.DSnap, "S%d; T%d; R(%s) - 'InstallSnapshot' to S%d finished", rf.me, nodeTerm, rf.currentRole, peerIdx)

			if !success {
				// stop InstallSnapshot request due to network unreliability
				dlog.Dlog(
					dlog.DDrop,
					"S%d; T%d; R(%s) - InstallSnapshot request to S%d failed due to network issue. Operation marked as finished.",
					rf.me,
					installSnapshotReq.Term,
					rf.currentRole,
					peerIdx,
				)
				operationFinished = true
				// execution is interrupted here
				continue
			}

			rf.mu.Lock()
			if installSnapshotReply.Term > nodeTerm {
				// the leader's term is outdated
				// so we need to set the received term on the node
				// and convert it back to follower role
				dlog.Dlog(dlog.DError, "S%d; T%d; R(%s) - InstallSnapshot call: Received higher term from S%d(T%d); converting to follower", rf.me, rf.currentTerm, rf.currentRole, peerIdx, installSnapshotReply.Term)
				rf.setNodeTerm(installSnapshotReply.Term)
				rf.mu.Unlock()
				continue
			} else {
				// update matchIndex and nextIndex
				// we want to use max() here, because these vaules can be
				// also updated from other replicateEntries goroutines that
				// could've come AFTER this current replicateEntries execution
				rf.nextIndex[peerIdx] = installSnapshotReq.LastIncludedIndex + 1
				rf.matchIndex[peerIdx] = installSnapshotReq.LastIncludedIndex

				// match index and  index updated
				// for performance reasons, we can check here if the commit index of the leader changed
				newCommitIndex := rf.getNewCommitIndex()
				if rf.commitIndex < newCommitIndex {
					// commit index on the leader CAN change => we have a majority of followers that have the entries until the commit index
					// send an appendEntries request to these nodes, so that the majority update their commit index
					// This is a performance improvement, because they don't need to wait until the next heartbeat execution

					// can already change the commit index on the leader
					// since we have a majority on the followers
					rf.commitIndex = newCommitIndex
					dlog.Dlog(dlog.DCommit, "S%d; T%d; R(%s) - refreshed commit index to '%d' after installing snapshot", rf.me, rf.currentTerm, rf.currentRole, rf.commitIndex)
					rf.stateUpdateCond.Broadcast()

					// propagate commit index change
					// to the followers
					go rf.sendHeartbeat()
				}

				if len(logCopy) == 1 {
					// the node has only the dummy snapshot entry in the log, so we don't need to send
					// any remaining log entries
					operationFinished = true
					rf.mu.Unlock()
					continue
				}

				// update nextIndex copy for the next iteration
				nextIndexCopy[peerIdx] = 1
				prevLogTerm = installSnapshotReq.LastIncludedTerm
				prevLogIndex = installSnapshotReq.LastIncludedIndex
				prevLogIndexNorm = rf.nextIndex[peerIdx] - installSnapshotReq.LastIncludedIndex - 1
				dlog.Dlog(
					dlog.DSnap,
					"S%d; T%d; R(%s) - InstallSnapshot: sending the remaining log entries to S%d: prevLogIndexNorm - '%d'; prevLogTerm - '%d'",
					rf.me,
					installSnapshotReq.Term,
					rf.currentRole,
					peerIdx,
					prevLogIndexNorm,
					prevLogTerm,
				)
				rf.mu.Unlock()
			}
		}

		// add support to recursively send the entries to followers
		entriesSlice := logCopy[prevLogIndexNorm+1:]
		appendReq := &transport.AppendEntriesArgs{
			Term:         nodeTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: leaderCommitIdx,
			Entries:      entriesSlice,
		}
		reply := &transport.AppendEntriesReply{}

		dlog.Dlog(
			dlog.DInfo,
			"S%d; T%d; R(%s) - requesting 'AppendEntries' to S%d",
			rf.me,
			appendReq.Term,
			rf.currentRole,
			peerIdx,
		)
		reqSuccess := rf.sendAppendEntries(peerIdx, appendReq, reply)

		dlog.Dlog(
			dlog.DInfo,
			"S%d; T%d; R(%s) - finished 'AppendEntries' request to S%d - success: %v",
			rf.me,
			appendReq.Term,
			rf.currentRole,
			peerIdx,
			reqSuccess,
		)

		// request completed, take lock to the node state
		rf.mu.Lock()
		if !reqSuccess {
			// the append entries request failes due to network issues, after multiple retries
			// therefore, we're "dropping" this request
			dlog.Dlog(
				dlog.DDrop,
				"S%d; T%d; R(%s) - AppendEntries request to S%d failed. Operation marked as finished.",
				rf.me,
				appendReq.Term,
				rf.currentRole,
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
				rf.nextIndex[peerIdx] = max(peerNextIndex, rf.nextIndex[peerIdx])
				rf.matchIndex[peerIdx] = maxReplicatedEntryIndex

				// match index and  index updated
				// for performance reasons, we can check here if the commit index of the leader changed
				newCommitIndex := rf.getNewCommitIndex()
				if rf.commitIndex < newCommitIndex {
					// commit index on the leader CAN change => we have a majority of followers that have the entries until the commit index
					// send an appendEntries request to these nodes, so that the majority update their commit index
					// This is a performance improvement, because they don't need to wait until the next heartbeat execution

					// can already change the commit index on the leader
					// since we have a majority on the followers
					rf.commitIndex = newCommitIndex
					dlog.Dlog(dlog.DCommit, "S%d; T%d; R(%s) - refreshed commit index to '%d'", rf.me, rf.currentTerm, rf.currentRole, rf.commitIndex)
					rf.stateUpdateCond.Broadcast()

					// propagate commit index change
					// to the followers
					go rf.sendHeartbeat()
				}

				dlog.Dlog(logTopic, "S%d; T%d; R(%s) - Slice appended to S%d", rf.me, appendReq.Term, rf.currentRole, peerIdx)
				operationFinished = true
			} else if reply.Term > rf.currentTerm {
				// the leader's term is outdated
				// so we need to set the received term on the node
				// and convert it back to follower role
				dlog.Dlog(dlog.DError, "S%d; T%d; R(%s) - Received higher term from S%d(T%d); converting to follower", rf.me, rf.currentTerm, rf.currentRole, peerIdx, reply.Term)
				rf.setNodeTerm(reply.Term)
				operationFinished = true
			} else {
				// the follower's node doesn't contain an entry at prevLogIndex
				// whose term matches prevLogTerm

				nextIndexCopy[peerIdx] = min(reply.EntryTermStartIndex, len(logCopy)+rf.snapshotLastIncludedIndex)
				dlog.Dlog(logTopic, "S%d; T%d; R(%s) - Slice not appended to S%d. Changed nextIndexCopy of peer to %d", rf.me, appendReq.Term, rf.currentRole, peerIdx, nextIndexCopy[peerIdx])
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) replicateEntries() {
	rf.mu.Lock()
	// copy the critical values that the goroutines need
	// because these values can be modified concurrently by other replicateEntries goroutines

	// copy nextIndex
	nextIndexCopy := make([]int, len(rf.nextIndex))
	copy(nextIndexCopy, rf.nextIndex)
	// copy leader's log
	logCopy := make([]transport.LogEntry, len(rf.log))
	copy(logCopy, rf.log)

	// copy current values (for accurate log)
	leaderCommitIdx := rf.commitIndex
	nodeTerm := rf.currentTerm
	snapshotLatestIncludedIdx := rf.snapshotLastIncludedIndex
	rf.mu.Unlock()

	for peerIdx := range rf.peers {
		if rf.me == peerIdx {
			// already applied entry on the leader
			continue
		}

		go rf.appendEntriesOnPeer(
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
func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	nextGlobalLogIdx := len(rf.log) + rf.snapshotLastIncludedIndex
	term := rf.currentTerm
	isLeader := rf.currentRole == rLeader

	if !isLeader || rf.killed() {
		return nextGlobalLogIdx, term, isLeader
	}
	dlog.Dlog(
		dlog.DClient,
		"S%d; T%d; R(%s) - leader received new entry: %v. Appending to existing log: %v",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
		command,
		rf.log,
	)
	// append new command to the leader's log
	rf.log = append(rf.log, transport.LogEntry{Term: term, Command: command})
	// log changed, persist
	rf.persist()
	// update followers' state arrays
	rf.matchIndex[rf.me] = nextGlobalLogIdx
	rf.nextIndex[rf.me] = nextGlobalLogIdx + 1

	dlog.Dlog(
		dlog.DClient,
		"S%d; T%d; R(%s) - Entry appended (%v). Replicating on followers",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
		dlog.ToTruncatedArrayItem(command),
	)
	go rf.replicateEntries()

	return nextGlobalLogIdx, term, isLeader
}

// Kill halts the Raft instance. Long-running goroutines use killed() to
// check whether they should stop. Use of atomic avoids the need for a lock.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// ensure we check again if we have to kill the state monitoring loop
	rf.stateUpdateCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// set term for the node
// when this method is called, the node automatically becomes a follower
func (rf *Raft) setNodeTerm(term int) {
	if rf.currentTerm > term {
		// current term can't be of a higher value
		// than the newly set term
		// ignoring request
		return
	}

	if rf.killed() {
		// panic("Can't set term on a killed node")
		// Can't set term on a killed node
		return
	}

	rf.currentRole = rFollower
	rf.currentTerm = term
	rf.votedFor = -1
	// Persist change
	rf.persist()
}

// increments term
// the node becomes a candidate
// votes on self
func (rf *Raft) markSelfAsCandidate() {
	rf.currentRole = rCandidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	// persist state change
	rf.persist()
}

func (rf *Raft) electLeader() {
	// staring election for new cluster Leader
	rf.mu.Lock()
	rf.markSelfAsCandidate()
	// track the term where the election begun
	electionTerm := rf.currentTerm
	// supporting votes received during current election
	votesReceived := 1
	// total of votes requests finished
	votesRequestsFinished := 0
	// setup log-related state variables before requesting the votes
	lastLogIdxNorm := len(rf.log) - 1
	lastLogIndexGlobal := lastLogIdxNorm + rf.snapshotLastIncludedIndex
	lastLogTerm := rf.log[lastLogIdxNorm].Term

	rf.mu.Unlock()

	dlog.Dlog(dlog.DInfo, "S%d; T%d - leader election started", rf.me, rf.currentTerm)

	// ask for vote from the other candidates
	c := sync.NewCond(&rf.mu)
	for peerIdx := range rf.peers {
		if rf.me == peerIdx {
			// already voted for self
			continue
		}

		// Q: what could happen concurrently during the votes gathering,
		// so that this current's election state is affected?
		// - election term might end (election timeout was hit)? how's this election "invalidated"?
		// - the node might've received a heartbeat:
		// 		a. another candidate won the election having term >= node's term, so the candidate
		// 		should become a follower

		// request votes concurrently
		go func() {
			var voteReply transport.RequestVoteReply

			if rf.killed() {
				dlog.Dlog(
					dlog.DDrop,
					"S%d; T%d - Node killed. Aborting requestVote call",
					rf.me,
					rf.currentTerm,
				)
				return
			}

			// With dummy entry at index 0, log is never empty
			// request vote from the peer
			voteReq := transport.RequestVoteArgs{
				Term:         electionTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndexGlobal,
				LastLogTerm:  lastLogTerm,
			}

			reqSuccess := rf.sendRequestVote(peerIdx, &voteReq, &voteReply)

			rf.mu.Lock()
			votesRequestsFinished += 1
			if !reqSuccess {
				// request failed, do nothing
				dlog.Dlog(dlog.DDrop, "S%d; T%d - Request to S%d failed", rf.me, voteReq.Term, peerIdx)
			} else {
				dlog.Dlog(dlog.DInfo, "S%d; T%d - Request to S%d succeeded", rf.me, voteReq.Term, peerIdx)
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
					if voteReply.Term > rf.currentTerm {
						// if the current's candidate term is behind
						// update the node's term and convert the node back to follower role
						rf.setNodeTerm(voteReply.Term)
					}
				}
			}
			c.Broadcast()
			rf.mu.Unlock()
		}()
	}

	// the goroutine code will try to write the same shared data read
	// in the for condition ('votesReceived' and 'votesRequestsFinished')
	// to prevent race conditions, we should use a lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
	expectedVoteRequests := len(rf.peers) - 1
	quorum := len(rf.peers)/2 + 1
	// we keep the same quorum size, regardless if there are network partitions or not
	for votesReceived < quorum && votesRequestsFinished != expectedVoteRequests {
		c.Wait()
	}

	if votesReceived < quorum {
		dlog.Dlog(dlog.DDrop, "Candidate %v not elected", rf.me)
		return
	}

	if rf.currentRole != rCandidate || rf.currentTerm != electionTerm {
		dlog.Dlog(
			dlog.DDrop,
			"S%d - Node no longer candidate (role - %s) OR current term changed (term - %d)",
			rf.me,
			rf.currentRole,
			rf.currentTerm,
		)
		return
	}

	dlog.Dlog(dlog.DVote, "S%d; T%d - elected as leader", rf.me, rf.currentTerm)
	rf.currentRole = rLeader
	dlog.Dlog(
		dlog.DLeader,
		"S%d; T%d; R(%s) - init leader-specific state",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
	)
	rf.initFollowersArrays()
	dlog.Dlog(
		dlog.DLeader,
		"S%d; T%d; R(%s) - sending first heartbeats as leader",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
	)
	go rf.sendHeartbeat()
}

func (rf *Raft) initFollowersArrays() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for peerIdx := range rf.peers {
		// note: we initialize the entries at the current node's index as well
		// but we're not gonna actually use it in the implementation,
		// because we have the actual node's state that we can use

		// initially: [1, 1, 1, ... ] (pointing past the dummy entry)
		rf.nextIndex[peerIdx] = len(rf.log) + rf.snapshotLastIncludedIndex
		// initially: [0, 0, 0, ...] (dummy entry is "replicated" everywhere)
		rf.matchIndex[peerIdx] = 0
	}
}

func (rf *Raft) sendHeartbeat() {
	// send heartbeat to the peers in the cluster
	// note that here the append entries method
	// also sends any missing entries to the follower
	// having a double role: both sending a heartbeat,
	// but also replicating the entries missing on the follower node

	rf.mu.Lock()

	if rf.currentRole != rLeader {
		dlog.Dlog(
			dlog.DHeartbit,
			"S%d; T%d; R(%s) - sendHeartbeat - NODE NOT A LEADER ANYMORE, DROPPING REQUEST",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
		)
		// release the lock
		rf.mu.Unlock()
		// finish the execution
		return
	}

	// adding here for race conditions protection
	dlog.Dlog(
		dlog.DHeartbit,
		"S%d; T%d; R(%s) - sendHeartbeat - LOCK ACQUIRED",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
	)
	// copy the critical values that the goroutines need
	// because these values can be modified concurrently by other replicateEntries goroutines

	// copy nextIndex
	nextIndexCopy := make([]int, len(rf.nextIndex))
	copy(nextIndexCopy, rf.nextIndex)
	// copy leader's log
	logCopy := make([]transport.LogEntry, len(rf.log))
	copy(logCopy, rf.log)

	leaderCommitIdx := rf.commitIndex
	nodeTerm := rf.currentTerm
	snapshotLatestIncludedIdx := rf.snapshotLastIncludedIndex

	// adding here for race conditions protection
	dlog.Dlog(
		dlog.DHeartbit,
		"S%d; T%d; R(%s) - sendHeartbeat - LOCK RELEASED",
		rf.me,
		rf.currentTerm,
		rf.currentRole,
	)
	rf.mu.Unlock()

	for peerIdx := range rf.peers {
		if rf.me == peerIdx {
			continue
		}

		go rf.appendEntriesOnPeer(
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

func (rf *Raft) ticker() {
	// stop loop when server gets killed
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.currentRole == rLeader
		rf.mu.Unlock()

		if isLeader {
			// send heartbeat to the peers
			// wait until sending the next heartbeat
			time.Sleep(heartbeatInterval)
			go rf.sendHeartbeat()
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
		rf.mu.Lock()
		timeSinceLastHeartbeat := time.Since(rf.lastHeartbeat)
		isLeader = rf.currentRole == rLeader

		dlog.Dlog(
			dlog.DInfo,
			"S%d; T%d; R(%s) - time since last hearbeat: %d ms",
			rf.me,
			rf.currentTerm,
			rf.currentRole,
			timeSinceLastHeartbeat.Milliseconds(),
		)
		rf.mu.Unlock()

		if isLeader {
			dlog.Dlog(
				dlog.DLeader,
				"S%d; T%d; R(%s) - Node became leader meanwhile",
				rf.me,
				rf.currentTerm,
				rf.currentRole,
			)
			// the node became a leader during the election timeout
			// so no need to start an election
		} else if timeSinceLastHeartbeat > electionTimeout {
			dlog.Dlog(dlog.DWarn, "S%d; T%d; R(%s) - tshb(%d ms) > electionTimeout (%d ms)", rf.me, rf.currentTerm, rf.currentRole, timeSinceLastHeartbeat.Milliseconds(), electionTimeout.Milliseconds())
			// 1. Follower node: if the election time passed without a heartbeat
			// then we need to start a new election

			// 2. Candidate node: the election time passed without
			// - the node becoming a leader (already checked above)
			// - the node receiving a heartbeat (a new leader was chosen)
			dlog.Dlog(dlog.DInfo, "S%d; T%d; R(%s) - election timeout crossed, starting a leader election", rf.me, rf.currentTerm, rf.currentRole)
			if !rf.killed() {
				go rf.electLeader()
			}
		}
	}
	dlog.Dlog(dlog.DDrop, "S%d; T%d; R(%s) - Node killed", rf.me, rf.currentTerm, rf.currentRole)
}

// Make creates a Raft server. The ports of all the Raft servers (including
// this one) are in peers[]. This server's index in that slice is me. store
// persists this server's durable state across restarts; applyCh is the
// channel on which the caller expects Raft to send ApplyMsg messages.
// Make() returns quickly; long-running work is started as goroutines.
func Make(peers []transport.Peer, me int,
	store storage.Store, applyCh chan ApplyMsg,
) (*Raft, error) {
	// Callers are responsible for calling Kill() when done so that the
	// long-running goroutines spawned below shut down cleanly; guard
	// goroutines with killed() to exit on cancellation.

	rf := &Raft{}
	rf.peers = peers
	rf.store = store
	rf.me = me
	rf.applyCh = applyCh

	dlog.Dlog(dlog.DTrace, "S%d; - SERVER JUST STARTED", rf.me)
	// Initialization
	rf.stateUpdateCond = sync.NewCond(&rf.mu)
	rf.currentRole = rFollower
	rf.currentTerm = 0
	rf.votedFor = -1
	// add dummy entry, to have the implementation 1-based
	rf.log = []transport.LogEntry{{Term: 0}}

	rf.commitIndex = 0
	rf.lastHeartbeat = time.Now()

	// initialize from state persisted before a crash
	// force init here, but not sure it's the best approach
	// we might want to include the next and match indexes in the persisted state
	rf.initFollowersArrays()
	state, err := store.ReadState()
	if err != nil {
		return nil, fmt.Errorf("consensus: reading persisted state: %w", err)
	}
	if err := rf.readPersist(state); err != nil {
		return nil, fmt.Errorf("consensus: decoding persisted state: %w", err)
	}
	dlog.Dlog(dlog.DTrace, "S%d; - SERVER JUST STARTED. Node role is '%d'", rf.me, rf.currentRole)

	// start monitor and state update goroutine
	go rf.monitorAndUpdateState()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf, nil
}
