// Package rsm wraps a consensus peer with a user-supplied state machine.
package rsm

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conache/raft-go/internal/consensus"
	"github.com/conache/raft-go/internal/dlog"
	"github.com/conache/raft-go/storage"
	"github.com/conache/raft-go/transport"
)

// Op is the envelope every Submit sends through the log; it must be
// registered for gob so it round-trips through the in-memory transport
// (and any other gob-based wire format)
func init() { gob.Register(Op{}) }

// useRaftStateMachine lets a test plug in its own consensus instance
// instead of letting MakeRSM construct one. When true, MakeRSM leaves
// rsm.rf nil and the caller is responsible for assigning it before any
// operations run.
var useRaftStateMachine bool

// Err identifies the outcome of Submit.
type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
)

// Raft is the full consensus surface backing an RSM
// *consensus.Node satisfies this interface
// rsm itself only calls the proposal/snapshot side; the RPC handlers
// (AppendEntries, RequestVote, InstallSnapshot) are exposed so a transport
// (e.g. transport/grpc) can register the same value as its inbound handler
type Raft interface {
	Start(command any) (int, int, bool)
	GetState() (int, bool)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int
	Kill()

	AppendEntries(args *transport.AppendEntriesArgs, reply *transport.AppendEntriesReply)
	RequestVote(args *transport.RequestVoteArgs, reply *transport.RequestVoteReply)
	InstallSnapshot(args *transport.InstallSnapshotArgs, reply *transport.InstallSnapshotReply)
}

type UID int

type ResponseCode int

const (
	RCNotApplied ResponseCode = -1
	RCPending    ResponseCode = iota
	RCApplied
)

type Op struct {
	Me      int
	ID      UID
	Req     any
	ResCode ResponseCode
	Result  any
}

// StateMachine - A server that wants to replicate itself calls MakeRSM and must
// implement the StateMachine interface. This interface allows the rsm
// package to interact with the server for server-specific operations:
// the server must implement DoOp to execute an operation (e.g., a Get
// or Put request), and Snapshot/Restore to snapshot and restore the
// server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu                 sync.Mutex
	me                 int
	rf                 Raft
	applyCh            chan consensus.ApplyMsg
	maxraftstate       int // snapshot if log grows this big
	sm                 StateMachine
	opReqCh            map[UID](chan Op)
	expectedOpByLogIdx map[int]Op
	dead               int32
	done               chan struct{}
}

// MakeRSM - peers[] contains the transport endpoints of the set of nodes that
// will cooperate via Raft to form the fault-tolerant service.
//
// me is the index of the current node in peers[].
//
// store persists Raft's durable state and the latest snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(peers []transport.Peer, me int, store storage.Store, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:                 me,
		maxraftstate:       maxraftstate,
		applyCh:            make(chan consensus.ApplyMsg),
		sm:                 sm,
		opReqCh:            make(map[UID]chan Op),
		expectedOpByLogIdx: make(map[int]Op),
		done:               make(chan struct{}),
	}
	if !useRaftStateMachine {
		node, err := consensus.Make(peers, me, store, rsm.applyCh)
		if err != nil {
			panic(fmt.Sprintf("rsm: consensus.Make failed: %v", err))
		}
		rsm.rf = node
	}

	existingSnapshot, err := store.ReadSnapshot()
	if err != nil {
		panic(fmt.Sprintf("rsm: store.ReadSnapshot failed: %v", err))
	}
	if len(existingSnapshot) > 0 {
		rsm.sm.Restore(existingSnapshot)
	}

	go rsm.listenToApplyCh()
	return rsm
}

func (rsm *RSM) Raft() Raft {
	return rsm.rf
}

// function that checks if the persisted raft state
// is above the max threshold
func (rsm *RSM) shouldTriggerSnapshot() bool {
	if rsm.maxraftstate < 0 || rsm.Raft().PersistBytes() < rsm.maxraftstate {
		return false
	}

	return true
}

func (rsm *RSM) listenToApplyCh() {
	lastAppliedCommandIndex := -1

	for {
		var applyMsg consensus.ApplyMsg
		select {
		case applyMsg = <-rsm.applyCh:
		case <-rsm.done:
			return
		}
		if applyMsg.SnapshotValid {
			// a snapshot was installed on the raft layer
			lastAppliedCommandIndex = max(applyMsg.SnapshotIndex, lastAppliedCommandIndex)
			rsm.sm.Restore(applyMsg.Snapshot)
			continue
		}

		rsm.mu.Lock()
		expectedOp, opExists := rsm.expectedOpByLogIdx[applyMsg.CommandIndex]
		rsm.mu.Unlock()

		if opExists {
			resOp := expectedOp
			isMatch := expectedOp.ID == applyMsg.Command.(Op).ID

			if isMatch {
				// the operation log idx matches the expected idx
				// which means that either the node was a leader and didn't crash
				// or it was a leader that crashed, and the new leader had the
				// new entry in its log
				resOp.ResCode = RCApplied
			} else {
				// the operation was initially sent to the node, which was a leader that eventually died
				// and was replaced by another leader with a different log
				// we need to inform the main Submit() process about this
				resOp.ResCode = RCNotApplied
			}

			rsm.mu.Lock()
			if isMatch {
				// request the sm server to apply the operation
				// our submit's op committed at this index; capture the result for Submit
				resOp.Result = rsm.sm.DoOp(resOp.Req)
			} else {
				// the log entry at this index belongs to a different op (log was
				// overwritten by a new leader)
				//
				// apply the actual log entry so the
				// state machine stays consistent with the log
				rsm.sm.DoOp(applyMsg.Command.(Op).Req)
			}
			lastAppliedCommandIndex = applyMsg.CommandIndex

			reqCh, chExists := rsm.opReqCh[expectedOp.ID]
			rsm.mu.Unlock()

			// sending operation execution results so that Submits returns accordingly
			if chExists {
				reqCh <- resOp
			}
		} else {
			// the operation was initiated from the leader node
			// the current RSM is not a leader
			// we just need to tell the server to apply it

			resOp := applyMsg.Command.(Op)
			rsm.sm.DoOp(resOp.Req)
			lastAppliedCommandIndex = applyMsg.CommandIndex
		}

		if rsm.shouldTriggerSnapshot() {
			// trigger snapshot on the server
			// and transmit it to the raft layer
			snap := rsm.sm.Snapshot()
			rsm.Raft().Snapshot(lastAppliedCommandIndex, snap)
		}
	}
}

func (rsm *RSM) Kill() {
	atomic.StoreInt32(&rsm.dead, 1)
	close(rsm.done)
	rsm.Raft().Kill()
}

func (rsm *RSM) killed() bool {
	return atomic.LoadInt32(&rsm.dead) == 1
}

func (rsm *RSM) cleanup(opID UID, opLogIdx int) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	delete(rsm.opReqCh, opID)
	delete(rsm.expectedOpByLogIdx, opLogIdx)
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (Err, any) {
	// what conditions should this node respect so that we can get further with handling the request
	// 1. the node has to be a leader in the current term
	//   - otherwise, the node wil return ErrWrongLeader
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	var isLeader, isKilled bool
	var nextOpLogIdx int

	isKilled = rsm.killed()
	_, isLeader = rsm.rf.GetState()

	dlog.Dlog(dlog.DClient, "S%d Submit received L(%v): %T(%v)", rsm.me, isLeader, req, req)

	// return if the node is aware of being a leader
	if isKilled || !isLeader {
		return ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	op := Op{Me: rsm.me, ID: UID(rand.New(rand.NewSource(time.Now().UnixNano())).Int()), Req: req, ResCode: RCPending}
	opReqCh := make(chan Op)
	rsm.opReqCh[op.ID] = opReqCh
	nextOpLogIdx, _, isLeader = rsm.rf.Start(op)
	rsm.expectedOpByLogIdx[nextOpLogIdx] = op
	rsm.mu.Unlock()

	// raft cluster leader changed meanwhile
	// returning ErrWrongLeader to signal the client
	// to look for the leader
	if !isLeader {
		go rsm.cleanup(op.ID, nextOpLogIdx)
		return ErrWrongLeader, nil
	}

	submitStart := time.Now()
	for {
		select {
		case finishedOp := <-opReqCh:

			isKilled = rsm.killed()
			_, isLeader = rsm.rf.GetState()

			if finishedOp.ResCode != RCApplied || !isLeader || isKilled {
				// opperation was not appended to the log of the leader
				// as explained above, that probably happened due to:
				// - leader change
				// - node killed
				go rsm.cleanup(op.ID, nextOpLogIdx)

				return ErrWrongLeader, nil
			}

			return OK, finishedOp.Result
		case <-time.After(100 * time.Millisecond):
			// no response in 100ms, check the state of the node
			// and the time passed since the request was made
			isKilled = rsm.killed()
			_, isLeader = rsm.rf.GetState()

			if isKilled || !isLeader || time.Since(submitStart) > 2*time.Second {
				go rsm.cleanup(op.ID, nextOpLogIdx)
				return ErrWrongLeader, nil
			}
		}
	}
}
