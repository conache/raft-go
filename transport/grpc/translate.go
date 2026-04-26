package grpc

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/conache/raft-go/transport"
	raftpb "github.com/conache/raft-go/transport/grpc/pb/raft/v1"
)

// Converters between the hand-written transport.* types and the proto wire types
// LogEntry.Command is gob-encoded into bytes at the boundary so consensus stays proto-agnostic
func toPbLogEntry(e transport.LogEntry) (*raftpb.LogEntry, error) {
	cmd, err := encodeCommand(e.Command)
	if err != nil {
		return nil, fmt.Errorf("encode command: %w", err)
	}

	return &raftpb.LogEntry{Term: int64(e.Term), Command: cmd}, nil
}

func fromPbLogEntry(e *raftpb.LogEntry) (transport.LogEntry, error) {
	cmd, err := decodeCommand(e.GetCommand())
	if err != nil {
		return transport.LogEntry{}, fmt.Errorf("decode command: %w", err)
	}

	return transport.LogEntry{Term: int(e.GetTerm()), Command: cmd}, nil
}

func toPbRequestVote(args *transport.RequestVoteArgs) *raftpb.RequestVoteRequest {
	return &raftpb.RequestVoteRequest{
		Term:         int64(args.Term),
		CandidateId:  int32(args.CandidateID),
		LastLogIndex: int64(args.LastLogIndex),
		LastLogTerm:  int64(args.LastLogTerm),
	}
}

func fromPbRequestVote(r *raftpb.RequestVoteRequest) transport.RequestVoteArgs {
	return transport.RequestVoteArgs{
		Term:         int(r.GetTerm()),
		CandidateID:  int(r.GetCandidateId()),
		LastLogIndex: int(r.GetLastLogIndex()),
		LastLogTerm:  int(r.GetLastLogTerm()),
	}
}

func toPbRequestVoteReply(r *transport.RequestVoteReply) *raftpb.RequestVoteResponse {
	return &raftpb.RequestVoteResponse{Term: int64(r.Term), Success: r.Success}
}

func fromPbRequestVoteReply(r *raftpb.RequestVoteResponse) transport.RequestVoteReply {
	return transport.RequestVoteReply{Term: int(r.GetTerm()), Success: r.GetSuccess()}
}

func toPbAppendEntries(args *transport.AppendEntriesArgs) (*raftpb.AppendEntriesRequest, error) {
	entries := make([]*raftpb.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		pb, err := toPbLogEntry(e)
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", i, err)
		}
		entries[i] = pb
	}

	return &raftpb.AppendEntriesRequest{
		Term:         int64(args.Term),
		LeaderId:     int32(args.LeaderID),
		PrevLogIndex: int64(args.PrevLogIndex),
		PrevLogTerm:  int64(args.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int64(args.LeaderCommit),
	}, nil
}

func fromPbAppendEntries(r *raftpb.AppendEntriesRequest) (transport.AppendEntriesArgs, error) {
	entries := make([]transport.LogEntry, len(r.GetEntries()))
	for i, pb := range r.GetEntries() {
		e, err := fromPbLogEntry(pb)
		if err != nil {
			return transport.AppendEntriesArgs{}, fmt.Errorf("entry %d: %w", i, err)
		}
		entries[i] = e
	}

	return transport.AppendEntriesArgs{
		Term:         int(r.GetTerm()),
		LeaderID:     int(r.GetLeaderId()),
		PrevLogIndex: int(r.GetPrevLogIndex()),
		PrevLogTerm:  int(r.GetPrevLogTerm()),
		Entries:      entries,
		LeaderCommit: int(r.GetLeaderCommit()),
	}, nil
}

func toPbAppendEntriesReply(r *transport.AppendEntriesReply) *raftpb.AppendEntriesResponse {
	return &raftpb.AppendEntriesResponse{
		Term:                int64(r.Term),
		Success:             r.Success,
		EntryTerm:           int64(r.EntryTerm),
		EntryTermStartIndex: int64(r.EntryTermStartIndex),
	}
}

func fromPbAppendEntriesReply(r *raftpb.AppendEntriesResponse) transport.AppendEntriesReply {
	return transport.AppendEntriesReply{
		Term:                int(r.GetTerm()),
		Success:             r.GetSuccess(),
		EntryTerm:           int(r.GetEntryTerm()),
		EntryTermStartIndex: int(r.GetEntryTermStartIndex()),
	}
}

func toPbInstallSnapshot(args *transport.InstallSnapshotArgs) *raftpb.InstallSnapshotRequest {
	return &raftpb.InstallSnapshotRequest{
		Term:              int64(args.Term),
		LeaderId:          int32(args.LeaderID),
		LastIncludedIndex: int64(args.LastIncludedIndex),
		LastIncludedTerm:  int64(args.LastIncludedTerm),
		Data:              args.Data,
	}
}

func fromPbInstallSnapshot(r *raftpb.InstallSnapshotRequest) transport.InstallSnapshotArgs {
	return transport.InstallSnapshotArgs{
		Term:              int(r.GetTerm()),
		LeaderID:          int(r.GetLeaderId()),
		LastIncludedIndex: int(r.GetLastIncludedIndex()),
		LastIncludedTerm:  int(r.GetLastIncludedTerm()),
		Data:              r.GetData(),
	}
}

func toPbInstallSnapshotReply(r *transport.InstallSnapshotReply) *raftpb.InstallSnapshotResponse {
	return &raftpb.InstallSnapshotResponse{Term: int64(r.Term)}
}

func fromPbInstallSnapshotReply(r *raftpb.InstallSnapshotResponse) transport.InstallSnapshotReply {
	return transport.InstallSnapshotReply{Term: int(r.GetTerm())}
}

// encodeCommand gob-encodes a Command interface value to bytes
// A nil input returns a nil byte slice
func encodeCommand(cmd any) ([]byte, error) {
	if cmd == nil {
		return nil, nil
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&cmd); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// decodeCommand gob-decodes bytes back into a Command
// An empty input returns nil
func decodeCommand(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var cmd any
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd); err != nil {
		return nil, err
	}

	return cmd, nil
}
