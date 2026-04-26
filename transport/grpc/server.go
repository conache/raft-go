package grpc

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/conache/raft-go/transport"
	raftpb "github.com/conache/raft-go/transport/grpc/pb/raft/v1"
)

// raftHandler is the wire-facing surface of a Raft node
// *consensus.Node satisfies it
type raftHandler interface {
	AppendEntries(args *transport.AppendEntriesArgs, reply *transport.AppendEntriesReply)
	RequestVote(args *transport.RequestVoteArgs, reply *transport.RequestVoteReply)
	InstallSnapshot(args *transport.InstallSnapshotArgs, reply *transport.InstallSnapshotReply)
}

// server is a gRPC RaftService that dispatches proto messages to a local raftHandler
type server struct {
	raftpb.UnimplementedRaftServiceServer
	node raftHandler
	gs   *grpc.Server
	lis  net.Listener
}

func newServer(node raftHandler, listenAddr string) (*server, error) {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", listenAddr, err)
	}

	gs := grpc.NewServer()
	srv := &server{node: node, gs: gs, lis: lis}
	raftpb.RegisterRaftServiceServer(gs, srv)

	return srv, nil
}

func (s *server) Serve() error { return s.gs.Serve(s.lis) }

func (s *server) Stop() { s.gs.GracefulStop() }

func (s *server) AppendEntries(
	_ context.Context,
	in *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResponse, error) {
	args, err := fromPbAppendEntries(in)
	if err != nil {
		return nil, err
	}

	var reply transport.AppendEntriesReply
	s.node.AppendEntries(&args, &reply)

	return toPbAppendEntriesReply(&reply), nil
}

func (s *server) RequestVote(
	_ context.Context,
	in *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
	args := fromPbRequestVote(in)

	var reply transport.RequestVoteReply
	s.node.RequestVote(&args, &reply)

	return toPbRequestVoteReply(&reply), nil
}

func (s *server) InstallSnapshot(
	_ context.Context,
	in *raftpb.InstallSnapshotRequest,
) (*raftpb.InstallSnapshotResponse, error) {
	args := fromPbInstallSnapshot(in)

	var reply transport.InstallSnapshotReply
	s.node.InstallSnapshot(&args, &reply)

	return toPbInstallSnapshotReply(&reply), nil
}
