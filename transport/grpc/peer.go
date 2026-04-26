package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/conache/raft-go/transport"
	raftpb "github.com/conache/raft-go/transport/grpc/pb/raft/v1"
)

// Peer implements transport.Peer over a gRPC client connection
type Peer struct {
	conn   *grpc.ClientConn
	client raftpb.RaftServiceClient
}

// ConnectPeer opens a plaintext gRPC client connection to target ("host:port")
// and returns a Peer that satisfies transport.Peer
func ConnectPeer(target string) (*Peer, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect peer %s: %w", target, err)
	}

	return &Peer{
		conn:   conn,
		client: raftpb.NewRaftServiceClient(conn),
	}, nil
}

func (p *Peer) Close() error { return p.conn.Close() }

// Call implements transport.Peer.Call by dispatching to the matching gRPC stub
func (p *Peer) Call(ctx context.Context, method string, args, reply any) error {
	switch method {
	case transport.MethodAppendEntries:
		in, err := toPbAppendEntries(args.(*transport.AppendEntriesArgs))
		if err != nil {
			return err
		}

		out, err := p.client.AppendEntries(ctx, in)
		if err != nil {
			return err
		}

		*reply.(*transport.AppendEntriesReply) = fromPbAppendEntriesReply(out)

		return nil

	case transport.MethodRequestVote:
		in := toPbRequestVote(args.(*transport.RequestVoteArgs))

		out, err := p.client.RequestVote(ctx, in)
		if err != nil {
			return err
		}

		*reply.(*transport.RequestVoteReply) = fromPbRequestVoteReply(out)

		return nil

	case transport.MethodInstallSnapshot:
		in := toPbInstallSnapshot(args.(*transport.InstallSnapshotArgs))

		out, err := p.client.InstallSnapshot(ctx, in)
		if err != nil {
			return err
		}

		*reply.(*transport.InstallSnapshotReply) = fromPbInstallSnapshotReply(out)

		return nil
	}

	return fmt.Errorf("grpc.Peer: unknown method %q", method)
}
