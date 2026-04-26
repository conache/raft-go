package grpc

import (
	"fmt"

	"github.com/conache/raft-go/transport"
)

// Cluster is one node's view of a Raft cluster
// A gRPC server plus the outbound peer handles
// Peers is in peer-id order
// The slot at this node's index is left nil since consensus never
// dispatches RPCs to itself
type Cluster struct {
	Peers  []transport.Peer
	server *server
}

// Build creates the gRPC peer handles for every other node and returns a
// Cluster with them populated
// The local server is created later by Start once the caller has
// constructed a Raft node bound to these Peers
func Build(me int, addrs []string) (*Cluster, error) {
	if me < 0 || me >= len(addrs) {
		return nil, fmt.Errorf("grpc.Build: me=%d out of range [0,%d)", me, len(addrs))
	}

	peers := make([]transport.Peer, len(addrs))

	for i, addr := range addrs {
		if i == me {
			continue
		}

		p, err := ConnectPeer(addr)
		if err != nil {
			closePeers(peers)
			return nil, fmt.Errorf("connect peer %d (%s): %w", i, addr, err)
		}

		peers[i] = p
	}

	return &Cluster{Peers: peers}, nil
}

// Start binds node to a gRPC server listening on listenAddr and serves
// incoming peer RPCs in a goroutine
// Returns once the listener is bound
func (c *Cluster) Start(node raftHandler, listenAddr string) error {
	srv, err := newServer(node, listenAddr)
	if err != nil {
		return err
	}

	c.server = srv
	go func() { _ = srv.Serve() }()

	return nil
}

// Stop tears down the server and closes every outbound peer connection
func (c *Cluster) Stop() {
	if c.server != nil {
		c.server.Stop()
	}

	closePeers(c.Peers)
}

// closePeers closes any *Peer handles in the slice and ignores nil/other entries
func closePeers(peers []transport.Peer) {
	for _, p := range peers {
		if rp, ok := p.(*Peer); ok {
			_ = rp.Close()
		}
	}
}
