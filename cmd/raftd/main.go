// raftd runs a single Raft node
// gRPC for inter-peer RPCs and HTTP for clients
// Returns 503 when not leader; the CLI client is the one that retries
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/conache/raft-go/rsm"
	memstorage "github.com/conache/raft-go/storage/memory"
	tgrpc "github.com/conache/raft-go/transport/grpc"
)

func main() {
	var (
		id           = flag.Int("id", 0, "this peer's index in --raft")
		raftAddrs    = flag.String("raft", "", "comma-separated gRPC addresses of all peers")
		httpAddr     = flag.String("http", "", "HTTP address this node serves clients on")
		maxRaftState = flag.Int("maxraftstate", -1, "snapshot threshold in bytes; -1 disables")
	)
	flag.Parse()

	if *raftAddrs == "" || *httpAddr == "" {
		log.Fatal("--raft and --http are required")
	}

	raftPeers := strings.Split(*raftAddrs, ",")

	if *id < 0 || *id >= len(raftPeers) {
		log.Fatalf("--id=%d out of range [0,%d)", *id, len(raftPeers))
	}

	cluster, err := tgrpc.Build(*id, raftPeers)
	if err != nil {
		log.Fatalf("build cluster: %v", err)
	}

	rsmNode := rsm.MakeRSM(cluster.Peers, *id, memstorage.New(), *maxRaftState, newKV())

	if err := cluster.Start(rsmNode.Raft(), raftPeers[*id]); err != nil {
		log.Fatalf("start grpc: %v", err)
	}

	log.Printf("peer %d: gRPC listening on %s", *id, raftPeers[*id])

	httpSrv := &http.Server{
		Addr:    *httpAddr,
		Handler: newHandler(*id, rsmNode),
	}

	go func() {
		log.Printf("peer %d: HTTP listening on %s", *id, *httpAddr)

		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Printf("peer %d: shutting down", *id)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = httpSrv.Shutdown(shutdownCtx)
	rsmNode.Kill()
	cluster.Stop()
}
