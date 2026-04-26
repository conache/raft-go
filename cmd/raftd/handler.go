package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/conache/raft-go/rsm"
)

// handler dispatches HTTP requests to the local Raft node
// Returns 503 when this node isn't the leader
// The client is responsible for retrying against another peer
type handler struct {
	id  int
	rsm *rsm.RSM
}

func newHandler(id int, r *rsm.RSM) *handler {
	return &handler{id: id, rsm: r}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/put":
		h.serveOp(w, req, "PUT")
	case "/get":
		h.serveOp(w, req, "GET")
	case "/keys":
		h.serveKeys(w, req)
	case "/status":
		h.serveStatus(w)
	default:
		http.NotFound(w, req)
	}
}

type opResponse struct {
	Value    string `json:"value"`
	LeaderID int    `json:"leader_id"`
	Term     int    `json:"term"`
}

type keysResponse struct {
	Keys     []string `json:"keys"`
	LeaderID int      `json:"leader_id"`
	Term     int      `json:"term"`
}

func (h *handler) serveOp(w http.ResponseWriter, req *http.Request, kind string) {
	key := req.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	value := req.URL.Query().Get("value")
	if kind == "PUT" && value == "" {
		http.Error(w, "missing value", http.StatusBadRequest)
		return
	}

	op := Op{Kind: kind, Key: key, Value: value}
	status, resp := h.rsm.Submit(op)

	if status != rsm.OK {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}

	term, _ := h.rsm.Raft().GetState()
	writeJSON(w, opResponse{
		Value:    fmt.Sprintf("%v", resp),
		LeaderID: h.id,
		Term:     term,
	})
}

func (h *handler) serveKeys(w http.ResponseWriter, req *http.Request) {
	pattern := req.URL.Query().Get("pattern")
	if pattern == "" {
		pattern = "*"
	}

	op := Op{Kind: "LIST", Key: pattern}
	status, resp := h.rsm.Submit(op)

	if status != rsm.OK {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}

	keys, _ := resp.([]string)
	term, _ := h.rsm.Raft().GetState()
	writeJSON(w, keysResponse{
		Keys:     keys,
		LeaderID: h.id,
		Term:     term,
	})
}

func (h *handler) serveStatus(w http.ResponseWriter) {
	term, isLeader := h.rsm.Raft().GetState()

	writeJSON(w, struct {
		ID       int  `json:"id"`
		Term     int  `json:"term"`
		IsLeader bool `json:"is_leader"`
	}{ID: h.id, Term: term, IsLeader: isLeader})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
