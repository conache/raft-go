// Package raftpb defines the Raft RPC message types and the LogEntry type.
// It is the shared wire vocabulary between the consensus implementation and
// any transport that carries Raft traffic.
//
// The "pb" suffix follows etcd/raft's naming convention; in v0.1 the types
// are hand-written Go structs, but may be replaced by protoc-generated types
// in a later release without changing the import path.
package raftpb
