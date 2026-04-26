package transport

// RPC method names used as the `method` argument to Peer.Call
// All transports (grpc, memory, ...) and consensus must reference these
// constants so the wire-level names live in exactly one place
const (
	MethodAppendEntries   = "AppendEntries"
	MethodRequestVote     = "RequestVote"
	MethodInstallSnapshot = "InstallSnapshot"
)
