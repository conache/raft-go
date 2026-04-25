// Package lincheck provides linearizability-testing helpers built on top
// of github.com/anishathalye/porcupine.
// It owns the reusable infrastructure for recording client operations
// against the raft cluster, feeding the resulting history to Porcupine,
// and optionally emitting an HTML visualization when a test fails.
//
// Typical usage:
//
//	log := lincheck.NewOpLog()
//	log.Op(clientID, input, func() any { return doWork() })
//	// ... many ops across goroutines ...
//	lincheck.Check(t, lincheck.RegisterModel, log, 5*time.Second)
//
// Set LINCHECK_VIS=path/to/file.html to dump a visualization on failure.
package lincheck
