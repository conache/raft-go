package lincheck

import (
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// Anchors all timestamps to the monotonic clock
// Porcupine reasons about real-time overlap, so NTP/wall-clock drift
// could otherwise produce spurious violations
var t0 = time.Now()

// nowMonotonic returns nanoseconds since package load, on the monotonic clock
func nowMonotonic() int64 { return int64(time.Since(t0)) }

// OpLog is a goroutine-safe buffer of recorded porcupine.Operation events
type OpLog struct {
	// Guards ops
	mu sync.Mutex
	// Recorded operations in append order; snapshotted by Read for checking
	ops []porcupine.Operation
}

// NewOpLog returns an empty log
func NewOpLog() *OpLog { return &OpLog{} }

// Append records a raw porcupine operation
// Prefer Op, which also handles timing
func (l *OpLog) Append(op porcupine.Operation) {
	l.mu.Lock()
	l.ops = append(l.ops, op)
	l.mu.Unlock()
}

// Read returns a snapshot of all recorded operations
func (l *OpLog) Read() []porcupine.Operation {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]porcupine.Operation, len(l.ops))
	copy(out, l.ops)
	return out
}

// Len returns the number of recorded operations
func (l *OpLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.ops)
}

// Op times fn's invocation and appends the resulting operation to the log
func (l *OpLog) Op(clientID int, input any, fn func() any) {
	start := nowMonotonic()
	output := fn()
	end := nowMonotonic()
	l.Append(porcupine.Operation{
		ClientId: clientID,
		Input:    input,
		Output:   output,
		Call:     start,
		Return:   end,
	})
}
