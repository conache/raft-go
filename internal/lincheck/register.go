package lincheck

import (
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/anishathalye/porcupine"
)

// RegOpKind tags a single-register operation
// Named string so logs and Porcupine diagnostics stay readable
type RegOpKind string

const (
	RegOpGet RegOpKind = "get"
	RegOpPut RegOpKind = "put"
)

// RegOp is a single-register command
// Registered with gob so it round-trips through the in-memory transport
type RegOp struct {
	// RegOpGet or RegOpPut
	Kind RegOpKind
	// Only meaningful for RegOpPut; ignored for RegOpGet
	Value int
}

func init() { gob.Register(RegOp{}) }

// RegisterFSM is a single-int register state machine
// Each cluster node holds its own; log-ordered applies keep copies in sync
type RegisterFSM struct {
	// Guards val
	mu sync.Mutex
	// Current register value, mutated by RegOpPut and read by RegOpGet
	val int
}

// NewRegisterFSM returns an FSM initialized to zero
func NewRegisterFSM() *RegisterFSM { return &RegisterFSM{} }

// Apply executes op and returns the value the caller should observe
func (s *RegisterFSM) Apply(op RegOp) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch op.Kind {
	case RegOpPut:
		s.val = op.Value
		return op.Value
	case RegOpGet:
		return s.val
	default:
		panic(fmt.Sprintf("RegisterFSM: unknown op kind %q", op.Kind))
	}
}

// RegisterModel is the Porcupine model matching RegisterFSM's semantics
var RegisterModel = porcupine.Model{
	Init: func() any { return 0 },
	Step: func(state, input, output any) (bool, any) {
		s := state.(int)
		op := input.(RegOp)
		got := output.(int)
		switch op.Kind {
		case RegOpPut:
			return got == op.Value, op.Value
		case RegOpGet:
			return got == s, s
		default:
			return false, s
		}
	},
	Equal: func(a, b any) bool { return a.(int) == b.(int) },
	DescribeOperation: func(input, output any) string {
		op := input.(RegOp)
		ret := output.(int)
		if op.Kind == RegOpPut {
			return fmt.Sprintf("put(%d) -> %d", op.Value, ret)
		}
		return fmt.Sprintf("get() -> %d", ret)
	},
}
