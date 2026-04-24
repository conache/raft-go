package lincheck

import (
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/anishathalye/porcupine"
)

// RegOpKind distinguishes the operations the single-register FSM supports.
// Defined as a named string so it's human-readable in logs and Porcupine
// diagnostics without any extra Stringer boilerplate.
type RegOpKind string

const (
	RegOpGet RegOpKind = "get"
	RegOpPut RegOpKind = "put"
)

// RegOp is a single-register command. Value is only meaningful for
// RegOpPut. Registered with gob so it round-trips through the in-memory
// transport's serialization.
type RegOp struct {
	Kind  RegOpKind
	Value int
}

func init() { gob.Register(RegOp{}) }

// RegisterFSM is a single-int register state machine. Each cluster node
// holds its own; because Raft delivers applies in log order, all copies
// converge to the same sequence of states.
type RegisterFSM struct {
	mu  sync.Mutex
	val int
}

// NewRegisterFSM returns an FSM initialized to zero.
func NewRegisterFSM() *RegisterFSM { return &RegisterFSM{} }

// Apply executes op against the register and returns the value that
// RegOpPut or RegOpGet would conventionally yield.
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

// RegisterModel is the Porcupine model matching RegisterFSM's semantics.
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
