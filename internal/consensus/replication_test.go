package consensus_test

import (
	"testing"

	"github.com/conache/raft-go/internal/testcluster"
)

// TestReplicationBasicAgreement proposes three commands in sequence on a
// healthy 3-node cluster and confirms each one commits at the expected
// index on every peer.
func TestReplicationBasicAgreement(t *testing.T) {
	c := testcluster.New(t, 3)
	defer c.Shutdown()

	// Make sure a leader exists before proposing anything — otherwise One()
	// would spin on "no leader" until the first election finishes anyway.
	c.CheckOneLeader()

	const commands = 3
	for i := range commands {
		cmd := 100 * (i + 1)
		expectedIdx := i + 1
		committedIdx := c.One(cmd, 3, false)
		if committedIdx != expectedIdx {
			t.Fatalf("command %d committed at index %d, want %d", cmd, committedIdx, expectedIdx)
		}
	}
}
