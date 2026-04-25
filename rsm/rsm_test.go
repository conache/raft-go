package rsm

import (
	"sync"
	"testing"
	"time"
)

// Each peer should execute increments and update its counter
func TestBasic(t *testing.T) {
	const nIncs = 10

	ts := makeTest(t, nServers, -1)
	defer ts.cleanup()

	for i := range nIncs {
		r := ts.oneInc()
		if r == nil {
			t.Fatalf("oneInc %d returned nil", i)
		}
		if r.N != i+1 {
			t.Fatalf("expected counter %d, got %d", i+1, r.N)
		}
		ts.checkCounter(r.N, nServers)
	}
}

// Concurrent submits should all commit and produce a consistent counter
func TestConcurrent(t *testing.T) {
	const nIncs = 50

	ts := makeTest(t, nServers, -1)
	defer ts.cleanup()

	var wg sync.WaitGroup
	for range nIncs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts.oneInc()
		}()
	}
	wg.Wait()

	ts.checkCounter(nIncs, nServers)
}

// Increments must keep flowing after the leader is disconnected and
// reconnected
func TestLeaderFailure(t *testing.T) {
	ts := makeTest(t, nServers, -1)
	defer ts.cleanup()

	r := ts.oneInc()
	ts.checkCounter(r.N, nServers)

	l := ts.disconnectLeader()

	r = ts.oneInc()
	// The disconnected peer can't apply
	ts.checkCounter(r.N, nServers-1)

	ts.Connect(l)
	ts.checkCounter(r.N, nServers)
}

// A partitioned leader must not commit any operation submitted directly
// to it; a fresh leader on the majority side must keep committing
func TestLeaderPartition(t *testing.T) {
	const nSubmits = 100

	ts := makeTest(t, nServers, -1)
	defer ts.cleanup()

	// Submit one Inc so a leader is established and recorded
	r := ts.oneInc()
	ts.checkCounter(r.N, nServers)

	foundL, l := ts.findLeader()
	if !foundL {
		t.Fatalf("no leader after first inc")
	}

	majority, minority := ts.makePartition(l)
	ts.Partition(majority, minority)

	// Wait for the majority side to elect a new leader and commit one Inc
	// before starting the minority-side Decs, so the timing window below
	// isn't racing the election
	rep := ts.onePartition(majority, Inc{})
	if rep == nil {
		t.Fatalf("majority side never committed Inc")
	}

	// Submit many Decs concurrently to the (now-minority) old leader.
	// None should commit; each Submit should hold until rsm's own timeout
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := range nSubmits {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				err, repDec := ts.srvs[l].rsm.Submit(Dec{})
				if err == OK {
					t.Errorf("submit %d in minority committed unexpectedly: %v", i, repDec)
				}
			}(i)
		}
		wg.Wait()
		done <- struct{}{}
	}()

	// Decs must still be in flight one second after starting
	select {
	case <-done:
		t.Fatalf("Decs in minority completed; should still be blocked")
	case <-time.After(time.Second):
	}

	// Reconnect the isolated leader; the goroutine should drain shortly
	ts.Connect(l)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("Submits did not return after partition healed")
	}

	ts.checkCounter(rep.(*IncRep).N, nServers)
}

// Restart must replay the log and rebuild every FSM's counter
func TestRestartReplay(t *testing.T) {
	const nIncs = 100

	ts := makeTest(t, nServers, -1)
	defer ts.cleanup()

	for i := range nIncs {
		r := ts.oneInc()
		if r.N != i+1 {
			t.Fatalf("expected counter %d, got %d", i+1, r.N)
		}
		ts.checkCounter(r.N, nServers)
	}

	ts.KillAll()
	time.Sleep(time.Second)
	ts.RestartAll()

	r := ts.oneInc()
	if r == nil {
		t.Fatalf("oneInc after restart returned nil")
	}
	if r.N != nIncs+1 {
		t.Fatalf("expected counter %d after restart, got %d", nIncs+1, r.N)
	}

	time.Sleep(time.Second)
	ts.checkCounter(r.N, nServers)
}

// Submits in flight when the cluster is shut down must terminate
// (with ErrWrongLeader) instead of hanging forever
func TestShutdown(t *testing.T) {
	const nSubmits = 100

	ts := makeTest(t, nServers, -1)
	defer ts.cleanup()

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for range nSubmits {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ts.oneNull()
			}()
		}
		wg.Wait()
		done <- struct{}{}
	}()

	// Let some submits get into flight
	time.Sleep(20 * time.Millisecond)
	ts.KillAll()

	select {
	case <-done:
	case <-time.After((nSeconds + 1) * time.Second):
		t.Fatalf("submits did not terminate after shutdown")
	}
}

// Commands submitted after a restart must not be confused with the
// in-flight ones from before the previous shutdown
func TestRestartSubmit(t *testing.T) {
	const (
		nIncs    = 100
		nSubmits = 100
	)

	ts := makeTest(t, nServers, -1)
	defer ts.cleanup()

	for i := range nIncs {
		r := ts.oneInc()
		if r.N != i+1 {
			t.Fatalf("expected counter %d, got %d", i+1, r.N)
		}
		ts.checkCounter(r.N, nServers)
	}

	ts.KillAll()
	time.Sleep(time.Second)
	ts.RestartAll()

	r := ts.oneInc()
	if r == nil {
		t.Fatalf("oneInc after first restart returned nil")
	}
	if r.N != nIncs+1 {
		t.Fatalf("expected counter %d after restart, got %d", nIncs+1, r.N)
	}

	time.Sleep(time.Second)

	// Submit Nulls concurrently, then shut down again mid-flight
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for range nSubmits {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ts.oneNull()
			}()
		}
		wg.Wait()
		done <- struct{}{}
	}()
	time.Sleep(20 * time.Millisecond)
	ts.KillAll()

	select {
	case <-done:
	case <-time.After((nSeconds + 1) * time.Second):
		t.Fatalf("submits did not terminate after second shutdown")
	}

	ts.RestartAll()

	r = ts.oneInc()
	ts.checkCounter(r.N, nServers)
}

// Snapshots must trim the log; restoring from a snapshot after restart
// must rebuild every FSM
func TestSnapshot(t *testing.T) {
	const (
		n            = 100
		maxRaftState = 1000
	)

	ts := makeTest(t, nServers, maxRaftState)
	defer ts.cleanup()

	for range n {
		ts.oneInc()
	}
	ts.checkCounter(n, nServers)

	if sz := ts.MaxStateSize(); sz > 2*maxRaftState {
		t.Fatalf("log was not trimmed: stateSize=%d > 2*%d", sz, maxRaftState)
	}

	ts.KillAll()
	ts.RestartAll()

	ts.oneInc()
	ts.checkCounter(n+1, nServers)
}
