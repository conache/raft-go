package memory

import (
	"bytes"
	"sync"
	"testing"

	"github.com/conache/raft-go/storage"
)

// Compile-time check: *Store satisfies storage.Store.
var _ storage.Store = (*Store)(nil)

func TestEmptyStoreReturnsNil(t *testing.T) {
	s := New()
	if state, _ := s.ReadState(); state != nil {
		t.Fatalf("ReadState on empty store = %v, want nil", state)
	}
	if snap, _ := s.ReadSnapshot(); snap != nil {
		t.Fatalf("ReadSnapshot on empty store = %v, want nil", snap)
	}
	if sz := s.StateSize(); sz != 0 {
		t.Fatalf("StateSize on empty store = %d, want 0", sz)
	}
}

func TestSaveReadRoundTrip(t *testing.T) {
	s := New()
	wantState := []byte("term=3 vote=1")
	wantSnap := []byte("snapshot-v1")

	if err := s.Save(wantState, wantSnap); err != nil {
		t.Fatal(err)
	}

	gotState, _ := s.ReadState()
	if !bytes.Equal(gotState, wantState) {
		t.Errorf("ReadState = %q, want %q", gotState, wantState)
	}
	gotSnap, _ := s.ReadSnapshot()
	if !bytes.Equal(gotSnap, wantSnap) {
		t.Errorf("ReadSnapshot = %q, want %q", gotSnap, wantSnap)
	}
	if sz := s.StateSize(); sz != len(wantState) {
		t.Errorf("StateSize = %d, want %d", sz, len(wantState))
	}
}

func TestNilSnapshotPreservesExisting(t *testing.T) {
	s := New()
	s.Save([]byte("state-1"), []byte("snap-1"))
	s.Save([]byte("state-2"), nil)

	state, _ := s.ReadState()
	if !bytes.Equal(state, []byte("state-2")) {
		t.Errorf("state = %q, want state-2", state)
	}
	snap, _ := s.ReadSnapshot()
	if !bytes.Equal(snap, []byte("snap-1")) {
		t.Errorf("snapshot = %q, want snap-1 (should have been preserved)", snap)
	}
}

func TestReadReturnsCopy(t *testing.T) {
	s := New()
	s.Save([]byte("original"), nil)

	got, _ := s.ReadState()
	got[0] = 'X' // mutate the returned slice

	again, _ := s.ReadState()
	if !bytes.Equal(again, []byte("original")) {
		t.Errorf("internal state was mutated through returned slice: %q", again)
	}
}

func TestSaveTakesCopy(t *testing.T) {
	s := New()
	src := []byte("hello")
	s.Save(src, nil)
	src[0] = 'X' // mutate the slice we passed in

	got, _ := s.ReadState()
	if !bytes.Equal(got, []byte("hello")) {
		t.Errorf("internal state was aliased to caller's slice: %q", got)
	}
}

func TestCloneIsIndependent(t *testing.T) {
	s := New()
	s.Save([]byte("a"), []byte("sA"))

	c := s.Clone()
	s.Save([]byte("b"), []byte("sB"))

	cState, _ := c.ReadState()
	if !bytes.Equal(cState, []byte("a")) {
		t.Errorf("clone state changed after original was modified: %q", cState)
	}
	cSnap, _ := c.ReadSnapshot()
	if !bytes.Equal(cSnap, []byte("sA")) {
		t.Errorf("clone snapshot changed after original was modified: %q", cSnap)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	s.Save([]byte("seed"), nil)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(3)
		go func() { defer wg.Done(); s.Save([]byte("state"), []byte("snap")) }()
		go func() { defer wg.Done(); _, _ = s.ReadState() }()
		go func() { defer wg.Done(); _ = s.StateSize() }()
	}
	wg.Wait()
}
