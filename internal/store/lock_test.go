package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	st.SetClock(func() time.Time { return base })
	return st
}

func TestAcquireFirstApplicantBecomesHolder(t *testing.T) {
	st := newTestStore(t)

	outcome, err := st.Acquire("res", "a", 10)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if outcome.Status != StatusAcquired {
		t.Fatalf("status = %q, want ACQUIRED", outcome.Status)
	}
	state := outcome.State
	if state.OwnerID != "a" || state.ReentryCount != 1 {
		t.Fatalf("state = %+v", state)
	}
	want := time.Date(2026, 1, 1, 12, 0, 10, 0, time.UTC)
	if !state.LeaseExpiresAt.Equal(want) {
		t.Fatalf("leaseExpiresAt = %v, want %v", state.LeaseExpiresAt, want)
	}
	if len(state.WaitingOwners) != 0 {
		t.Fatalf("waiting = %+v", state.WaitingOwners)
	}
}

func TestSameOwnerReentryKeepsLeaseAndBumpsCount(t *testing.T) {
	st := newTestStore(t)

	first, _ := st.Acquire("res", "a", 10)
	outcome, err := st.Acquire("res", "a", 30)
	if err != nil {
		t.Fatalf("reentry acquire: %v", err)
	}
	if outcome.Status != StatusReentry {
		t.Fatalf("status = %q, want REENTRY", outcome.Status)
	}
	if outcome.State.ReentryCount != 2 {
		t.Fatalf("reentryCount = %d, want 2", outcome.State.ReentryCount)
	}
	if !outcome.State.LeaseExpiresAt.Equal(first.State.LeaseExpiresAt) {
		t.Fatalf("lease changed: %v vs %v", outcome.State.LeaseExpiresAt, first.State.LeaseExpiresAt)
	}

	reenter, err := st.Reenter("res", "a")
	if err != nil {
		t.Fatalf("reenter: %v", err)
	}
	if reenter.State.ReentryCount != 3 {
		t.Fatalf("reentryCount = %d, want 3", reenter.State.ReentryCount)
	}
	if !reenter.State.LeaseExpiresAt.Equal(first.State.LeaseExpiresAt) {
		t.Fatalf("reenter changed lease")
	}
}

func TestDifferentOwnersQueueInOrderAndRejectDuplicates(t *testing.T) {
	st := newTestStore(t)

	st.Acquire("res", "a", 10)
	waiting, err := st.Acquire("res", "b", 20)
	if err != nil || waiting.Status != StatusWaiting {
		t.Fatalf("b waiting = %+v, %v", waiting, err)
	}
	if _, err := st.Acquire("res", "c", 30); err != nil {
		t.Fatalf("c queue: %v", err)
	}

	if _, err := st.Acquire("res", "b", 20); !errors.Is(err, ErrDuplicateWaiting) {
		t.Fatalf("duplicate = %v, want ErrDuplicateWaiting", err)
	}

	state, err := st.GetState("res")
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if len(state.WaitingOwners) != 2 {
		t.Fatalf("waiting len = %d", len(state.WaitingOwners))
	}
	if state.WaitingOwners[0].OwnerID != "b" || state.WaitingOwners[0].RequestedLeaseSecond != 20 {
		t.Fatalf("first waiting = %+v", state.WaitingOwners[0])
	}
	if state.WaitingOwners[1].OwnerID != "c" || state.WaitingOwners[1].RequestedLeaseSecond != 30 {
		t.Fatalf("second waiting = %+v", state.WaitingOwners[1])
	}
}
