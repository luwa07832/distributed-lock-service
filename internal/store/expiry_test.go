package store

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	base    time.Time
	advance atomic.Int64
}

func newTestStoreClock(t *testing.T) (*Store, *fakeClock) {
	t.Helper()
	st := newTestStore(t)
	clock := &fakeClock{base: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	st.SetClock(func() time.Time {
		return clock.base.Add(time.Duration(clock.advance.Load()))
	})
	return st, clock
}

func (c *fakeClock) now() time.Time {
	return c.base.Add(time.Duration(c.advance.Load()))
}

func (c *fakeClock) add(d time.Duration) { c.advance.Add(int64(d)) }

func TestExpiryPromotesNextWaiterWithRequestedLease(t *testing.T) {
	st, clock := newTestStoreClock(t)

	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)
	st.Acquire("res", "c", 30)

	clock.add(10*time.Second + time.Millisecond)
	st.reapExpired()

	state, err := st.GetState("res")
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if state.OwnerID != "b" || state.ReentryCount != 1 {
		t.Fatalf("owner = %q count = %d, want b/1", state.OwnerID, state.ReentryCount)
	}
	want := clock.now().Add(20 * time.Second)
	if !state.LeaseExpiresAt.Equal(want) {
		t.Fatalf("lease = %v, want %v", state.LeaseExpiresAt, want)
	}
	if len(state.WaitingOwners) != 1 || state.WaitingOwners[0].OwnerID != "c" {
		t.Fatalf("waiting = %+v", state.WaitingOwners)
	}

	// The old holder cannot cover the new holder.
	if _, err := st.Release("res", "a"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late release = %v, want ErrLeaseExpired", err)
	}
	if _, err := st.Reenter("res", "a"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late reenter = %v, want ErrLeaseExpired", err)
	}
	state, _ = st.GetState("res")
	if state.OwnerID != "b" {
		t.Fatalf("late action changed owner to %q", state.OwnerID)
	}
}

func TestExpiryWithoutWaitersLeavesResourceHolderless(t *testing.T) {
	st, clock := newTestStoreClock(t)

	st.Acquire("res", "a", 10)
	clock.add(11 * time.Second)
	st.reapExpired()

	state, err := st.GetState("res")
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if state.OwnerID != "" || state.ReentryCount != 0 || len(state.WaitingOwners) != 0 {
		t.Fatalf("state = %+v, want holderless", state)
	}
	if !state.LeaseExpiresAt.IsZero() {
		t.Fatalf("leaseExpiresAt = %v, want zero", state.LeaseExpiresAt)
	}
}

func TestExpiredOldOwnerAcquireIsRejectedAndPromotes(t *testing.T) {
	st, clock := newTestStoreClock(t)

	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)
	clock.add(11 * time.Second)

	if _, err := st.Acquire("res", "a", 5); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late acquire = %v, want ErrLeaseExpired", err)
	}
	state, _ := st.GetState("res")
	if state.OwnerID != "b" {
		t.Fatalf("owner = %q, want b", state.OwnerID)
	}
}

func TestReleaseTransfersOnlyOnLastReentry(t *testing.T) {
	st := newTestStore(t)

	st.Acquire("res", "a", 10)
	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)

	outcome, err := st.Release("res", "a")
	if err != nil || outcome.Status != StatusReentryDecrement {
		t.Fatalf("first release = %+v, %v", outcome, err)
	}
	state := outcome.State
	if state.OwnerID != "a" || state.ReentryCount != 1 {
		t.Fatalf("state = %+v", state)
	}
	if len(state.WaitingOwners) != 1 {
		t.Fatalf("waiting leaked transfer: %+v", state.WaitingOwners)
	}

	outcome, err = st.Release("res", "a")
	if err != nil || outcome.Status != StatusReleased {
		t.Fatalf("second release = %+v, %v", outcome, err)
	}
	if outcome.State.OwnerID != "b" || outcome.State.ReentryCount != 1 {
		t.Fatalf("transferred state = %+v", outcome.State)
	}
}

func TestNonOwnerReleaseAndReenterRejected(t *testing.T) {
	st := newTestStore(t)

	st.Acquire("res", "a", 10)
	if _, err := st.Release("res", "b"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("release = %v, want ErrNotOwner", err)
	}
	if _, err := st.Reenter("res", "b"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("reenter = %v, want ErrNotOwner", err)
	}
}

func TestGetStateMissingResource(t *testing.T) {
	st := newTestStore(t)
	if _, err := st.GetState("nope"); !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("get = %v, want ErrResourceNotFound", err)
	}
}

func TestInvalidLeaseSecondsRejected(t *testing.T) {
	st := newTestStore(t)
	if _, err := st.Acquire("res", "a", 0); !errors.Is(err, ErrInvalidLeaseSecond) {
		t.Fatalf("zero lease = %v", err)
	}
	if _, err := st.Acquire("res", "a", -1); !errors.Is(err, ErrInvalidLeaseSecond) {
		t.Fatalf("negative lease = %v", err)
	}
}
