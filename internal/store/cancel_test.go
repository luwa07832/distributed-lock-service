package store

import (
	"errors"
	"testing"
	"time"
)

func TestCancelWaitingRemovesOnlyCaller(t *testing.T) {
	st, clock := newTestStoreClock(t)

	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)
	st.Acquire("res", "c", 30)
	st.Acquire("res", "a", 10) // holder reentry, count becomes 2

	outcome, err := st.CancelWaiting("res", "b")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if outcome.Status != StatusCanceled {
		t.Fatalf("status = %q, want %q", outcome.Status, StatusCanceled)
	}

	state := outcome.State
	if state.ResourceID != "res" || state.OwnerID != "a" {
		t.Fatalf("holder changed: %+v", state)
	}
	if state.ReentryCount != 2 {
		t.Fatalf("reentry count = %d, want 2", state.ReentryCount)
	}
	wantExpires := clock.now().Add(10 * time.Second)
	if !state.LeaseExpiresAt.Equal(wantExpires) {
		t.Fatalf("leaseExpiresAt = %v, want %v", state.LeaseExpiresAt, wantExpires)
	}
	if len(state.WaitingOwners) != 1 || state.WaitingOwners[0].OwnerID != "c" ||
		state.WaitingOwners[0].RequestedLeaseSecond != 30 {
		t.Fatalf("waiting queue = %+v, want only c(30)", state.WaitingOwners)
	}
}

func TestCancelWaitingIsIdempotentNotWaiting(t *testing.T) {
	st, _ := newTestStoreClock(t)
	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)

	if _, err := st.CancelWaiting("res", "b"); err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	before, _ := st.GetState("res")
	_, err := st.CancelWaiting("res", "b")
	if !errors.Is(err, ErrNotWaiting) {
		t.Fatalf("second cancel err = %v, want ErrNotWaiting", err)
	}
	after, _ := st.GetState("res")
	if after.OwnerID != before.OwnerID || !after.LeaseExpiresAt.Equal(before.LeaseExpiresAt) ||
		after.ReentryCount != before.ReentryCount ||
		len(after.WaitingOwners) != len(before.WaitingOwners) {
		t.Fatalf("duplicate cancel changed state: before=%+v after=%+v", before, after)
	}

	if _, err := st.CancelWaiting("res", "a"); !errors.Is(err, ErrNotWaiting) {
		t.Fatalf("holder cancel err = %v, want ErrNotWaiting", err)
	}
	if _, err := st.CancelWaiting("missing", "a"); !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("unknown resource err = %v, want ErrResourceNotFound", err)
	}
}

func TestCancelWaitingThenReacquireFollowsCurrentState(t *testing.T) {
	st, clock := newTestStoreClock(t)

	// Still held: canceled owner re-enters at the tail of the queue.
	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)
	st.Acquire("res", "c", 30)
	st.CancelWaiting("res", "b")

	outcome, err := st.Acquire("res", "b", 40)
	if err != nil || outcome.Status != StatusWaiting {
		t.Fatalf("reacquire while held: outcome=%+v err=%v", outcome, err)
	}
	queue := outcome.State.WaitingOwners
	if len(queue) != 2 || queue[0].OwnerID != "c" || queue[1].OwnerID != "b" ||
		queue[1].RequestedLeaseSecond != 40 {
		t.Fatalf("queue after reacquire = %+v", queue)
	}

	// Expire without waiters after canceling everyone: holderless acquire wins.
	st.CancelWaiting("res", "c")
	st.CancelWaiting("res", "b")
	clock.add(11 * time.Second)
	st.reapExpired()

	outcome, err = st.Acquire("res", "b", 15)
	if err != nil || outcome.Status != StatusAcquired || outcome.State.OwnerID != "b" {
		t.Fatalf("acquire when holderless: outcome=%+v err=%v", outcome, err)
	}
	if len(outcome.State.WaitingOwners) != 0 {
		t.Fatalf("waiting = %+v, want empty", outcome.State.WaitingOwners)
	}
}

func TestCancelRacingExpiryNeverPromotesCanceled(t *testing.T) {
	st, clock := newTestStoreClock(t)

	// Cancel completes first: the canceled request is never promoted later.
	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)
	st.Acquire("res", "c", 30)
	if _, err := st.CancelWaiting("res", "b"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	clock.add(11 * time.Second)
	st.reapExpired()

	state, _ := st.GetState("res")
	if state.OwnerID != "c" {
		t.Fatalf("holder after expiry = %q, want c", state.OwnerID)
	}
	if state.LeaseExpiresAt.Equal(clock.now()) {
		t.Fatalf("lease should use c's requested seconds: %v", state.LeaseExpiresAt)
	}
	if len(state.WaitingOwners) != 0 {
		t.Fatalf("waiting = %+v, want empty", state.WaitingOwners)
	}
}

func TestExpiryFirstThenCancelReturnsNotWaiting(t *testing.T) {
	st, clock := newTestStoreClock(t)

	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)
	st.Acquire("res", "c", 30)
	clock.add(11 * time.Second)
	st.reapExpired() // b is promoted and leaves the queue

	state, _ := st.GetState("res")
	if state.OwnerID != "b" {
		t.Fatalf("holder = %q, want b", state.OwnerID)
	}
	if _, err := st.CancelWaiting("res", "b"); !errors.Is(err, ErrNotWaiting) {
		t.Fatalf("cancel after promotion err = %v, want ErrNotWaiting", err)
	}
	state, _ = st.GetState("res")
	if state.OwnerID != "b" {
		t.Fatalf("cancel reclaimed holding: holder = %q, want b", state.OwnerID)
	}
	if len(state.WaitingOwners) != 1 || state.WaitingOwners[0].OwnerID != "c" {
		t.Fatalf("waiting = %+v, want only c", state.WaitingOwners)
	}
}

func TestCancelConcurrentlyWithExpiry(t *testing.T) {
	st, clock := newTestStoreClock(t)

	st.Acquire("res", "a", 10)
	const waiters = 30
	for i := 0; i < waiters; i++ {
		st.Acquire("res", waiterName(i), int64(10+i))
	}

	done := make(chan struct{})
	go func() {
		clock.add(11 * time.Second)
		st.reapExpired()
		close(done)
	}()
	for i := 0; i < waiters; i++ {
		st.CancelWaiting("res", waiterName(i))
	}
	<-done

	state, err := st.GetState("res")
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if state.OwnerID == "" {
		t.Fatalf("resource lost its holder")
	}
	seen := map[string]bool{state.OwnerID: true}
	for _, waiting := range state.WaitingOwners {
		if seen[waiting.OwnerID] {
			t.Fatalf("owner %q appears twice", waiting.OwnerID)
		}
		seen[waiting.OwnerID] = true
	}
	if state.ReentryCount != 1 {
		t.Fatalf("reentry count = %d, want 1", state.ReentryCount)
	}
}

func waiterName(i int) string {
	return "w" + string(rune('a'+i%26)) + string(rune('a'+i/26))
}
