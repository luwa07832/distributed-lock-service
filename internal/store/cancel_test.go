package store

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCancelWaitingRemovesOnlyThatWaiter(t *testing.T) {
	st := newTestStore(t)

	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)
	st.Acquire("res", "c", 30)
	before, _ := st.GetState("res")

	outcome, err := st.CancelWaiting("res", "b")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if outcome.Status != StatusCanceled {
		t.Fatalf("status = %q, want CANCELED", outcome.Status)
	}
	state := outcome.State
	if state.OwnerID != "a" || state.ReentryCount != 1 {
		t.Fatalf("holder changed: %+v", state)
	}
	if !state.LeaseExpiresAt.Equal(before.LeaseExpiresAt) {
		t.Fatalf("lease changed: %v vs %v", state.LeaseExpiresAt, before.LeaseExpiresAt)
	}
	if len(state.WaitingOwners) != 1 || state.WaitingOwners[0].OwnerID != "c" ||
		state.WaitingOwners[0].RequestedLeaseSecond != 30 {
		t.Fatalf("waiting = %+v", state.WaitingOwners)
	}
}

func TestCancelWaitingMissingResourceAndNotWaiting(t *testing.T) {
	st := newTestStore(t)

	if _, err := st.CancelWaiting("nope", "b"); !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("missing = %v, want ErrResourceNotFound", err)
	}

	st.Acquire("res", "a", 10)
	if _, err := st.CancelWaiting("res", "a"); !errors.Is(err, ErrNotWaiting) {
		t.Fatalf("holder cancel = %v, want ErrNotWaiting", err)
	}
	if _, err := st.CancelWaiting("res", "stranger"); !errors.Is(err, ErrNotWaiting) {
		t.Fatalf("stranger cancel = %v, want ErrNotWaiting", err)
	}

	st.Acquire("res", "b", 20)
	if _, err := st.CancelWaiting("res", "b"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := st.CancelWaiting("res", "b"); !errors.Is(err, ErrNotWaiting) {
		t.Fatalf("repeat cancel = %v, want ErrNotWaiting", err)
	}
	state, _ := st.GetState("res")
	if state.OwnerID != "a" || len(state.WaitingOwners) != 0 {
		t.Fatalf("repeat cancel changed state: %+v", state)
	}
}

func TestCancelThenReacquireFollowsQueueSemantics(t *testing.T) {
	st := newTestStore(t)

	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)
	st.Acquire("res", "c", 30)
	if _, err := st.CancelWaiting("res", "b"); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// Still held by someone else: b rejoins at the tail of the current queue.
	outcome, err := st.Acquire("res", "b", 40)
	if err != nil || outcome.Status != StatusWaiting {
		t.Fatalf("re-acquire = %+v, %v", outcome, err)
	}
	state, _ := st.GetState("res")
	if len(state.WaitingOwners) != 2 ||
		state.WaitingOwners[0].OwnerID != "c" || state.WaitingOwners[1].OwnerID != "b" {
		t.Fatalf("waiting = %+v", state.WaitingOwners)
	}
	if state.WaitingOwners[1].RequestedLeaseSecond != 40 {
		t.Fatalf("requested lease = %+v", state.WaitingOwners[1])
	}

	// Holderless after the release: b acquires directly.
	st.Acquire("free", "a", 10)
	st.Acquire("free", "b", 20)
	if _, err := st.CancelWaiting("free", "b"); err != nil {
		t.Fatalf("cancel free: %v", err)
	}
	if _, err := st.Release("free", "a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	outcome, err = st.Acquire("free", "b", 20)
	if err != nil || outcome.Status != StatusAcquired {
		t.Fatalf("acquire holderless = %+v, %v", outcome, err)
	}
	if outcome.State.OwnerID != "b" {
		t.Fatalf("owner = %q, want b", outcome.State.OwnerID)
	}
}

func TestCancelBeforeExpiryTransferIsNotPromoted(t *testing.T) {
	st, clock := newTestStoreClock(t)

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
		t.Fatalf("owner = %q, want c (b canceled before transfer)", state.OwnerID)
	}
	if len(state.WaitingOwners) != 0 {
		t.Fatalf("waiting = %+v", state.WaitingOwners)
	}
}

func TestCancelAfterTransferKeepsNewHolder(t *testing.T) {
	st, clock := newTestStoreClock(t)

	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)
	clock.add(11 * time.Second)
	st.reapExpired()

	if _, err := st.CancelWaiting("res", "b"); !errors.Is(err, ErrNotWaiting) {
		t.Fatalf("cancel promoted owner = %v, want ErrNotWaiting", err)
	}
	state, _ := st.GetState("res")
	if state.OwnerID != "b" || state.ReentryCount != 1 {
		t.Fatalf("cancel revoked the new holder: %+v", state)
	}
}

func TestCancelHeadWaiterThenReleasePromotesNext(t *testing.T) {
	st := newTestStore(t)

	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)
	st.Acquire("res", "c", 30)
	if _, err := st.CancelWaiting("res", "b"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	outcome, err := st.Release("res", "a")
	if err != nil || outcome.Status != StatusReleased {
		t.Fatalf("release = %+v, %v", outcome, err)
	}
	if outcome.State.OwnerID != "c" {
		t.Fatalf("owner = %q, want c", outcome.State.OwnerID)
	}
}

func TestCancelRacingReleaseKeepsConsistentState(t *testing.T) {
	st := newTestStore(t)

	st.Acquire("res", "a", 10)
	st.Acquire("res", "b", 20)
	st.Acquire("res", "c", 30)

	var wg sync.WaitGroup
	var cancelErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, cancelErr = st.CancelWaiting("res", "b")
	}()
	go func() {
		defer wg.Done()
		st.Release("res", "a")
	}()
	wg.Wait()

	if cancelErr != nil && !errors.Is(cancelErr, ErrNotWaiting) {
		t.Fatalf("cancel = %v", cancelErr)
	}
	state, _ := st.GetState("res")
	if state.OwnerID != "b" && state.OwnerID != "c" {
		t.Fatalf("owner = %q, want b or c", state.OwnerID)
	}
	for _, waiting := range state.WaitingOwners {
		if waiting.OwnerID == state.OwnerID {
			t.Fatalf("holder %q still queued", state.OwnerID)
		}
		if waiting.OwnerID == "b" && cancelErr == nil {
			t.Fatalf("b canceled but still queued")
		}
	}
}

func TestConcurrentCancelsNeverDisturbHolder(t *testing.T) {
	st := newTestStore(t)

	st.Acquire("hot", "holder", 10)
	const waiters = 20
	for i := 0; i < waiters; i++ {
		owner := "w" + string(rune('a'+i))
		st.Acquire("hot", owner, 10)
	}

	var wg sync.WaitGroup
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			owner := "w" + string(rune('a'+i))
			if _, err := st.CancelWaiting("hot", owner); err != nil {
				t.Errorf("cancel %s: %v", owner, err)
			}
		}(i)
	}
	wg.Wait()

	state, err := st.GetState("hot")
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if state.OwnerID != "holder" || state.ReentryCount != 1 {
		t.Fatalf("holder disturbed: %+v", state)
	}
	if len(state.WaitingOwners) != 0 {
		t.Fatalf("waiting = %+v, want empty", state.WaitingOwners)
	}
}
