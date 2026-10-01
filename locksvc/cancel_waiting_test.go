package locksvc

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCancelWaitingRemovesOnlySelectedWaiterAndKeepsHolding(t *testing.T) {
	svc, _ := newTestService(t)

	if _, err := svc.Acquire("res", "a", 10); err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	if _, err := svc.Acquire("res", "b", 20); err != nil {
		t.Fatalf("queue b: %v", err)
	}
	if _, err := svc.Acquire("res", "c", 30); err != nil {
		t.Fatalf("queue c: %v", err)
	}

	before := svc.locks["res"]
	outcome, err := svc.CancelWaiting("res", "b")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if outcome.Status != StatusCanceled {
		t.Fatalf("status = %q", outcome.Status)
	}
	if !outcome.State.Locked || outcome.State.OwnerID != "a" {
		t.Fatalf("holder changed: %+v", outcome.State)
	}
	if outcome.State.ReentryCount != before.reentryCount ||
		!outcome.State.LeaseExpiresAt.Equal(before.leaseExpiresAt) {
		t.Fatalf("lease metadata changed: before=%+v after=%+v", before, outcome.State)
	}
	if len(outcome.State.Waiting) != 1 || outcome.State.Waiting[0].OwnerID != "c" ||
		outcome.State.Waiting[0].RequestedLeaseSeconds != 30 {
		t.Fatalf("waiting = %+v", outcome.State.Waiting)
	}

	query, err := svc.GetResource("res")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if query.ResourceID != outcome.State.ResourceID ||
		query.Locked != outcome.State.Locked || query.OwnerID != outcome.State.OwnerID ||
		!query.LeaseExpiresAt.Equal(outcome.State.LeaseExpiresAt) ||
		query.ReentryCount != outcome.State.ReentryCount ||
		len(query.Waiting) != len(outcome.State.Waiting) {
		t.Fatalf("outcome state %+v does not match query %+v", outcome.State, query)
	}
	if after := svc.locks["res"]; after.leaseSeconds != before.leaseSeconds {
		t.Fatalf("leaseSeconds changed: before=%d after=%d", before.leaseSeconds, after.leaseSeconds)
	}
}

func TestCancelWaitingValidationAndStateErrors(t *testing.T) {
	svc, _ := newTestService(t)

	if _, err := svc.CancelWaiting("", "a"); !errors.Is(err, ErrInvalidLockIdentity) {
		t.Fatalf("empty resource = %v", err)
	}
	if _, err := svc.CancelWaiting("res", ""); !errors.Is(err, ErrInvalidHolderIdentity) {
		t.Fatalf("empty owner = %v", err)
	}
	if _, err := svc.CancelWaiting("missing", "a"); !errors.Is(err, ResourceNotFoundError) {
		t.Fatalf("missing resource = %v", err)
	}

	if _, err := svc.Acquire("res", "a", 10); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := svc.CancelWaiting("res", "a"); !errors.Is(err, NotWaitingError) {
		t.Fatalf("holder cancel = %v", err)
	}
	if _, err := svc.CancelWaiting("res", "z"); !errors.Is(err, ErrNotWaiting) {
		t.Fatalf("unknown waiter cancel = %v", err)
	}
}

func TestCancelWaitingAndExpiryTransferAreOrderDeterministic(t *testing.T) {
	t.Run("cancel first", func(t *testing.T) {
		svc, clock := newTestService(t)
		svc.Acquire("res", "a", 10)
		svc.Acquire("res", "b", 20)
		svc.Acquire("res", "c", 30)

		if _, err := svc.CancelWaiting("res", "b"); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		clock.add(10 * time.Second)
		state, err := svc.GetResource("res")
		if err != nil {
			t.Fatalf("get resource: %v", err)
		}
		if state.OwnerID != "c" || state.ReentryCount != 1 || len(state.Waiting) != 0 {
			t.Fatalf("state after canceled waiter expiry = %+v", state)
		}
		if !state.LeaseExpiresAt.Equal(clock.now().Add(30 * time.Second)) {
			t.Fatalf("expiry = %v", state.LeaseExpiresAt)
		}
	})

	t.Run("transfer first", func(t *testing.T) {
		svc, clock := newTestService(t)
		svc.Acquire("res", "a", 10)
		svc.Acquire("res", "b", 20)
		svc.Acquire("res", "c", 30)

		clock.add(10 * time.Second)
		outcome, err := svc.CancelWaiting("res", "b")
		if !errors.Is(err, ErrNotWaiting) {
			t.Fatalf("cancel promoted owner = %+v, %v", outcome, err)
		}
		state, _ := svc.GetResource("res")
		if state.OwnerID != "b" || state.ReentryCount != 1 || len(state.Waiting) != 1 {
			t.Fatalf("promoted owner lost lock: %+v", state)
		}
		lease := state.LeaseExpiresAt
		if _, err := svc.CancelWaiting("res", "b"); !errors.Is(err, ErrNotWaiting) {
			t.Fatalf("second cancel = %v", err)
		}
		state, _ = svc.GetResource("res")
		if state.OwnerID != "b" || !state.LeaseExpiresAt.Equal(lease) {
			t.Fatalf("cancel released new lock: %+v", state)
		}
	})
}

func TestCancelWaitingAndVoluntaryTransferAreOrderDeterministic(t *testing.T) {
	t.Run("cancel first", func(t *testing.T) {
		svc, _ := newTestService(t)
		svc.Acquire("res", "a", 10)
		svc.Acquire("res", "b", 20)
		svc.Acquire("res", "c", 30)

		if _, err := svc.CancelWaiting("res", "b"); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		release, err := svc.Release("res", "a")
		if err != nil || release.State.OwnerID != "c" {
			t.Fatalf("release = %+v, %v", release, err)
		}
	})

	t.Run("transfer first", func(t *testing.T) {
		svc, _ := newTestService(t)
		svc.Acquire("res", "a", 10)
		svc.Acquire("res", "b", 20)

		release, err := svc.Release("res", "a")
		if err != nil || release.State.OwnerID != "b" {
			t.Fatalf("release = %+v, %v", release, err)
		}
		if _, err := svc.CancelWaiting("res", "b"); !errors.Is(err, ErrNotWaiting) {
			t.Fatalf("cancel after promotion = %v", err)
		}
		state, _ := svc.GetResource("res")
		if state.OwnerID != "b" {
			t.Fatalf("new holder was released: %+v", state)
		}
	})
}

func TestConcurrentCancelAndVoluntaryReleaseHasOneDeterministicResult(t *testing.T) {
	const iterations = 100
	for i := 0; i < iterations; i++ {
		svc, _ := newTestService(t)
		svc.Acquire("res", "a", 10)
		svc.Acquire("res", "b", 20)
		svc.Acquire("res", "c", 30)

		var wg sync.WaitGroup
		var cancelErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, cancelErr = svc.CancelWaiting("res", "b")
		}()
		go func() {
			defer wg.Done()
			if _, err := svc.Release("res", "a"); err != nil {
				t.Errorf("release: %v", err)
			}
		}()
		wg.Wait()

		state, _ := svc.GetResource("res")
		switch state.OwnerID {
		case "b":
			if !errors.Is(cancelErr, ErrNotWaiting) || len(state.Waiting) != 1 || state.Waiting[0].OwnerID != "c" {
				t.Fatalf("transfer-first result: err=%v state=%+v", cancelErr, state)
			}
		case "c":
			if cancelErr != nil || len(state.Waiting) != 0 {
				t.Fatalf("cancel-first result: err=%v state=%+v", cancelErr, state)
			}
		default:
			t.Fatalf("unexpected owner %q: %+v", state.OwnerID, state)
		}
	}
}
