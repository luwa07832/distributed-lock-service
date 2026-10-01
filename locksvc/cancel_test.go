package locksvc

import (
	"errors"
	"testing"
	"time"
)

func TestCancelWaitingRemovesOnlyTheCaller(t *testing.T) {
	svc, clock := newTestService(t)

	acquired, err := svc.Acquire("res", "a", 30)
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	if _, err := svc.Acquire("res", "b", 45); err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	if _, err := svc.Acquire("res", "c", 60); err != nil {
		t.Fatalf("acquire c: %v", err)
	}

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
	if !outcome.State.LeaseExpiresAt.Equal(acquired.State.LeaseExpiresAt) {
		t.Fatalf("leaseExpiresAt changed: %v, want %v",
			outcome.State.LeaseExpiresAt, acquired.State.LeaseExpiresAt)
	}
	if outcome.State.ReentryCount != 1 {
		t.Fatalf("reentryCount = %d", outcome.State.ReentryCount)
	}
	if len(outcome.State.Waiting) != 1 || outcome.State.Waiting[0].OwnerID != "c" {
		t.Fatalf("waiting = %+v", outcome.State.Waiting)
	}
	if outcome.State.Waiting[0].RequestedLeaseSeconds != 60 {
		t.Fatalf("requestedLeaseSeconds = %d", outcome.State.Waiting[0].RequestedLeaseSeconds)
	}

	view, err := svc.GetResource("res")
	if err != nil {
		t.Fatalf("get resource: %v", err)
	}
	if len(view.Waiting) != 1 || view.Waiting[0].OwnerID != "c" {
		t.Fatalf("query view waiting = %+v", view.Waiting)
	}
	if !view.LeaseExpiresAt.Equal(clock.base.Add(30 * time.Second)) {
		t.Fatalf("lease seconds changed: %v", view.LeaseExpiresAt)
	}

	// A canceled waiter can enqueue again at the tail under the existing semantics.
	again, err := svc.Acquire("res", "b", 15)
	if err != nil {
		t.Fatalf("re-acquire b: %v", err)
	}
	if again.Status != StatusWaiting {
		t.Fatalf("status = %q", again.Status)
	}
	owners := []string{again.State.Waiting[0].OwnerID, again.State.Waiting[1].OwnerID}
	if owners[0] != "c" || owners[1] != "b" || again.State.Waiting[1].RequestedLeaseSeconds != 15 {
		t.Fatalf("queue after re-enqueue = %+v", again.State.Waiting)
	}
}

func TestCancelWaitingHeadDoesNotGetPromotedOnLaterExpiry(t *testing.T) {
	svc, clock := newTestService(t)

	if _, err := svc.Acquire("res", "a", 30); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	if _, err := svc.Acquire("res", "b", 45); err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	if _, err := svc.Acquire("res", "c", 60); err != nil {
		t.Fatalf("acquire c: %v", err)
	}
	if _, err := svc.CancelWaiting("res", "b"); err != nil {
		t.Fatalf("cancel b: %v", err)
	}

	clock.add(31 * time.Second)
	view, err := svc.GetResource("res")
	if err != nil {
		t.Fatalf("get resource: %v", err)
	}
	if view.OwnerID != "c" {
		t.Fatalf("promoted holder = %q, want c", view.OwnerID)
	}
	if !view.LeaseExpiresAt.Equal(clock.now().Add(60 * time.Second)) {
		t.Fatalf("lease based on wrong requested seconds: %v", view.LeaseExpiresAt)
	}
	if len(view.Waiting) != 0 {
		t.Fatalf("waiting = %+v", view.Waiting)
	}
}

func TestCancelWaitingAfterExpiryTransferReturnsNotWaiting(t *testing.T) {
	svc, clock := newTestService(t)

	if _, err := svc.Acquire("res", "a", 30); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	if _, err := svc.Acquire("res", "b", 45); err != nil {
		t.Fatalf("acquire b: %v", err)
	}

	clock.add(31 * time.Second)
	outcome, err := svc.CancelWaiting("res", "b")
	if !errors.Is(err, ErrNotWaiting) {
		t.Fatalf("err = %v, want ErrNotWaiting", err)
	}
	if outcome.Status != "" || outcome.State.ResourceID != "" {
		t.Fatalf("outcome = %+v, want zero", outcome)
	}

	view, err := svc.GetResource("res")
	if err != nil {
		t.Fatalf("get resource: %v", err)
	}
	if view.OwnerID != "b" || !view.Locked {
		t.Fatalf("b must still hold the promoted lock: %+v", view)
	}
	if len(view.Waiting) != 0 {
		t.Fatalf("waiting = %+v", view.Waiting)
	}
}

func TestCancelWaitingAfterReleaseTransferReturnsNotWaiting(t *testing.T) {
	svc, _ := newTestService(t)

	if _, err := svc.Acquire("res", "a", 30); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	if _, err := svc.Acquire("res", "b", 45); err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	if _, err := svc.Release("res", "a"); err != nil {
		t.Fatalf("release a: %v", err)
	}

	if _, err := svc.CancelWaiting("res", "b"); !errors.Is(err, ErrNotWaiting) {
		t.Fatalf("err = %v, want ErrNotWaiting", err)
	}
	view, _ := svc.GetResource("res")
	if view.OwnerID != "b" {
		t.Fatalf("holder = %q, want b", view.OwnerID)
	}
}

func TestCancelWaitingUnknownResource(t *testing.T) {
	svc, _ := newTestService(t)

	_, err := svc.CancelWaiting("never", "a")
	if !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("err = %v, want ErrResourceNotFound", err)
	}
	if !errors.Is(err, ResourceNotFoundError) {
		t.Fatalf("ResourceNotFoundError not recognizable")
	}

	if _, err := svc.Acquire("res", "a", 10); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := svc.Release("res", "a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	// The record is removed after a release with an empty queue.
	if _, err := svc.CancelWaiting("res", "a"); !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("err = %v, want ErrResourceNotFound", err)
	}
}

func TestCancelWaitingNotAPendingWaiter(t *testing.T) {
	svc, _ := newTestService(t)

	if _, err := svc.Acquire("res", "a", 30); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	if _, err := svc.Acquire("res", "b", 45); err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	if _, err := svc.CancelWaiting("res", "b"); err != nil {
		t.Fatalf("cancel b: %v", err)
	}

	// A stranger, the current holder, and a repeated cancel all return NOT_WAITING.
	for _, owner := range []string{"a", "b", "stranger"} {
		if _, err := svc.CancelWaiting("res", owner); !errors.Is(err, ErrNotWaiting) {
			t.Fatalf("owner %q err = %v, want ErrNotWaiting", owner, err)
		}
	}
	if !errors.Is(ErrNotWaiting, NotWaitingError) {
		t.Fatalf("NotWaitingError not recognizable")
	}
	view, _ := svc.GetResource("res")
	if view.OwnerID != "a" || len(view.Waiting) != 0 {
		t.Fatalf("state changed by failed cancels: %+v", view)
	}
}

func TestCancelWaitingValidation(t *testing.T) {
	svc, _ := newTestService(t)

	if _, err := svc.CancelWaiting("", "a"); !errors.Is(err, ErrInvalidLockIdentity) {
		t.Fatalf("empty resource err = %v", err)
	}
	if _, err := svc.CancelWaiting("res", ""); !errors.Is(err, ErrInvalidHolderIdentity) {
		t.Fatalf("empty owner err = %v", err)
	}
}
