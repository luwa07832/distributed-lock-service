package locksvc

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type testClock struct {
	base    time.Time
	advance atomic.Int64
}

func (c *testClock) now() time.Time      { return c.base.Add(time.Duration(c.advance.Load())) }
func (c *testClock) add(d time.Duration) { c.advance.Add(int64(d)) }

func newTestService(t *testing.T) (*Service, *testClock) {
	t.Helper()
	clock := &testClock{base: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	svc := New(WithClock(clock.now))
	return svc, clock
}

func TestAcquireFreeResourceStartsLeaseAtSuccessInstant(t *testing.T) {
	svc, clock := newTestService(t)

	outcome, err := svc.Acquire("res", "a", 30)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if outcome.Status != StatusAcquired {
		t.Fatalf("status = %q", outcome.Status)
	}
	if !outcome.State.Locked || outcome.State.OwnerID != "a" || outcome.State.ReentryCount != 1 {
		t.Fatalf("state = %+v", outcome.State)
	}
	want := clock.base.Add(30 * time.Second)
	if !outcome.State.LeaseExpiresAt.Equal(want) {
		t.Fatalf("expiry = %v, want %v", outcome.State.LeaseExpiresAt, want)
	}
}

func TestReentryKeepsOriginalExpiryAndBumpsCount(t *testing.T) {
	svc, clock := newTestService(t)

	first, err := svc.Acquire("res", "a", 30)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	clock.add(5 * time.Second)
	outcome, err := svc.Acquire("res", "a", 60)
	if err != nil {
		t.Fatalf("reentry: %v", err)
	}
	if outcome.Status != StatusReentry || outcome.State.ReentryCount != 2 {
		t.Fatalf("outcome = %+v", outcome)
	}
	if !outcome.State.LeaseExpiresAt.Equal(first.State.LeaseExpiresAt) {
		t.Fatalf("expiry moved on reentry")
	}

	again, err := svc.Reenter("res", "a")
	if err != nil || again.State.ReentryCount != 3 {
		t.Fatalf("explicit reentry = %+v, %v", again, err)
	}
	if !again.State.LeaseExpiresAt.Equal(first.State.LeaseExpiresAt) {
		t.Fatalf("explicit reentry moved expiry")
	}
}

func TestWaitersQueueInOrderAndDuplicateIsRejected(t *testing.T) {
	svc, _ := newTestService(t)

	svc.Acquire("res", "a", 10)
	if b, err := svc.Acquire("res", "b", 20); err != nil || b.Status != StatusWaiting {
		t.Fatalf("b = %+v, %v", b, err)
	}
	if c, err := svc.Acquire("res", "c", 30); err != nil || c.Status != StatusWaiting {
		t.Fatalf("c = %+v, %v", c, err)
	}
	if _, err := svc.Acquire("res", "b", 20); !errors.Is(err, ErrDuplicateWaiter) {
		t.Fatalf("dup waiter = %v", err)
	}

	state, _ := svc.GetResource("res")
	if len(state.Waiting) != 2 || state.Waiting[0].OwnerID != "b" || state.Waiting[1].OwnerID != "c" {
		t.Fatalf("waiting = %+v", state.Waiting)
	}
	if state.Waiting[0].RequestedLeaseSeconds != 20 || state.Waiting[1].RequestedLeaseSeconds != 30 {
		t.Fatalf("requested leases = %+v", state.Waiting)
	}
}

func TestSameHolderCannotEstablishSecondHolding(t *testing.T) {
	svc, _ := newTestService(t)

	if _, err := svc.Acquire("r1", "a", 10); err != nil {
		t.Fatalf("r1: %v", err)
	}
	if _, err := svc.Acquire("r2", "a", 10); !errors.Is(err, ErrDuplicateHold) {
		t.Fatalf("second hold = %v, want ErrDuplicateHold", err)
	}
	// Queued attempt on another resource is also rejected.
	svc.Acquire("r3", "b", 10)
	if _, err := svc.Acquire("r3", "a", 10); !errors.Is(err, ErrDuplicateHold) {
		t.Fatalf("queued second hold = %v", err)
	}
	// The same resource still re-enters instead.
	outcome, err := svc.Acquire("r1", "a", 10)
	if err != nil || outcome.Status != StatusReentry {
		t.Fatalf("same resource = %+v, %v", outcome, err)
	}
}

func TestExpiryPromotesHeadWaiterWithFreshLease(t *testing.T) {
	svc, clock := newTestService(t)

	svc.Acquire("res", "a", 10)
	svc.Acquire("res", "b", 20)
	svc.Acquire("res", "c", 30)

	clock.add(10 * time.Second)
	state, err := svc.GetResource("res")
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if state.OwnerID != "b" || state.ReentryCount != 1 {
		t.Fatalf("owner = %q count = %d", state.OwnerID, state.ReentryCount)
	}
	want := clock.now().Add(20 * time.Second)
	if !state.LeaseExpiresAt.Equal(want) {
		t.Fatalf("expiry = %v, want %v", state.LeaseExpiresAt, want)
	}
	if len(state.Waiting) != 1 || state.Waiting[0].OwnerID != "c" {
		t.Fatalf("waiting = %+v", state.Waiting)
	}
}

func TestExpiryWithoutWaitersFreesResource(t *testing.T) {
	svc, clock := newTestService(t)

	svc.Acquire("res", "a", 10)
	clock.add(10 * time.Second)
	state, _ := svc.GetResource("res")
	if state.Locked || state.OwnerID != "" || state.ReentryCount != 0 || len(state.Waiting) != 0 {
		t.Fatalf("state = %+v", state)
	}
	if !state.Exists {
		t.Fatalf("former-owner record should still exist")
	}

	// The old owner can take the free resource again with a fresh lease.
	outcome, err := svc.Acquire("res", "a", 5)
	if err != nil || outcome.Status != StatusAcquired {
		t.Fatalf("reacquire = %+v, %v", outcome, err)
	}
}

func TestLateReleaseAndReentryReturnLeaseExpired(t *testing.T) {
	svc, clock := newTestService(t)

	svc.Acquire("res", "a", 10)
	clock.add(10 * time.Second)

	if _, err := svc.Release("res", "a"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late release = %v", err)
	}
	if _, err := svc.Reenter("res", "a"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late reenter = %v", err)
	}
	if _, err := svc.Renew("res", "a", 10); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late renew = %v", err)
	}
}

func TestReleaseCountsDownThenTransfers(t *testing.T) {
	svc, clock := newTestService(t)

	svc.Acquire("res", "a", 10)
	svc.Acquire("res", "a", 10) // reentry -> 2
	svc.Acquire("res", "b", 20)

	first, err := svc.Release("res", "a")
	if err != nil || first.Status != StatusReentryDecrement {
		t.Fatalf("first release = %+v, %v", first, err)
	}
	if first.State.ReentryCount != 1 || first.State.OwnerID != "a" {
		t.Fatalf("state = %+v", first.State)
	}

	second, err := svc.Release("res", "a")
	if err != nil || second.Status != StatusReleased {
		t.Fatalf("second release = %+v, %v", second, err)
	}
	if second.State.OwnerID != "b" || second.State.ReentryCount != 1 {
		t.Fatalf("state = %+v", second.State)
	}
	want := clock.now().Add(20 * time.Second)
	if !second.State.LeaseExpiresAt.Equal(want) {
		t.Fatalf("new expiry = %v, want %v", second.State.LeaseExpiresAt, want)
	}
}

func TestReleaseWithEmptyQueueRemovesRecord(t *testing.T) {
	svc, _ := newTestService(t)

	svc.Acquire("res", "a", 10)
	outcome, err := svc.Release("res", "a")
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if outcome.State.Exists {
		t.Fatalf("record should be removed, got %+v", outcome.State)
	}
	state, _ := svc.GetResource("res")
	if state.Exists {
		t.Fatalf("query should report empty result, got %+v", state)
	}
}

func TestNonOwnerReleaseAndRenewRejected(t *testing.T) {
	svc, _ := newTestService(t)

	svc.Acquire("res", "a", 10)
	if _, err := svc.Release("res", "b"); !errors.Is(err, ErrNotLockOwner) {
		t.Fatalf("release = %v", err)
	}
	if _, err := svc.Renew("res", "b", 10); !errors.Is(err, ErrNotLockOwner) {
		t.Fatalf("renew = %v", err)
	}
	if _, err := svc.Reenter("res", "b"); !errors.Is(err, ErrNotLockOwner) {
		t.Fatalf("reenter = %v", err)
	}
}

func TestRenewMeasuresFromRenewalInstant(t *testing.T) {
	svc, clock := newTestService(t)

	svc.Acquire("res", "a", 10)
	clock.add(7 * time.Second)
	outcome, err := svc.Renew("res", "a", 15)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	want := clock.now().Add(15 * time.Second)
	if !outcome.State.LeaseExpiresAt.Equal(want) {
		t.Fatalf("expiry = %v, want %v", outcome.State.LeaseExpiresAt, want)
	}
	if outcome.State.ReentryCount != 1 {
		t.Fatalf("renew changed reentry count = %d", outcome.State.ReentryCount)
	}
}

func TestGetHolderReturnsHolds(t *testing.T) {
	svc, clock := newTestService(t)

	svc.Acquire("res", "a", 10)
	holder, err := svc.GetHolder("a")
	if err != nil {
		t.Fatalf("holder: %v", err)
	}
	if len(holder.Holds) != 1 || holder.Holds[0].ResourceID != "res" {
		t.Fatalf("holds = %+v", holder.Holds)
	}
	if holder.Holds[0].LeaseSeconds != 10 || holder.Holds[0].ReentryCount != 1 {
		t.Fatalf("hold = %+v", holder.Holds[0])
	}

	// Unknown holder is an empty result, not an error.
	empty, err := svc.GetHolder("nobody")
	if err != nil || len(empty.Holds) != 0 {
		t.Fatalf("empty = %+v, %v", empty, err)
	}

	// After expiry the old holder has no holds.
	clock.add(11 * time.Second)
	expired, err := svc.GetHolder("a")
	if err != nil || len(expired.Holds) != 0 {
		t.Fatalf("expired holds = %+v, %v", expired, err)
	}
}

func TestUnknownResourceQueryIsEmptyNotError(t *testing.T) {
	svc, _ := newTestService(t)

	state, err := svc.GetResource("missing")
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if state.Exists || state.Locked || state.OwnerID != "" || len(state.Waiting) != 0 {
		t.Fatalf("state = %+v", state)
	}
}

func TestValidationErrors(t *testing.T) {
	svc, _ := newTestService(t)

	if _, err := svc.Acquire("r", "a", 0); !errors.Is(err, ErrInvalidLeaseDuration) {
		t.Fatalf("zero lease = %v", err)
	}
	if _, err := svc.AcquireWithReentry("r", "a", 10, -1); !errors.Is(err, ErrInvalidReentryCount) {
		t.Fatalf("negative reentry = %v", err)
	}
	if _, err := svc.Acquire("", "a", 10); !errors.Is(err, ErrInvalidLockIdentity) {
		t.Fatalf("empty resource = %v", err)
	}
	if _, err := svc.Acquire("r", "", 10); !errors.Is(err, ErrInvalidHolderIdentity) {
		t.Fatalf("empty owner = %v", err)
	}
	if _, err := svc.Renew("r", "a", -1); !errors.Is(err, ErrInvalidLeaseDuration) {
		t.Fatalf("negative renew lease = %v", err)
	}
	if _, err := svc.GetResource(""); !errors.Is(err, ErrInvalidLockIdentity) {
		t.Fatalf("query empty resource = %v", err)
	}
	if _, err := svc.GetHolder(""); !errors.Is(err, ErrInvalidHolderIdentity) {
		t.Fatalf("query empty owner = %v", err)
	}
}

func TestErrorSentinelAliases(t *testing.T) {
	if !errors.Is(InvalidLeaseDurationError, ErrInvalidLeaseDuration) {
		t.Fatal("lease duration alias mismatch")
	}
	if !errors.Is(DuplicateHoldError, ErrDuplicateHold) {
		t.Fatal("duplicate hold alias mismatch")
	}
	if !errors.Is(NotLockOwnerError, ErrNotLockOwner) {
		t.Fatal("not owner alias mismatch")
	}
}

func TestIdentifiersUsedVerbatim(t *testing.T) {
	svc, _ := newTestService(t)

	resource := "  Doc-MIXED_CASE "
	owner := "  Owner-MIXED_CASE "
	outcome, err := svc.Acquire(resource, owner, 5)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if outcome.State.ResourceID != resource || outcome.State.OwnerID != owner {
		t.Fatalf("identifiers altered: %+v", outcome.State)
	}
	state, _ := svc.GetResource(resource)
	if state.ResourceID != resource || state.OwnerID != owner {
		t.Fatalf("query identifiers altered: %+v", state)
	}
	holder, _ := svc.GetHolder(owner)
	if len(holder.Holds) != 1 || holder.Holds[0].ResourceID != resource {
		t.Fatalf("holder lookup altered: %+v", holder)
	}
}
