package store

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) (*Store, *time.Time) {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	st.SetClockForTest(func() time.Time { return now })
	return st, &now
}

func bg() context.Context { return context.Background() }

func asServiceError(t *testing.T, err error) *ServiceError {
	t.Helper()
	var serviceErr *ServiceError
	if !errors.As(err, &serviceErr) {
		t.Fatalf("error %v is not a ServiceError", err)
	}
	return serviceErr
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", code)
	}
	if got := asServiceError(t, err).Code; got != code {
		t.Fatalf("error code = %s, want %s", got, code)
	}
}

func TestAcquireGrantsFirstAndReturnsSnapshot(t *testing.T) {
	st, _ := newTestStore(t)

	state, status, err := st.Acquire(bg(), "doc", "alice", 5)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if status != StatusAcquired || state.OwnerID != "alice" || state.ReentryCount != 1 {
		t.Fatalf("unexpected result: status=%s state=%+v", status, state)
	}
	wantExpiry := time.Date(2026, 10, 1, 12, 0, 5, 0, time.UTC).Format(time.RFC3339Nano)
	if state.LeaseExpiresAt != wantExpiry {
		t.Fatalf("leaseExpiresAt = %s, want %s", state.LeaseExpiresAt, wantExpiry)
	}
	if len(state.WaitingOwners) != 0 {
		t.Fatalf("waitingOwners = %v, want empty", state.WaitingOwners)
	}
}

func TestValidationErrors(t *testing.T) {
	st, _ := newTestStore(t)

	_, _, err := st.Acquire(bg(), "  ", "alice", 5)
	assertCode(t, err, CodeMissingResourceID)
	_, _, err = st.Acquire(bg(), "doc", " ", 5)
	assertCode(t, err, CodeMissingOwnerID)
	_, _, err = st.Acquire(bg(), "doc", "alice", 0)
	assertCode(t, err, CodeInvalidLease)
	_, _, err = st.Acquire(bg(), "doc", "alice", -3)
	assertCode(t, err, CodeInvalidLease)

	_, err = st.Reenter(bg(), " ", "alice")
	assertCode(t, err, CodeMissingResourceID)
	_, err = st.Reenter(bg(), "doc", "")
	assertCode(t, err, CodeMissingOwnerID)

	_, _, err = st.Release(bg(), " ", "alice")
	assertCode(t, err, CodeMissingResourceID)
	_, _, err = st.Release(bg(), "doc", "")
	assertCode(t, err, CodeMissingOwnerID)

	_, err = st.Query(bg(), " ")
	assertCode(t, err, CodeMissingResourceID)
}

func TestReentryKeepsExpiryAndSingleRecord(t *testing.T) {
	st, _ := newTestStore(t)

	first, _, err := st.Acquire(bg(), "doc", "alice", 5)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	again, status, err := st.Acquire(bg(), "doc", "alice", 9)
	if err != nil {
		t.Fatalf("reentry acquire: %v", err)
	}
	if status != StatusAcquired || again.ReentryCount != 2 {
		t.Fatalf("reentry result = %s %+v", status, again)
	}
	if again.LeaseExpiresAt != first.LeaseExpiresAt || again.OwnerID != "alice" {
		t.Fatalf("reentry changed holder or expiry: %+v vs %+v", again, first)
	}

	viaReenter, err := st.Reenter(bg(), "doc", "alice")
	if err != nil {
		t.Fatalf("reenter: %v", err)
	}
	if viaReenter.ReentryCount != 3 || viaReenter.LeaseExpiresAt != first.LeaseExpiresAt {
		t.Fatalf("reenter endpoint changed state: %+v", viaReenter)
	}
}

func TestQueueOrderAndDuplicateWaiting(t *testing.T) {
	st, _ := newTestStore(t)

	if _, _, err := st.Acquire(bg(), "doc", "alice", 5); err != nil {
		t.Fatalf("acquire alice: %v", err)
	}
	bob, status, err := st.Acquire(bg(), "doc", "bob", 3)
	if err != nil || status != StatusWaiting {
		t.Fatalf("bob acquire = %s, %v", status, err)
	}
	if bob.OwnerID != "alice" || len(bob.WaitingOwners) != 1 {
		t.Fatalf("bob snapshot = %+v", bob)
	}
	carol, status, err := st.Acquire(bg(), "doc", "carol", 4)
	if err != nil || status != StatusWaiting {
		t.Fatalf("carol acquire = %s, %v", status, err)
	}
	if len(carol.WaitingOwners) != 2 ||
		carol.WaitingOwners[0].OwnerID != "bob" || carol.WaitingOwners[0].RequestedLeaseSeconds != 3 ||
		carol.WaitingOwners[1].OwnerID != "carol" || carol.WaitingOwners[1].RequestedLeaseSeconds != 4 {
		t.Fatalf("queue snapshot = %+v", carol.WaitingOwners)
	}

	_, _, err = st.Acquire(bg(), "doc", "bob", 3)
	assertCode(t, err, CodeDuplicateWaiting)

	state, err := st.Query(bg(), "doc")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(state.WaitingOwners) != 2 {
		t.Fatalf("duplicate request altered queue: %+v", state.WaitingOwners)
	}
}

func TestReleaseReentryLevelsThenTransfersFifo(t *testing.T) {
	st, _ := newTestStore(t)

	if _, _, err := st.Acquire(bg(), "doc", "alice", 5); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := st.Reenter(bg(), "doc", "alice"); err != nil {
		t.Fatalf("reenter: %v", err)
	}
	if _, _, err := st.Acquire(bg(), "doc", "bob", 3); err != nil {
		t.Fatalf("queue bob: %v", err)
	}
	if _, _, err := st.Acquire(bg(), "doc", "carol", 4); err != nil {
		t.Fatalf("queue carol: %v", err)
	}

	afterFirst, status, err := st.Release(bg(), "doc", "alice")
	if err != nil || status != StatusReleased {
		t.Fatalf("first release = %s, %v", status, err)
	}
	if afterFirst.OwnerID != "alice" || afterFirst.ReentryCount != 1 || len(afterFirst.WaitingOwners) != 2 {
		t.Fatalf("release handed over too early: %+v", afterFirst)
	}
	wantExpiry := time.Date(2026, 10, 1, 12, 0, 5, 0, time.UTC).Format(time.RFC3339Nano)
	if afterFirst.LeaseExpiresAt != wantExpiry {
		t.Fatalf("lease changed on partial release: %s", afterFirst.LeaseExpiresAt)
	}

	afterSecond, _, err := st.Release(bg(), "doc", "alice")
	if err != nil {
		t.Fatalf("second release: %v", err)
	}
	if afterSecond.OwnerID != "bob" || afterSecond.ReentryCount != 1 {
		t.Fatalf("new holder = %+v", afterSecond)
	}
	bobExpiry := time.Date(2026, 10, 1, 12, 0, 3, 0, time.UTC).Format(time.RFC3339Nano)
	if afterSecond.LeaseExpiresAt != bobExpiry {
		t.Fatalf("bob expiry = %s, want %s", afterSecond.LeaseExpiresAt, bobExpiry)
	}
	if len(afterSecond.WaitingOwners) != 1 || afterSecond.WaitingOwners[0].OwnerID != "carol" {
		t.Fatalf("remaining queue = %+v", afterSecond.WaitingOwners)
	}
}

func TestFinalReleaseWithEmptyQueueKeepsQueryableResource(t *testing.T) {
	st, _ := newTestStore(t)

	if _, _, err := st.Acquire(bg(), "doc", "alice", 5); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	state, _, err := st.Release(bg(), "doc", "alice")
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if state.OwnerID != "" || state.LeaseExpiresAt != "" || state.ReentryCount != 0 {
		t.Fatalf("resource not cleared: %+v", state)
	}
	if len(state.WaitingOwners) != 0 {
		t.Fatalf("waitingOwners = %v", state.WaitingOwners)
	}

	again, err := st.Query(bg(), "doc")
	if err != nil {
		t.Fatalf("query released resource: %v", err)
	}
	if again.OwnerID != "" || again.LeaseExpiresAt != "" || again.ReentryCount != 0 || len(again.WaitingOwners) != 0 {
		t.Fatalf("released snapshot = %+v", again)
	}
}

func TestNotOwnerRejections(t *testing.T) {
	st, _ := newTestStore(t)

	_, err := st.Reenter(bg(), "doc", "alice")
	assertCode(t, err, CodeNotOwner)
	_, _, err = st.Release(bg(), "doc", "alice")
	assertCode(t, err, CodeNotOwner)

	if _, _, err := st.Acquire(bg(), "doc", "alice", 5); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_, err = st.Reenter(bg(), "doc", "bob")
	assertCode(t, err, CodeNotOwner)
	_, _, err = st.Release(bg(), "doc", "bob")
	assertCode(t, err, CodeNotOwner)
}

func TestQueryUnknownResourceAndReadonlyBehavior(t *testing.T) {
	st, now := newTestStore(t)

	_, err := st.Query(bg(), "missing")
	assertCode(t, err, CodeResourceNotFound)

	if _, _, err := st.Acquire(bg(), "doc", "alice", 1); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// Queries must not advance expiry: after the lease passes, a query still
	// reports the stale holder, queue and expiry verbatim.
	*now = now.Add(2 * time.Second)
	state, err := st.Query(bg(), "doc")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if state.OwnerID != "alice" || state.ReentryCount != 1 {
		t.Fatalf("query mutated expired state: %+v", state)
	}
}

func TestLeaseExpiryTransfersToHeadWaiter(t *testing.T) {
	st, now := newTestStore(t)

	if _, _, err := st.Acquire(bg(), "doc", "alice", 1); err != nil {
		t.Fatalf("acquire alice: %v", err)
	}
	if _, _, err := st.Acquire(bg(), "doc", "bob", 3); err != nil {
		t.Fatalf("queue bob: %v", err)
	}
	if _, _, err := st.Acquire(bg(), "doc", "carol", 4); err != nil {
		t.Fatalf("queue carol: %v", err)
	}
	if _, err := st.Reenter(bg(), "doc", "alice"); err != nil {
		t.Fatalf("alice reenter: %v", err)
	}

	*now = now.Add(1001 * time.Millisecond)

	// A new acquire from a different holder observes the deterministic
	// transition: bob becomes holder with the lease he requested while queued.
	dave, status, err := st.Acquire(bg(), "doc", "dave", 7)
	if err != nil || status != StatusWaiting {
		t.Fatalf("dave acquire = %s, %v", status, err)
	}
	if dave.OwnerID != "bob" || dave.ReentryCount != 1 {
		t.Fatalf("post-expiry holder = %+v", dave)
	}
	bobExpiry := time.Date(2026, 10, 1, 12, 0, 4, 1_000_000, time.UTC).UTC().Format(time.RFC3339Nano)
	if dave.LeaseExpiresAt != bobExpiry {
		t.Fatalf("bob expiry = %s, want %s", dave.LeaseExpiresAt, bobExpiry)
	}
	if len(dave.WaitingOwners) != 2 ||
		dave.WaitingOwners[0].OwnerID != "carol" || dave.WaitingOwners[1].OwnerID != "dave" {
		t.Fatalf("post-expiry queue = %+v", dave.WaitingOwners)
	}

	// Late operations from the old holder cannot override the new holder.
	_, err = st.Reenter(bg(), "doc", "alice")
	assertCode(t, err, CodeLeaseExpired)
	_, _, err = st.Release(bg(), "doc", "alice")
	assertCode(t, err, CodeLeaseExpired)

	query, err := st.Query(bg(), "doc")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if query.OwnerID != "bob" {
		t.Fatalf("stale operation moved holder: %s", query.OwnerID)
	}
}

func TestExpiryTransfersExactlyOneAtATime(t *testing.T) {
	st, now := newTestStore(t)

	if _, _, err := st.Acquire(bg(), "doc", "alice", 1); err != nil {
		t.Fatalf("acquire alice: %v", err)
	}
	if _, _, err := st.Acquire(bg(), "doc", "bob", 1); err != nil {
		t.Fatalf("queue bob: %v", err)
	}
	if _, _, err := st.Acquire(bg(), "doc", "carol", 5); err != nil {
		t.Fatalf("queue carol: %v", err)
	}

	*now = now.Add(3 * time.Second)

	_, err := st.Reenter(bg(), "doc", "carol")
	assertCode(t, err, CodeNotOwner)

	state, err := st.Query(bg(), "doc")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if state.OwnerID != "bob" {
		t.Fatalf("holder = %s, want exactly one transfer to bob", state.OwnerID)
	}
	if len(state.WaitingOwners) != 1 || state.WaitingOwners[0].OwnerID != "carol" {
		t.Fatalf("queue = %+v", state.WaitingOwners)
	}
}

func TestExpiryWithEmptyQueueFreesThenGrantsFreshAcquire(t *testing.T) {
	st, now := newTestStore(t)

	if _, _, err := st.Acquire(bg(), "doc", "alice", 1); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	*now = now.Add(2 * time.Second)

	_, err := st.Reenter(bg(), "doc", "alice")
	assertCode(t, err, CodeLeaseExpired)
	_, _, err = st.Release(bg(), "doc", "alice")
	assertCode(t, err, CodeLeaseExpired)

	dave, status, err := st.Acquire(bg(), "doc", "dave", 5)
	if err != nil || status != StatusAcquired {
		t.Fatalf("fresh acquire = %s, %v", status, err)
	}
	if dave.OwnerID != "dave" || dave.ReentryCount != 1 {
		t.Fatalf("post-expiry acquire = %+v", dave)
	}
}

func TestExpiredLeaseUsesWaiterRequestedSeconds(t *testing.T) {
	st, now := newTestStore(t)

	if _, _, err := st.Acquire(bg(), "doc", "alice", 10); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := st.Acquire(bg(), "doc", "bob", 3); err != nil {
		t.Fatalf("queue bob: %v", err)
	}
	*now = now.Add(11 * time.Second)

	state, status, err := st.Acquire(bg(), "doc", "carol", 10)
	if err != nil || status != StatusWaiting {
		t.Fatalf("carol = %s, %v", status, err)
	}
	wantExpiry := time.Date(2026, 10, 1, 12, 0, 14, 0, time.UTC).Format(time.RFC3339Nano)
	if state.OwnerID != "bob" || state.LeaseExpiresAt != wantExpiry {
		t.Fatalf("bob state = %+v, want expiry %s", state, wantExpiry)
	}
}

func TestConcurrentAcquiresProduceSingleHolderAndFifoQueue(t *testing.T) {
	st, _ := newTestStore(t)

	const callers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	results := make(chan State, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		ownerID := "owner-" + strconv.Itoa(i)
		go func() {
			defer wg.Done()
			<-start
			state, _, err := st.Acquire(context.Background(), "hot", ownerID, 10)
			if err != nil {
				errs <- err
				return
			}
			results <- state
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	close(results)

	for err := range errs {
		t.Fatalf("concurrent acquire: %v", err)
	}

	holders := map[string]int{}
	for state := range results {
		holders[state.OwnerID]++
	}
	if len(holders) != 1 {
		t.Fatalf("distinct holders in responses = %d, want 1", len(holders))
	}

	final, err := st.Query(context.Background(), "hot")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if final.OwnerID == "" {
		t.Fatalf("no holder after concurrent acquires")
	}
	if len(final.WaitingOwners) != callers-1 {
		t.Fatalf("queue length = %d, want %d", len(final.WaitingOwners), callers-1)
	}
	seen := map[string]bool{}
	for _, waiting := range final.WaitingOwners {
		if seen[waiting.OwnerID] {
			t.Fatalf("duplicate queue entry for %s", waiting.OwnerID)
		}
		seen[waiting.OwnerID] = true
	}
}

func TestConcurrentStaleReleasesAfterExpiryTransferOnce(t *testing.T) {
	st, now := newTestStore(t)

	if _, _, err := st.Acquire(context.Background(), "doc", "alice", 1); err != nil {
		t.Fatalf("acquire alice: %v", err)
	}
	if _, _, err := st.Acquire(context.Background(), "doc", "bob", 5); err != nil {
		t.Fatalf("queue bob: %v", err)
	}
	if _, _, err := st.Acquire(context.Background(), "doc", "carol", 5); err != nil {
		t.Fatalf("queue carol: %v", err)
	}
	*now = now.Add(2 * time.Second)

	const workers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	leaseExpired := 0
	var mu sync.Mutex
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := st.Release(context.Background(), "doc", "alice")
			if err == nil {
				t.Errorf("stale release unexpectedly succeeded")
				return
			}
			code := asServiceError(t, err).Code
			mu.Lock()
			defer mu.Unlock()
			if code != CodeLeaseExpired {
				t.Errorf("stale release code = %s, want LEASE_EXPIRED", code)
				return
			}
			leaseExpired++
		}()
	}
	close(start)
	wg.Wait()
	if leaseExpired != workers {
		t.Fatalf("leaseExpired = %d, want %d", leaseExpired, workers)
	}

	state, err := st.Query(context.Background(), "doc")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if state.OwnerID != "bob" || state.ReentryCount != 1 {
		t.Fatalf("holder = %s reentryCount = %d, want bob/1", state.OwnerID, state.ReentryCount)
	}
	if len(state.WaitingOwners) != 1 || state.WaitingOwners[0].OwnerID != "carol" {
		t.Fatalf("queue = %+v", state.WaitingOwners)
	}
}

func TestDuplicateWaitingRejectedEvenAfterTransition(t *testing.T) {
	st, now := newTestStore(t)

	if _, _, err := st.Acquire(bg(), "doc", "alice", 1); err != nil {
		t.Fatalf("acquire alice: %v", err)
	}
	if _, _, err := st.Acquire(bg(), "doc", "bob", 5); err != nil {
		t.Fatalf("queue bob: %v", err)
	}
	if _, _, err := st.Acquire(bg(), "doc", "carol", 5); err != nil {
		t.Fatalf("queue carol: %v", err)
	}
	*now = now.Add(2 * time.Second)

	// dave's acquire reconciles alice -> bob; carol is still queued.
	if _, status, err := st.Acquire(bg(), "doc", "dave", 5); err != nil || status != StatusWaiting {
		t.Fatalf("dave acquire = %s, %v", status, err)
	}

	_, _, err := st.Acquire(bg(), "doc", "carol", 5)
	assertCode(t, err, CodeDuplicateWaiting)
}
