// Package lockstate implements the queryable lock state service.
//
// The service records, for every resource, the current holder, the lease
// expiry, the reentry count, and the FIFO waiting queue, and answers queries
// about a single resource and about every resource a holder currently owns.
// Resource and holder identities are used exactly as passed: never re-cased
// and never truncated.
//
// State lives in process memory only. Nothing is persisted, and losing the
// state with the process is expected rather than an error. Expired leases are
// settled lazily: every acquire, release, reenter, renew, and query first
// transfers every lease that already ran out, so callers always observe the
// latest valid state and an expired lease is never reported as a valid hold.
package lockstate

import (
	"sort"
	"sync"
	"time"
)

// Outcome status values mirror the public response codes.
const (
	StatusAcquired         = "ACQUIRED"
	StatusReentry          = "REENTRY"
	StatusWaiting          = "WAITING"
	StatusReleased         = "RELEASED"
	StatusReentryDecrement = "REENTRY_DECREMENTED"
	StatusRenewed          = "RENEWED"
)

// WaiterView is one queued holder, in entry order.
type WaiterView struct {
	HolderID              string
	RequestedLeaseSeconds int64
}

// ResourceView is the query result for one resource. Exists reports whether
// the service tracks a lock for the resource at all.
type ResourceView struct {
	Exists         bool
	ResourceID     string
	HolderID       string
	LeaseExpiresAt time.Time
	ReentryCount   int64
	Waiters        []WaiterView
}

// HeldLock is one resource a holder currently owns, with its lease expiry.
type HeldLock struct {
	ResourceID     string
	LeaseExpiresAt time.Time
}

// HolderView is the query result for one holder.
type HolderView struct {
	HolderID string
	Locks    []HeldLock
}

// Outcome is the result of a state-changing lock operation.
type Outcome struct {
	Status string
	State  ResourceView
}

// waiter is one queued request, in entry order.
type waiter struct {
	holderID     string
	leaseSeconds int64
}

// lockEntry is the mutable state tracked for one resource.
type lockEntry struct {
	holderID       string
	leaseExpiresAt time.Time
	reentryCount   int64
	waiters        []waiter

	// expiredHolderID remembers the most recent holder whose lease ran out so
	// its late release, reenter, or renew keeps returning LeaseExpiredError
	// even after the lock moved on.
	expiredHolderID  string
	hasExpiredHolder bool
}

func (e *lockEntry) held() bool { return e.holderID != "" }

// Service is the in-memory, queryable lock state service. Every operation
// holds mu, so concurrent callers are serialized and observe one consistent
// state.
type Service struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
	now   func() time.Time
}

// NewService returns an empty lock state service.
func NewService() *Service {
	return &Service{locks: make(map[string]*lockEntry), now: time.Now}
}

// SetClock overrides the time source. It is intended for deterministic tests
// and must be called before the service is shared across goroutines.
func (s *Service) SetClock(clock func() time.Time) { s.now = clock }

// validateIdentities rejects empty resource and holder identities.
func validateIdentities(resourceID, holderID string) error {
	if resourceID == "" {
		return &InvalidLockIdentityError{}
	}
	if holderID == "" {
		return &InvalidHolderIdentityError{}
	}
	return nil
}

// Acquire grants the lock, records a same-holder reentry, or enqueues the
// caller in entry order.
//
// reentryCount records additional reentries when a brand-new hold is
// established, so a fresh hold starts at 1 + reentryCount. Passing an explicit
// reentry count for a hold the caller already owns is rejected with
// DuplicateHoldError: the same hold must not be established twice through a
// duplicate entry in a way that bypasses the reentry counting.
func (s *Service) Acquire(resourceID, holderID string, leaseSeconds, reentryCount int64) (Outcome, error) {
	if err := validateIdentities(resourceID, holderID); err != nil {
		return Outcome{}, err
	}
	if leaseSeconds <= 0 {
		return Outcome{}, &InvalidLeaseDurationError{LeaseSeconds: leaseSeconds}
	}
	if reentryCount < 0 {
		return Outcome{}, &InvalidReentryCountError{ReentryCount: reentryCount}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	expired := s.sweepExpiredLocked(now)
	if expired[resourceID] == holderID {
		return Outcome{}, &LeaseExpiredError{ResourceID: resourceID, HolderID: holderID}
	}

	entry, known := s.locks[resourceID]
	if !known {
		entry = &lockEntry{}
		s.locks[resourceID] = entry
	}

	switch {
	case !entry.held():
		entry.holderID = holderID
		entry.leaseExpiresAt = now.Add(time.Duration(leaseSeconds) * time.Second)
		entry.reentryCount = 1 + reentryCount
		entry.forgetExpiredHolder(holderID)
		return Outcome{Status: StatusAcquired, State: viewOf(resourceID, entry)}, nil
	case entry.holderID == holderID:
		if reentryCount > 0 {
			return Outcome{}, &DuplicateHoldError{ResourceID: resourceID, HolderID: holderID}
		}
		entry.reentryCount++
		return Outcome{Status: StatusReentry, State: viewOf(resourceID, entry)}, nil
	default:
		for _, queued := range entry.waiters {
			if queued.holderID == holderID {
				return Outcome{}, &DuplicateWaiterError{ResourceID: resourceID, HolderID: holderID}
			}
		}
		entry.waiters = append(entry.waiters, waiter{holderID: holderID, leaseSeconds: leaseSeconds})
		entry.forgetExpiredHolder(holderID)
		return Outcome{Status: StatusWaiting, State: viewOf(resourceID, entry)}, nil
	}
}

// Reenter adds one reentry for the current holder. It never extends the lease
// and keeps the original lease expiry.
func (s *Service) Reenter(resourceID, holderID string) (Outcome, error) {
	if err := validateIdentities(resourceID, holderID); err != nil {
		return Outcome{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.sweepExpiredLocked(s.now())

	entry, known := s.locks[resourceID]
	if known && entry.held() && entry.holderID == holderID {
		entry.reentryCount++
		return Outcome{Status: StatusReentry, State: viewOf(resourceID, entry)}, nil
	}
	return Outcome{}, notOwnerOrExpired(resourceID, holderID, entry, known)
}

// Release frees one reentry of the current holder. Only when the reentry count
// reaches zero is the resource handed over: the head waiter becomes the new
// holder with a fresh lease counted from the transfer moment, or the resource
// turns free when the queue is empty.
func (s *Service) Release(resourceID, holderID string) (Outcome, error) {
	if err := validateIdentities(resourceID, holderID); err != nil {
		return Outcome{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	s.sweepExpiredLocked(now)

	entry, known := s.locks[resourceID]
	if known && entry.held() && entry.holderID == holderID {
		if entry.reentryCount > 1 {
			entry.reentryCount--
			return Outcome{Status: StatusReentryDecrement, State: viewOf(resourceID, entry)}, nil
		}
		s.promoteLocked(entry, now)
		return Outcome{Status: StatusReleased, State: viewOf(resourceID, entry)}, nil
	}
	return Outcome{}, notOwnerOrExpired(resourceID, holderID, entry, known)
}

// Renew restarts the current holder's lease from the renew moment. The
// reentry count and the waiting queue stay untouched.
func (s *Service) Renew(resourceID, holderID string, leaseSeconds int64) (Outcome, error) {
	if err := validateIdentities(resourceID, holderID); err != nil {
		return Outcome{}, err
	}
	if leaseSeconds <= 0 {
		return Outcome{}, &InvalidLeaseDurationError{LeaseSeconds: leaseSeconds}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	s.sweepExpiredLocked(now)

	entry, known := s.locks[resourceID]
	if known && entry.held() && entry.holderID == holderID {
		entry.leaseExpiresAt = now.Add(time.Duration(leaseSeconds) * time.Second)
		return Outcome{Status: StatusRenewed, State: viewOf(resourceID, entry)}, nil
	}
	return Outcome{}, notOwnerOrExpired(resourceID, holderID, entry, known)
}

// QueryResource returns the latest valid state of one resource. A resource the
// service has never seen yields an empty view with Exists unset — never an
// error.
func (s *Service) QueryResource(resourceID string) (ResourceView, error) {
	if resourceID == "" {
		return ResourceView{}, &InvalidLockIdentityError{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.sweepExpiredLocked(s.now())

	entry, known := s.locks[resourceID]
	if !known {
		return viewOf(resourceID, nil), nil
	}
	return viewOf(resourceID, entry), nil
}

// QueryHolder lists every resource the holder currently owns with the lease
// expiry of each, ordered by resource identity. A holder that owns nothing
// yields an empty list — never an error.
func (s *Service) QueryHolder(holderID string) (HolderView, error) {
	if holderID == "" {
		return HolderView{}, &InvalidHolderIdentityError{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.sweepExpiredLocked(s.now())

	view := HolderView{HolderID: holderID, Locks: []HeldLock{}}
	for resourceID, entry := range s.locks {
		if entry.held() && entry.holderID == holderID {
			view.Locks = append(view.Locks, HeldLock{
				ResourceID:     resourceID,
				LeaseExpiresAt: entry.leaseExpiresAt,
			})
		}
	}
	sort.Slice(view.Locks, func(i, j int) bool {
		return view.Locks[i].ResourceID < view.Locks[j].ResourceID
	})
	return view, nil
}

// notOwnerOrExpired maps a hold-scoped call that is not from the current
// holder to LeaseExpiredError for a former holder whose lease ran out, and to
// NotLockOwnerError for everyone else.
func notOwnerOrExpired(resourceID, holderID string, entry *lockEntry, known bool) error {
	if known && entry.hasExpiredHolder && entry.expiredHolderID == holderID {
		return &LeaseExpiredError{ResourceID: resourceID, HolderID: holderID}
	}
	return &NotLockOwnerError{ResourceID: resourceID, HolderID: holderID}
}

// forgetExpiredHolder clears the expired-holder marker once that holder is
// back on the resource as holder or waiter.
func (e *lockEntry) forgetExpiredHolder(holderID string) {
	if e.hasExpiredHolder && e.expiredHolderID == holderID {
		e.hasExpiredHolder = false
		e.expiredHolderID = ""
	}
}

// sweepExpiredLocked settles every lease that ran out by now: the head waiter
// becomes the new holder with a fresh lease counted from the transfer moment,
// or the resource turns free when the queue is empty. It reports the former
// holder of every resource it transferred, keyed by resource identity, so the
// triggering call can reject that holder's late operation.
func (s *Service) sweepExpiredLocked(now time.Time) map[string]string {
	var expired map[string]string
	for resourceID, entry := range s.locks {
		if !entry.held() || now.Before(entry.leaseExpiresAt) {
			continue
		}
		if expired == nil {
			expired = make(map[string]string)
		}
		expired[resourceID] = entry.holderID
		entry.expiredHolderID = entry.holderID
		entry.hasExpiredHolder = true
		s.promoteLocked(entry, now)
	}
	return expired
}

// promoteLocked hands the resource to the head waiter or turns it free. The
// new holder's lease counts from the transfer moment; the remaining waiters
// keep their entry order.
func (s *Service) promoteLocked(entry *lockEntry, now time.Time) {
	if len(entry.waiters) == 0 {
		entry.holderID = ""
		entry.leaseExpiresAt = time.Time{}
		entry.reentryCount = 0
		return
	}
	next := entry.waiters[0]
	entry.waiters = entry.waiters[1:]
	entry.holderID = next.holderID
	entry.leaseExpiresAt = now.Add(time.Duration(next.leaseSeconds) * time.Second)
	entry.reentryCount = 1
}

// viewOf renders the public view of one resource. A nil entry renders the
// empty view used for resources the service has never seen.
func viewOf(resourceID string, entry *lockEntry) ResourceView {
	view := ResourceView{ResourceID: resourceID, Waiters: []WaiterView{}}
	if entry == nil {
		return view
	}
	view.Exists = true
	view.HolderID = entry.holderID
	view.LeaseExpiresAt = entry.leaseExpiresAt
	view.ReentryCount = entry.reentryCount
	for _, queued := range entry.waiters {
		view.Waiters = append(view.Waiters, WaiterView{
			HolderID:              queued.holderID,
			RequestedLeaseSeconds: queued.leaseSeconds,
		})
	}
	return view
}
