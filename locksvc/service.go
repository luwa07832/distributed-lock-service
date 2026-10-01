package locksvc

import (
	"sync"
	"time"
)

// Service is the queryable, in-process lock-state service.
//
// Every method holds mu so all operations — including the deterministic transfer
// of expired leases — are serialized. Expired leases are swept at the start of
// every acquire, release, renew and query so consecutive and concurrent calls see
// the same effective state.
type Service struct {
	mu    sync.Mutex
	now   func() time.Time
	locks map[string]*entry
	// held indexes ownerID -> resourceID for owners with an active holding. It is
	// kept in sync with entries and rebuilt defensively if a transfer collision is
	// ever observed.
	held map[string]string
}

// Option customizes a Service.
type Option func(*Service)

// WithClock overrides the time source. It is intended for deterministic tests.
func WithClock(clock func() time.Time) Option {
	return func(s *Service) {
		if clock != nil {
			s.now = clock
		}
	}
}

// New creates an empty lock-state service.
func New(opts ...Option) *Service {
	s := &Service{
		now:   time.Now,
		locks: make(map[string]*entry),
		held:  make(map[string]string),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// SetClock replaces the time source after construction (tests only).
func (s *Service) SetClock(clock func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if clock != nil {
		s.now = clock
	}
}

// setHeld updates the owner -> resource index. A system-driven transfer is
// unconditional, so the most recent winning resource wins the index.
func (s *Service) setHeld(ownerID, resourceID string) {
	if ownerID == "" {
		return
	}
	s.held[ownerID] = resourceID
}

func (s *Service) clearHeld(ownerID, resourceID string) {
	if existing, ok := s.held[ownerID]; ok && existing == resourceID {
		delete(s.held, ownerID)
	}
}

// sweepLocked transfers every expired lease. With waiters the head waiter becomes
// the holder with a fresh lease measured from now; without waiters the resource
// becomes free and keeps a former-owner marker. The caller holds mu.
func (s *Service) sweepLocked(now time.Time) {
	for resourceID, e := range s.locks {
		if !e.locked() || !e.expired(now) {
			continue
		}
		oldOwner := e.ownerID
		if len(e.waiting) > 0 {
			head := e.waiting[0]
			e.waiting = e.waiting[1:]
			e.ownerID = head.OwnerID
			e.leaseSeconds = head.RequestedLeaseSeconds
			e.leaseExpiresAt = now.Add(time.Duration(head.RequestedLeaseSeconds) * time.Second)
			e.reentryCount = 1
			e.formerOwner = oldOwner
			s.setHeld(head.OwnerID, resourceID)
		} else {
			e.ownerID = ""
			e.leaseExpiresAt = time.Time{}
			e.leaseSeconds = 0
			e.reentryCount = 0
			e.formerOwner = oldOwner
		}
		s.clearHeld(oldOwner, resourceID)
	}
}

// snapshotLocked copies one record into the public view. The caller holds mu.
func (s *Service) snapshotLocked(resourceID string, e *entry) ResourceState {
	state := ResourceState{
		ResourceID: resourceID,
		Exists:     e != nil,
		Waiting:    []Waiter{},
	}
	if e == nil {
		return state
	}
	state.Locked = e.locked()
	state.OwnerID = e.ownerID
	state.LeaseExpiresAt = e.leaseExpiresAt
	state.ReentryCount = e.reentryCount
	if len(e.waiting) > 0 {
		state.Waiting = append(state.Waiting, e.waiting...)
	}
	return state
}
