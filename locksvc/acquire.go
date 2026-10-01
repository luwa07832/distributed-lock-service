package locksvc

import "time"

// Acquire grants the lock immediately, records a same-holder reentry, or enqueues
// the caller behind the current holder.
//
// leaseSeconds must be positive; it is measured from the successful instant. When
// the resource is free the caller becomes the holder at once. The same holder
// acquiring the resource it already holds while the lease is valid only bumps the
// reentry count and keeps the original expiry. A holder that tries to establish a
// holding on another resource is rejected with ErrDuplicateHold; a holder already
// queued on the same resource is rejected with ErrDuplicateWaiter.
func (s *Service) Acquire(resourceID, ownerID string, leaseSeconds int64) (Outcome, error) {
	return s.AcquireWithReentry(resourceID, ownerID, leaseSeconds, 1)
}

// AcquireWithReentry works like Acquire but sets the initial reentry count of a new
// holding. A negative count is rejected with ErrInvalidReentryCount; zero falls
// back to the default of one. The count is ignored for reentry and queued calls.
func (s *Service) AcquireWithReentry(resourceID, ownerID string, leaseSeconds, initialReentry int64) (Outcome, error) {
	if resourceID == "" {
		return Outcome{}, ErrInvalidLockIdentity
	}
	if ownerID == "" {
		return Outcome{}, ErrInvalidHolderIdentity
	}
	if leaseSeconds <= 0 {
		return Outcome{}, ErrInvalidLeaseDuration
	}
	if initialReentry < 0 {
		return Outcome{}, ErrInvalidReentryCount
	}
	if initialReentry == 0 {
		initialReentry = 1
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	s.sweepLocked(now)

	e := s.locks[resourceID]
	switch {
	case e != nil && e.ownerID == ownerID:
		// The lease is valid here: an expired holding was just swept.
		e.reentryCount++
		return Outcome{Status: StatusReentry, State: s.snapshotLocked(resourceID, e)}, nil
	}

	if heldResource, ok := s.held[ownerID]; ok && heldResource != resourceID {
		return Outcome{}, ErrDuplicateHold
	}

	if e == nil || !e.locked() {
		if e == nil {
			e = &entry{}
			s.locks[resourceID] = e
		}
		e.ownerID = ownerID
		e.leaseSeconds = leaseSeconds
		e.leaseExpiresAt = now.Add(time.Duration(leaseSeconds) * time.Second)
		e.reentryCount = initialReentry
		e.formerOwner = ""
		s.setHeld(ownerID, resourceID)
		return Outcome{Status: StatusAcquired, State: s.snapshotLocked(resourceID, e)}, nil
	}

	for _, waiter := range e.waiting {
		if waiter.OwnerID == ownerID {
			return Outcome{}, ErrDuplicateWaiter
		}
	}
	if e.formerOwner == ownerID {
		e.formerOwner = ""
	}
	e.waiting = append(e.waiting, Waiter{
		OwnerID:               ownerID,
		RequestedLeaseSeconds: leaseSeconds,
	})
	return Outcome{Status: StatusWaiting, State: s.snapshotLocked(resourceID, e)}, nil
}

// Reenter adds one reentry for the current holder. It never extends the lease.
// A non-holder gets ErrNotLockOwner; a holder whose lease expired gets
// ErrLeaseExpired.
func (s *Service) Reenter(resourceID, ownerID string) (Outcome, error) {
	if resourceID == "" {
		return Outcome{}, ErrInvalidLockIdentity
	}
	if ownerID == "" {
		return Outcome{}, ErrInvalidHolderIdentity
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	s.sweepLocked(now)

	e := s.locks[resourceID]
	if e == nil || !e.locked() {
		if e != nil && e.formerOwner == ownerID {
			return Outcome{}, ErrLeaseExpired
		}
		return Outcome{}, ErrNotLockOwner
	}
	if e.ownerID != ownerID {
		if e.formerOwner == ownerID {
			return Outcome{}, ErrLeaseExpired
		}
		return Outcome{}, ErrNotLockOwner
	}

	e.reentryCount++
	return Outcome{Status: StatusReentry, State: s.snapshotLocked(resourceID, e)}, nil
}
