package locksvc

import "time"

// Release frees exactly one reentry of the current holder. While the reentry count
// is above one it is decremented and the lease is kept. At the final reentry the
// lock is transferred to the head waiter, who receives a fresh lease measured from
// now; with an empty queue the record is removed and the resource is free.
//
// Only the current holder may release. A non-holder gets ErrNotLockOwner; the
// former holder acting after its lease expired gets ErrLeaseExpired.
func (s *Service) Release(resourceID, ownerID string) (Outcome, error) {
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

	status := StatusReleased
	if e.reentryCount > 1 {
		e.reentryCount--
		status = StatusReentryDecrement
	} else {
		s.transferOrRemoveLocked(resourceID, e, now)
	}
	return Outcome{Status: status, State: s.snapshotLocked(resourceID, s.locks[resourceID])}, nil
}

// transferOrRemoveLocked performs a voluntary release transfer: the head waiter
// takes over with a fresh lease, otherwise the record disappears. The caller holds
// mu.
func (s *Service) transferOrRemoveLocked(resourceID string, e *entry, now time.Time) {
	s.clearHeld(e.ownerID, resourceID)
	if len(e.waiting) == 0 {
		delete(s.locks, resourceID)
		return
	}
	head := e.waiting[0]
	e.waiting = e.waiting[1:]
	e.ownerID = head.OwnerID
	e.leaseSeconds = head.RequestedLeaseSeconds
	e.leaseExpiresAt = now.Add(time.Duration(head.RequestedLeaseSeconds) * time.Second)
	e.reentryCount = 1
	e.formerOwner = ""
	s.setHeld(head.OwnerID, resourceID)
}
