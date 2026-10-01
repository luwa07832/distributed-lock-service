package locksvc

// CancelWaiting withdraws one pending waiting request. Only the queued record is
// removed: the current holder, lease expiry, lease seconds and reentry count stay
// untouched, and the remaining waiters keep their entry order and requested lease
// seconds.
//
// Expired leases are swept first, so a waiter already promoted by a lease transfer
// is the new holder when this runs and gets ErrNotWaiting instead of losing the new
// holding. A resource that has never appeared returns ErrResourceNotFound; a known
// resource on which the caller is not a pending waiter returns ErrNotWaiting.
func (s *Service) CancelWaiting(resourceID, ownerID string) (Outcome, error) {
	if resourceID == "" {
		return Outcome{}, ErrInvalidLockIdentity
	}
	if ownerID == "" {
		return Outcome{}, ErrInvalidHolderIdentity
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.sweepLocked(s.now())

	e := s.locks[resourceID]
	if e == nil {
		return Outcome{}, ErrResourceNotFound
	}

	index := -1
	for i, waiter := range e.waiting {
		if waiter.OwnerID == ownerID {
			index = i
			break
		}
	}
	if index < 0 {
		return Outcome{}, ErrNotWaiting
	}
	e.waiting = append(e.waiting[:index], e.waiting[index+1:]...)
	return Outcome{Status: StatusCanceled, State: s.snapshotLocked(resourceID, e)}, nil
}
