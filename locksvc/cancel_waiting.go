package locksvc

// CancelWaiting withdraws one pending waiter without changing the current holder,
// lease, reentry count, or remaining waiters. Expired leases are transferred
// first, so an owner promoted before cancellation is no longer considered a
// waiter and keeps the lock.
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

	waitingIndex := -1
	for index, waiter := range e.waiting {
		if waiter.OwnerID == ownerID {
			waitingIndex = index
			break
		}
	}
	if waitingIndex < 0 {
		return Outcome{}, ErrNotWaiting
	}

	e.waiting = append(e.waiting[:waitingIndex], e.waiting[waitingIndex+1:]...)
	return Outcome{Status: StatusCanceled, State: s.snapshotLocked(resourceID, e)}, nil
}
