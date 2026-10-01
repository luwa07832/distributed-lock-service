package locksvc

import "time"

// Renew extends the current holder's lease for another leaseSeconds measured from
// the moment the renewal succeeds. Only the current holder may renew while its
// lease is still valid; non-holders get ErrNotLockOwner and a former holder past
// expiry gets ErrLeaseExpired.
func (s *Service) Renew(resourceID, ownerID string, leaseSeconds int64) (Outcome, error) {
	if resourceID == "" {
		return Outcome{}, ErrInvalidLockIdentity
	}
	if ownerID == "" {
		return Outcome{}, ErrInvalidHolderIdentity
	}
	if leaseSeconds <= 0 {
		return Outcome{}, ErrInvalidLeaseDuration
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

	e.leaseSeconds = leaseSeconds
	e.leaseExpiresAt = now.Add(time.Duration(leaseSeconds) * time.Second)
	return Outcome{Status: StatusRenewed, State: s.snapshotLocked(resourceID, e)}, nil
}
