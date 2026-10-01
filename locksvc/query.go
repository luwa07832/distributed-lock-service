package locksvc

import "sort"

// GetResource returns the current effective state of one resource. Expired leases
// are transferred first, so an expired holding is never reported as valid. A
// resource that has never appeared is reported with Exists == false and no error.
func (s *Service) GetResource(resourceID string) (ResourceState, error) {
	if resourceID == "" {
		return ResourceState{}, ErrInvalidLockIdentity
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.sweepLocked(s.now())
	return s.snapshotLocked(resourceID, s.locks[resourceID]), nil
}

// GetHolder returns every resource the owner currently holds, each with its lease
// deadline, duration and reentry count. Expired holdings are transferred first and
// never included. An owner that holds nothing returns an empty Holds list and no
// error.
func (s *Service) GetHolder(ownerID string) (HolderState, error) {
	if ownerID == "" {
		return HolderState{}, ErrInvalidHolderIdentity
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.sweepLocked(s.now())

	state := HolderState{OwnerID: ownerID, Holds: []HeldResource{}}
	if resourceID, ok := s.held[ownerID]; ok {
		if e := s.locks[resourceID]; e != nil && e.locked() && e.ownerID == ownerID {
			state.Holds = append(state.Holds, HeldResource{
				ResourceID:     resourceID,
				LeaseSeconds:   e.leaseSeconds,
				LeaseExpiresAt: e.leaseExpiresAt,
				ReentryCount:   e.reentryCount,
			})
			return state, nil
		}
	}
	// Defensive scan in case the index was ever out of date.
	resourceIDs := make([]string, 0)
	for resourceID, e := range s.locks {
		if e.locked() && e.ownerID == ownerID {
			resourceIDs = append(resourceIDs, resourceID)
		}
	}
	sort.Strings(resourceIDs)
	for _, resourceID := range resourceIDs {
		e := s.locks[resourceID]
		state.Holds = append(state.Holds, HeldResource{
			ResourceID:     resourceID,
			LeaseSeconds:   e.leaseSeconds,
			LeaseExpiresAt: e.leaseExpiresAt,
			ReentryCount:   e.reentryCount,
		})
	}
	return state, nil
}
