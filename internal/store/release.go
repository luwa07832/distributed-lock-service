package store

import (
	"database/sql"
	"errors"
)

// Release frees one reentry of the current holder. The final reentry transfers the
// lock to the first waiter, or leaves the resource holderless.
func (s *Store) Release(resourceID, ownerID string) (Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return Outcome{}, err
	}
	defer tx.Rollback()

	now := s.nowTime()
	row, err := readRow(tx, resourceID)
	if errors.Is(err, sql.ErrNoRows) {
		return Outcome{}, ErrResourceNotFound
	}
	if err != nil {
		return Outcome{}, err
	}

	if row.hasOwner() && row.isExpired(now) && row.ownerID.String == ownerID {
		if err := promoteExpired(tx, resourceID, row, now); err != nil {
			return Outcome{}, err
		}
		if err := tx.Commit(); err != nil {
			return Outcome{}, err
		}
		return Outcome{}, ErrLeaseExpired
	}
	if !row.hasOwner() || row.ownerID.String != ownerID {
		if row.expiredOwnerID.Valid && row.expiredOwnerID.String == ownerID {
			return Outcome{}, ErrLeaseExpired
		}
		return Outcome{}, ErrNotOwner
	}

	status := StatusReleased
	if row.reentryCount > 1 {
		if _, err := tx.Exec(
			`UPDATE resources SET reentry_count = reentry_count - 1 WHERE resource_id = ?`,
			resourceID,
		); err != nil {
			return Outcome{}, err
		}
		status = StatusReentryDecrement
	} else {
		if err := promoteVoluntary(tx, resourceID, now); err != nil {
			return Outcome{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return Outcome{}, err
	}
	state, err := s.getStateLocked(resourceID)
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{Status: status, State: state}, nil
}

// Reenter adds one reentry for the current holder. It never extends the lease and
// returns the same ownerId and leaseExpiresAt as the current holding.
func (s *Store) Reenter(resourceID, ownerID string) (Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return Outcome{}, err
	}
	defer tx.Rollback()

	now := s.nowTime()
	row, err := readRow(tx, resourceID)
	if errors.Is(err, sql.ErrNoRows) {
		return Outcome{}, ErrResourceNotFound
	}
	if err != nil {
		return Outcome{}, err
	}

	if row.hasOwner() && row.isExpired(now) && row.ownerID.String == ownerID {
		if err := promoteExpired(tx, resourceID, row, now); err != nil {
			return Outcome{}, err
		}
		if err := tx.Commit(); err != nil {
			return Outcome{}, err
		}
		return Outcome{}, ErrLeaseExpired
	}
	if !row.hasOwner() || row.ownerID.String != ownerID {
		if row.expiredOwnerID.Valid && row.expiredOwnerID.String == ownerID {
			return Outcome{}, ErrLeaseExpired
		}
		return Outcome{}, ErrNotOwner
	}

	if _, err := tx.Exec(
		`UPDATE resources SET reentry_count = reentry_count + 1 WHERE resource_id = ?`,
		resourceID,
	); err != nil {
		return Outcome{}, err
	}
	if err := tx.Commit(); err != nil {
		return Outcome{}, err
	}
	state, err := s.getStateLocked(resourceID)
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{Status: StatusReentry, State: state}, nil
}
