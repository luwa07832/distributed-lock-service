package store

import (
	"database/sql"
	"errors"
)

// CancelWaiting withdraws one pending waiting request. The current holder, the
// lease, and the reentry count are left untouched, and the remaining waiters
// keep their entry order and requested lease seconds. A caller that is not a
// pending waiter — including the current holder or an owner already promoted by
// a lease transfer — is rejected with ErrNotWaiting and nothing changes.
func (s *Store) CancelWaiting(resourceID, ownerID string) (Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return Outcome{}, err
	}
	defer tx.Rollback()

	if _, err := readRow(tx, resourceID); errors.Is(err, sql.ErrNoRows) {
		return Outcome{}, ErrResourceNotFound
	} else if err != nil {
		return Outcome{}, err
	}

	waiting, err := isWaiting(tx, resourceID, ownerID)
	if err != nil {
		return Outcome{}, err
	}
	if !waiting {
		return Outcome{}, ErrNotWaiting
	}

	if _, err := tx.Exec(
		`DELETE FROM waiting_requests WHERE resource_id = ? AND owner_id = ?`,
		resourceID, ownerID,
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
	return Outcome{Status: StatusCanceled, State: state}, nil
}
