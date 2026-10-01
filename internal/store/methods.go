package store

import (
	"database/sql"
	"errors"
	"time"
)

// Acquire grants the lock, records a same-owner reentry, or enqueues a waiter.
//
// leaseSeconds is the duration the caller wants once the lock is effective; waiters
// keep their own requested duration until promotion.
func (s *Store) Acquire(resourceID, ownerID string, leaseSeconds int64) (Outcome, error) {
	if leaseSeconds <= 0 {
		return Outcome{}, ErrInvalidLeaseSecond
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return Outcome{}, err
	}
	defer tx.Rollback()

	now := s.nowTime()
	row, err := readRow(tx, resourceID)
	status := StatusAcquired
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.Exec(
			`INSERT INTO resources(resource_id, owner_id, lease_expires_at, reentry_count, expired_owner_id)
			 VALUES(?, ?, ?, 1, NULL)`,
			resourceID, ownerID, now.Add(time.Duration(leaseSeconds)*time.Second).UnixMilli(),
		); err != nil {
			return Outcome{}, err
		}
	} else if err != nil {
		return Outcome{}, err
	} else {
		if row.hasOwner() && row.isExpired(now) {
			if row.ownerID.String == ownerID {
				if err := promoteExpired(tx, resourceID, row, now); err != nil {
					return Outcome{}, err
				}
				if err := tx.Commit(); err != nil {
					return Outcome{}, err
				}
				return Outcome{}, ErrLeaseExpired
			}
			if err := promoteExpired(tx, resourceID, row, now); err != nil {
				return Outcome{}, err
			}
			if row, err = readRow(tx, resourceID); err != nil {
				return Outcome{}, err
			}
		}
		switch {
		case !row.hasOwner():
			if _, err := tx.Exec(
				`UPDATE resources
				 SET owner_id = ?, lease_expires_at = ?, reentry_count = 1,
				     expired_owner_id = CASE WHEN expired_owner_id = ? THEN NULL ELSE expired_owner_id END
				 WHERE resource_id = ?`,
				ownerID, now.Add(time.Duration(leaseSeconds)*time.Second).UnixMilli(),
				ownerID, resourceID,
			); err != nil {
				return Outcome{}, err
			}
		case row.ownerID.String == ownerID:
			if _, err := tx.Exec(
				`UPDATE resources SET reentry_count = reentry_count + 1 WHERE resource_id = ?`,
				resourceID,
			); err != nil {
				return Outcome{}, err
			}
			status = StatusReentry
		default:
			dup, err := isWaiting(tx, resourceID, ownerID)
			if err != nil {
				return Outcome{}, err
			}
			if dup {
				if err := tx.Commit(); err != nil {
					return Outcome{}, err
				}
				state, err := s.getStateLocked(resourceID)
				if err != nil {
					return Outcome{}, err
				}
				return Outcome{Status: StatusWaiting, State: state}, ErrDuplicateWaiting
			}
			if _, err := tx.Exec(
				`INSERT INTO waiting_requests(resource_id, owner_id, lease_seconds) VALUES(?, ?, ?)`,
				resourceID, ownerID, leaseSeconds,
			); err != nil {
				return Outcome{}, err
			}
			if row.expiredOwnerID.Valid && row.expiredOwnerID.String == ownerID {
				if _, err := tx.Exec(
					`UPDATE resources SET expired_owner_id = NULL WHERE resource_id = ?`, resourceID,
				); err != nil {
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
			return Outcome{Status: StatusWaiting, State: state}, nil
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
