package store

import (
	"database/sql"
	"errors"
	"time"
)

// GetState reads one resource without changing the holder, lease, or queue.
// A resource that has never been seen returns ErrResourceNotFound; a known but
// holderless resource returns empty owner fields, a zero reentry count, and an
// empty waiting queue.
func (s *Store) GetState(resourceID string) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getStateLocked(resourceID)
}

// getStateLocked assumes the caller already holds mu.
func (s *Store) getStateLocked(resourceID string) (State, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return State{}, err
	}
	defer tx.Rollback()

	var state State
	state.ResourceID = resourceID
	state.WaitingOwners = []WaitingOwner{}

	var ownerID sql.NullString
	var leaseExpiresAt sql.NullInt64
	err = tx.QueryRow(
		`SELECT owner_id, lease_expires_at FROM resources WHERE resource_id = ?`,
		resourceID,
	).Scan(&ownerID, &leaseExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return State{}, ErrResourceNotFound
	}
	if err != nil {
		return State{}, err
	}
	if ownerID.Valid {
		state.OwnerID = ownerID.String
	}
	if leaseExpiresAt.Valid {
		state.LeaseExpiresAt = time.UnixMilli(leaseExpiresAt.Int64).UTC()
	}
	if err := tx.QueryRow(
		`SELECT reentry_count FROM resources WHERE resource_id = ?`, resourceID,
	).Scan(&state.ReentryCount); err != nil {
		return State{}, err
	}

	rows, err := tx.Query(
		`SELECT owner_id, lease_seconds FROM waiting_requests
		 WHERE resource_id = ? ORDER BY id`, resourceID,
	)
	if err != nil {
		return State{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var waiting WaitingOwner
		if err := rows.Scan(&waiting.OwnerID, &waiting.RequestedLeaseSecond); err != nil {
			return State{}, err
		}
		state.WaitingOwners = append(state.WaitingOwners, waiting)
	}
	return state, rows.Err()
}
