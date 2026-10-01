package store

// CancelWaiting removes one queued request before it is granted. Cancellation
// never touches the holder, lease, reentry count, or the remaining queue order;
// it also never triggers expiry promotion.
func (s *Store) CancelWaiting(resourceID, ownerID string) (Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return Outcome{}, err
	}
	defer tx.Rollback()

	var exists int
	if err := tx.QueryRow(
		`SELECT COUNT(1) FROM resources WHERE resource_id = ?`, resourceID,
	).Scan(&exists); err != nil {
		return Outcome{}, err
	}
	if exists == 0 {
		return Outcome{}, ErrResourceNotFound
	}

	result, err := tx.Exec(
		`DELETE FROM waiting_requests WHERE resource_id = ? AND owner_id = ?`,
		resourceID, ownerID,
	)
	if err != nil {
		return Outcome{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return Outcome{}, err
	}
	if affected == 0 {
		return Outcome{}, ErrNotWaiting
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
