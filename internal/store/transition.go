package store

import (
	"database/sql"
	"time"
)

// promoteExpired performs the deterministic transfer after a lease ran out.
//
// When a waiter exists, the first queued request becomes the new holder using the
// lease seconds it requested when it entered the queue, and is removed from the
// queue. With no waiter the resource returns to holderless state. The former owner
// is remembered so its late release/reentry keeps returning LEASE_EXPIRED even
// after a new holder took over.
func promoteExpired(tx *sql.Tx, resourceID string, row resourceRow, now time.Time) error {
	if !row.hasOwner() {
		return nil
	}

	var nextOwner string
	var nextLeaseSeconds int64
	var nextID int64
	err := tx.QueryRow(
		`SELECT id, owner_id, lease_seconds FROM waiting_requests
		 WHERE resource_id = ? ORDER BY id LIMIT 1`, resourceID,
	).Scan(&nextID, &nextOwner, &nextLeaseSeconds)
	if err == sql.ErrNoRows {
		_, err = tx.Exec(
			`UPDATE resources
			 SET owner_id = NULL, lease_expires_at = NULL, reentry_count = 0,
			     expired_owner_id = ?
			 WHERE resource_id = ?`,
			row.ownerID.String, resourceID,
		)
		return err
	}
	if err != nil {
		return err
	}

	if _, err := tx.Exec(
		`UPDATE resources
		 SET owner_id = ?, lease_expires_at = ?, reentry_count = 1,
		     expired_owner_id = ?
		 WHERE resource_id = ?`,
		nextOwner, now.Add(time.Duration(nextLeaseSeconds)*time.Second).UnixMilli(),
		row.ownerID.String, resourceID,
	); err != nil {
		return err
	}
	_, err = tx.Exec(`DELETE FROM waiting_requests WHERE id = ?`, nextID)
	return err
}

// promoteVoluntary transfers the lock because the current holder released it while
// its lease was still valid. No former-owner marker is recorded.
func promoteVoluntary(tx *sql.Tx, resourceID string, now time.Time) error {
	var nextOwner string
	var nextLeaseSeconds int64
	var nextID int64
	err := tx.QueryRow(
		`SELECT id, owner_id, lease_seconds FROM waiting_requests
		 WHERE resource_id = ? ORDER BY id LIMIT 1`, resourceID,
	).Scan(&nextID, &nextOwner, &nextLeaseSeconds)
	if err == sql.ErrNoRows {
		_, err = tx.Exec(
			`UPDATE resources
			 SET owner_id = NULL, lease_expires_at = NULL, reentry_count = 0
			 WHERE resource_id = ?`,
			resourceID,
		)
		return err
	}
	if err != nil {
		return err
	}

	if _, err := tx.Exec(
		`UPDATE resources
		 SET owner_id = ?, lease_expires_at = ?, reentry_count = 1
		 WHERE resource_id = ?`,
		nextOwner, now.Add(time.Duration(nextLeaseSeconds)*time.Second).UnixMilli(),
		resourceID,
	); err != nil {
		return err
	}
	_, err = tx.Exec(`DELETE FROM waiting_requests WHERE id = ?`, nextID)
	return err
}
