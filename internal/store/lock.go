package store

import (
	"database/sql"
	"time"
)

// Outcome status values mirror the public response codes.
const (
	StatusAcquired         = "ACQUIRED"
	StatusReentry          = "REENTRY"
	StatusWaiting          = "WAITING"
	StatusReleased         = "RELEASED"
	StatusReentryDecrement = "REENTRY_DECREMENTED"
	StatusCanceled         = "CANCELED"
)

// resourceRow is the mutable state of one resource row.
type resourceRow struct {
	ownerID        sql.NullString
	leaseExpiresAt sql.NullInt64
	reentryCount   int64
	expiredOwnerID sql.NullString
}

func (r resourceRow) hasOwner() bool { return r.ownerID.Valid }

func (r resourceRow) isExpired(now time.Time) bool {
	return r.ownerID.Valid && r.leaseExpiresAt.Valid &&
		now.UnixMilli() >= r.leaseExpiresAt.Int64
}

func readRow(q queryable, resourceID string) (resourceRow, error) {
	var row resourceRow
	err := q.QueryRow(
		`SELECT owner_id, lease_expires_at, reentry_count, expired_owner_id
		 FROM resources WHERE resource_id = ?`, resourceID,
	).Scan(&row.ownerID, &row.leaseExpiresAt, &row.reentryCount, &row.expiredOwnerID)
	return row, err
}

func isWaiting(q queryable, resourceID, ownerID string) (bool, error) {
	var count int
	err := q.QueryRow(
		`SELECT COUNT(1) FROM waiting_requests WHERE resource_id = ? AND owner_id = ?`,
		resourceID, ownerID,
	).Scan(&count)
	return count > 0, err
}

type queryable interface {
	QueryRow(query string, args ...any) *sql.Row
}
