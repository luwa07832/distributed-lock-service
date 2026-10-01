// Package store owns the SQLite file and every write the service performs.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const millisPerSecond = int64(time.Second / time.Millisecond)

// Public status values returned by the mutating lock operations.
const (
	StatusAcquired = "ACQUIRED"
	StatusWaiting  = "WAITING"
	StatusReleased = "RELEASED"
)

// Public domain error codes shared with the HTTP layer.
const (
	CodeMissingResourceID = "MISSING_RESOURCE_ID"
	CodeMissingOwnerID    = "MISSING_OWNER_ID"
	CodeInvalidLease      = "INVALID_LEASE_SECONDS"
	CodeResourceNotFound  = "RESOURCE_NOT_FOUND"
	CodeNotOwner          = "NOT_OWNER"
	CodeLeaseExpired      = "LEASE_EXPIRED"
	CodeDuplicateWaiting  = "DUPLICATE_WAITING"
)

// ServiceError carries a stable, public error code that callers can switch on.
type ServiceError struct {
	Code    string
	Message string
}

func (e *ServiceError) Error() string { return e.Code + ": " + e.Message }

func serviceError(code, message string) *ServiceError {
	return &ServiceError{Code: code, Message: message}
}

// WaitingOwner describes one queued request in arrival order.
type WaitingOwner struct {
	OwnerID               string `json:"ownerId"`
	RequestedLeaseSeconds int64  `json:"requestedLeaseSeconds"`
}

// State is the public, read-only view of one resource.
type State struct {
	ResourceID       string         `json:"resourceId"`
	OwnerID          string         `json:"ownerId"`
	LeaseExpiresAt   string         `json:"leaseExpiresAt"`
	ReentryCount     int64          `json:"reentryCount"`
	WaitingOwners    []WaitingOwner `json:"waitingOwners"`
	lastExpiredOwner string
}

func (s State) hasHolder() bool { return s.OwnerID != "" }

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db    *sql.DB
	mu    sync.Mutex
	nowFn func() time.Time
}

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	dsn := url.URL{Scheme: "file", Path: path}
	query := url.Values{}
	query.Add("_txlock", "immediate")
	query.Add("_pragma", "busy_timeout(5000)")
	dsn.RawQuery = query.Encode()

	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable wal: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	if _, err := db.Exec(`ALTER TABLE resources ADD COLUMN last_expired_owner TEXT NOT NULL DEFAULT ''`); err != nil {
		// The column already exists on databases created with the current schema.
		if !strings.Contains(err.Error(), "duplicate column name") {
			db.Close()
			return nil, fmt.Errorf("migrate resources: %w", err)
		}
	}
	return &Store{db: db, nowFn: time.Now}, nil
}

// SetClockForTest overrides the time source used for lease decisions.
func (s *Store) SetClockForTest(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	s.nowFn = now
}

// Ping reports whether the storage layer is usable.
func (s *Store) Ping() error { return s.db.Ping() }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

const schema = `
CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS resources (
	resource_id      TEXT PRIMARY KEY,
	owner_id         TEXT NOT NULL DEFAULT '',
	lease_expires_ms INTEGER NOT NULL DEFAULT 0,
	reentry_count    INTEGER NOT NULL DEFAULT 0,
	last_expired_owner TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS waiting_owners (
	id                   INTEGER PRIMARY KEY AUTOINCREMENT,
	resource_id          TEXT NOT NULL,
	owner_id             TEXT NOT NULL,
	requested_lease_secs INTEGER NOT NULL CHECK (requested_lease_secs > 0),
	UNIQUE(resource_id, owner_id)
);
`

func validateResource(resourceID string) *ServiceError {
	if strings.TrimSpace(resourceID) == "" {
		return serviceError(CodeMissingResourceID, "resourceId is required")
	}
	return nil
}

func validateOwner(ownerID string) *ServiceError {
	if strings.TrimSpace(ownerID) == "" {
		return serviceError(CodeMissingOwnerID, "ownerId is required")
	}
	return nil
}

func validateLease(leaseSeconds int64) *ServiceError {
	if leaseSeconds <= 0 {
		return serviceError(CodeInvalidLease, "leaseSeconds must be greater than zero")
	}
	return nil
}

// queryRunner covers both *sql.Tx and *sql.DB for read-only access.
type queryRunner interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// readState loads one resource with its waiting queue. The second result is
// false when no resource row exists yet. It performs no writes, so queries
// never extend leases, move the queue or unlock expired holders.
func readState(ctx context.Context, q queryRunner, resourceID string) (State, bool, error) {
	state := State{ResourceID: resourceID, WaitingOwners: []WaitingOwner{}}

	var ownerID, lastExpiredOwner string
	var expiresMillis, reentryCount int64
	err := q.QueryRowContext(ctx,
		`SELECT owner_id, lease_expires_ms, reentry_count, last_expired_owner
		 FROM resources WHERE resource_id = ?1`,
		resourceID).Scan(&ownerID, &expiresMillis, &reentryCount, &lastExpiredOwner)
	if errors.Is(err, sql.ErrNoRows) {
		return state, false, nil
	}
	if err != nil {
		return State{}, false, fmt.Errorf("load resource: %w", err)
	}

	rows, err := q.QueryContext(ctx,
		`SELECT owner_id, requested_lease_secs FROM waiting_owners
		 WHERE resource_id = ?1 ORDER BY id ASC`, resourceID)
	if err != nil {
		return State{}, false, fmt.Errorf("load waiting owners: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var waiting WaitingOwner
		if err := rows.Scan(&waiting.OwnerID, &waiting.RequestedLeaseSeconds); err != nil {
			return State{}, false, fmt.Errorf("scan waiting owner: %w", err)
		}
		state.WaitingOwners = append(state.WaitingOwners, waiting)
	}
	if err := rows.Err(); err != nil {
		return State{}, false, fmt.Errorf("iterate waiting owners: %w", err)
	}

	if ownerID != "" {
		state.OwnerID = ownerID
		state.LeaseExpiresAt = time.UnixMilli(expiresMillis).UTC().Format(time.RFC3339Nano)
		state.ReentryCount = reentryCount
	}
	state.lastExpiredOwner = lastExpiredOwner
	return state, true, nil
}

// promoteHead transfers ownership to the first waiter using the lease seconds
// requested when that waiter queued, or frees the resource when nobody waits.
func (s *Store) promoteHead(ctx context.Context, tx *sql.Tx, resourceID, expiredHolder string) error {
	var nextOwner string
	var leaseSeconds int64
	err := tx.QueryRowContext(ctx,
		`SELECT owner_id, requested_lease_secs FROM waiting_owners
		 WHERE resource_id = ?1 ORDER BY id ASC LIMIT 1`, resourceID).
		Scan(&nextOwner, &leaseSeconds)
	if errors.Is(err, sql.ErrNoRows) {
		_, err := tx.ExecContext(ctx,
			`UPDATE resources SET owner_id = '', lease_expires_ms = 0, reentry_count = 0,
			        last_expired_owner = ?2
			 WHERE resource_id = ?1`, resourceID, expiredHolder)
		if err != nil {
			return fmt.Errorf("clear expired holder: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("select next holder: %w", err)
	}

	expiresMillis := s.nowFn().UnixMilli() + leaseSeconds*millisPerSecond
	if _, err := tx.ExecContext(ctx,
		`UPDATE resources SET owner_id = ?1, lease_expires_ms = ?2, reentry_count = 1,
		        last_expired_owner = ?4
		 WHERE resource_id = ?3`, nextOwner, expiresMillis, resourceID, expiredHolder); err != nil {
		return fmt.Errorf("promote next holder: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM waiting_owners WHERE resource_id = ?1 AND owner_id = ?2`,
		resourceID, nextOwner); err != nil {
		return fmt.Errorf("dequeue promoted holder: %w", err)
	}
	return nil
}

// expireIfNeeded applies at most one deterministic transition when the current
// holder's lease is due. The returned state reflects the post-transition view;
// expiredOwner is the holder that just lost the lease (empty when nothing
// expired) so callers can reject its stale release/reentry with LEASE_EXPIRED.
func (s *Store) expireIfNeeded(ctx context.Context, tx *sql.Tx, resourceID string) (state State, expiredOwner string, err error) {
	state, found, err := readState(ctx, tx, resourceID)
	if err != nil || !found || !state.hasHolder() {
		return state, "", err
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, state.LeaseExpiresAt)
	if err != nil {
		return State{}, "", fmt.Errorf("parse lease expiry: %w", err)
	}
	now := s.nowFn()
	if expiresAt.After(now) {
		return state, "", nil
	}
	expiredOwner = state.OwnerID
	if err := s.promoteHead(ctx, tx, resourceID, expiredOwner); err != nil {
		return State{}, "", err
	}
	state, _, err = readState(ctx, tx, resourceID)
	if err != nil {
		return State{}, "", err
	}
	return state, expiredOwner, nil
}

// classifyRejectedCaller maps a non-matching caller to the right public error.
// expiredInTx is the holder that lost the lease in the current transaction;
// state.lastExpiredOwner covers the case where an earlier caller already did it.
func classifyRejectedCaller(state State, expiredInTx, ownerID string) *ServiceError {
	if expiredInTx == ownerID || state.lastExpiredOwner == ownerID {
		return serviceError(CodeLeaseExpired, "the lease has expired and ownership moved on")
	}
	return serviceError(CodeNotOwner, "caller is not the current holder")
}

// Acquire applies for resource on behalf of ownerID with leaseSeconds validity.
// A free resource is granted immediately; the same holder re-entering only
// raises reentryCount and keeps the existing expiry; other holders queue in
// arrival order, and a holder already queued is rejected as a duplicate.
func (s *Store) Acquire(ctx context.Context, resourceID, ownerID string, leaseSeconds int64) (State, string, error) {
	if err := validateResource(resourceID); err != nil {
		return State{}, "", err
	}
	if err := validateOwner(ownerID); err != nil {
		return State{}, "", err
	}
	if err := validateLease(leaseSeconds); err != nil {
		return State{}, "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var result State
	status := StatusAcquired
	var lateErr *ServiceError
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO resources (resource_id) VALUES (?1)
			 ON CONFLICT(resource_id) DO NOTHING`, resourceID); err != nil {
			return fmt.Errorf("ensure resource: %w", err)
		}

		state, _, err := s.expireIfNeeded(ctx, tx, resourceID)
		if err != nil {
			return err
		}

		switch {
		case state.hasHolder() && state.OwnerID == ownerID:
			if _, err := tx.ExecContext(ctx,
				`UPDATE resources SET reentry_count = reentry_count + 1
				 WHERE resource_id = ?1`, resourceID); err != nil {
				return fmt.Errorf("record reentry: %w", err)
			}
			state.ReentryCount++
			status = StatusAcquired
		case !state.hasHolder():
			expiresMillis := s.nowFn().UnixMilli() + leaseSeconds*millisPerSecond
			if _, err := tx.ExecContext(ctx,
				`UPDATE resources SET owner_id = ?1, lease_expires_ms = ?2, reentry_count = 1,
			        last_expired_owner = ''
				 WHERE resource_id = ?3`, ownerID, expiresMillis, resourceID); err != nil {
				return fmt.Errorf("grant lock: %w", err)
			}
			state.OwnerID = ownerID
			state.LeaseExpiresAt = time.UnixMilli(expiresMillis).UTC().Format(time.RFC3339Nano)
			state.ReentryCount = 1
			status = StatusAcquired
		default:
			for _, waiting := range state.WaitingOwners {
				if waiting.OwnerID == ownerID {
					lateErr = serviceError(CodeDuplicateWaiting,
						"owner already has a pending request in the waiting queue")
					return nil
				}
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO waiting_owners (resource_id, owner_id, requested_lease_secs)
				 VALUES (?1, ?2, ?3)`, resourceID, ownerID, leaseSeconds); err != nil {
				return fmt.Errorf("enqueue waiter: %w", err)
			}
			state.WaitingOwners = append(state.WaitingOwners, WaitingOwner{
				OwnerID:               ownerID,
				RequestedLeaseSeconds: leaseSeconds,
			})
			status = StatusWaiting
		}

		result, _, err = readState(ctx, tx, resourceID)
		return err
	})
	if err != nil {
		return State{}, "", err
	}
	if lateErr != nil {
		return State{}, "", lateErr
	}
	return result, status, nil
}

// Reenter adds one reentry level for the current holder without extending the
// lease. A non-holder gets NOT_OWNER; the previous holder acting after its
// lease transitioned gets LEASE_EXPIRED.
func (s *Store) Reenter(ctx context.Context, resourceID, ownerID string) (State, error) {
	if err := validateResource(resourceID); err != nil {
		return State{}, err
	}
	if err := validateOwner(ownerID); err != nil {
		return State{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var result State
	var lateErr *ServiceError
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		state, found, err := readState(ctx, tx, resourceID)
		if err != nil {
			return err
		}
		if !found {
			lateErr = serviceError(CodeNotOwner, "caller is not the current holder")
			return nil
		}
		state, expiredOwner, err := s.expireIfNeeded(ctx, tx, resourceID)
		if err != nil {
			return err
		}
		if !state.hasHolder() || state.OwnerID != ownerID {
			lateErr = classifyRejectedCaller(state, expiredOwner, ownerID)
			return nil
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE resources SET reentry_count = reentry_count + 1
			 WHERE resource_id = ?1`, resourceID); err != nil {
			return fmt.Errorf("record reentry: %w", err)
		}
		result, _, err = readState(ctx, tx, resourceID)
		return err
	})
	if err != nil {
		return State{}, err
	}
	if lateErr != nil {
		return State{}, lateErr
	}
	return result, nil
}

// Release removes one reentry level from the current holder. The last release
// hands the lock to the first waiter (using that waiter's requested lease) or
// frees the resource. Non-holders get NOT_OWNER; stale holders whose lease has
// already transitioned get LEASE_EXPIRED.
func (s *Store) Release(ctx context.Context, resourceID, ownerID string) (State, string, error) {
	if err := validateResource(resourceID); err != nil {
		return State{}, "", err
	}
	if err := validateOwner(ownerID); err != nil {
		return State{}, "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var result State
	status := StatusReleased
	var lateErr *ServiceError
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		state, found, err := readState(ctx, tx, resourceID)
		if err != nil {
			return err
		}
		if !found {
			lateErr = serviceError(CodeNotOwner, "caller is not the current holder")
			return nil
		}
		state, expiredOwner, err := s.expireIfNeeded(ctx, tx, resourceID)
		if err != nil {
			return err
		}
		if !state.hasHolder() || state.OwnerID != ownerID {
			lateErr = classifyRejectedCaller(state, expiredOwner, ownerID)
			return nil
		}

		if state.ReentryCount > 1 {
			if _, err := tx.ExecContext(ctx,
				`UPDATE resources SET reentry_count = reentry_count - 1
				 WHERE resource_id = ?1`, resourceID); err != nil {
				return fmt.Errorf("record release: %w", err)
			}
		} else if len(state.WaitingOwners) == 0 {
			if _, err := tx.ExecContext(ctx,
				`UPDATE resources SET owner_id = '', lease_expires_ms = 0, reentry_count = 0
				 WHERE resource_id = ?1`, resourceID); err != nil {
				return fmt.Errorf("release lock: %w", err)
			}
		} else {
			next := state.WaitingOwners[0]
			expiresMillis := s.nowFn().UnixMilli() + next.RequestedLeaseSeconds*millisPerSecond
			if _, err := tx.ExecContext(ctx,
				`UPDATE resources SET owner_id = ?1, lease_expires_ms = ?2, reentry_count = 1
				 WHERE resource_id = ?3`, next.OwnerID, expiresMillis, resourceID); err != nil {
				return fmt.Errorf("transfer lock: %w", err)
			}
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM waiting_owners WHERE resource_id = ?1 AND owner_id = ?2`,
				resourceID, next.OwnerID); err != nil {
				return fmt.Errorf("dequeue new holder: %w", err)
			}
		}
		result, _, err = readState(ctx, tx, resourceID)
		return err
	})
	if err != nil {
		return State{}, "", err
	}
	if lateErr != nil {
		return State{}, "", lateErr
	}
	return result, status, nil
}

// Query returns the stored state of one resource without changing it. Unknown
// resources return RESOURCE_NOT_FOUND.
func (s *Store) Query(ctx context.Context, resourceID string) (State, error) {
	if err := validateResource(resourceID); err != nil {
		return State{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, found, err := readState(ctx, s.db, resourceID)
	if err != nil {
		return State{}, err
	}
	if !found {
		return State{}, serviceError(CodeResourceNotFound, "resource does not exist")
	}
	return state, nil
}

func (s *Store) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}
