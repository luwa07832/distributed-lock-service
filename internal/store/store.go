// Package store owns the SQLite file and every write the service performs.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// Sentinel errors map one-to-one to the public error codes in the service contract.
var (
	ErrResourceNotFound   = errors.New("resource not found")
	ErrLeaseExpired       = errors.New("lease expired")
	ErrNotOwner           = errors.New("not owner")
	ErrDuplicateWaiting   = errors.New("duplicate waiting")
	ErrInvalidLeaseSecond = errors.New("invalid lease seconds")
)

// WaitingOwner is one queued request, in entry order.
type WaitingOwner struct {
	OwnerID              string `json:"ownerId"`
	RequestedLeaseSecond int64  `json:"requestedLeaseSeconds"`
}

// State is the public view of one resource.
type State struct {
	ResourceID     string         `json:"resourceId"`
	OwnerID        string         `json:"ownerId"`
	LeaseExpiresAt time.Time      `json:"leaseExpiresAt"`
	ReentryCount   int64          `json:"reentryCount"`
	WaitingOwners  []WaitingOwner `json:"waitingOwners"`
}

// Outcome is the result of a state-changing lock operation.
type Outcome struct {
	Status string
	State  State
}

// reapInterval is how often expired leases are deterministically transferred in
// the background so promotion does not depend on another request arriving.
const reapInterval = 25 * time.Millisecond

// Store wraps the SQLite handle so callers never touch database/sql directly.
//
// Every lock transition holds mu so concurrent requests are serialized; the
// database itself is also pinned to a single connection and updated inside
// transactions.
type Store struct {
	db *sql.DB
	mu sync.Mutex

	now    atomic.Pointer[func() time.Time]
	cancel context.CancelFunc
	done   chan struct{}
}

// nowTime returns the current wall-clock time used for lease math.
func (s *Store) nowTime() time.Time { return (*s.now.Load())() }

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
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

	ctx, cancel := context.WithCancel(context.Background())
	clock := time.Now
	s := &Store{
		db:     db,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	s.now.Store(&clock)
	go s.runReaper(ctx)
	return s, nil
}

// Ping reports whether the storage layer is usable.
func (s *Store) Ping() error { return s.db.Ping() }

// Close releases the database handle and stops the lease reaper.
func (s *Store) Close() error {
	s.cancel()
	<-s.done
	return s.db.Close()
}

// runReaper drives deterministic transfer for every expired lease.
func (s *Store) runReaper(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reapExpired()
		}
	}
}

// reapExpired transfers every resource whose lease is due. It is safe to call
// directly from tests that do not want to wait for the background tick.
func (s *Store) reapExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowTime()
	rows, err := s.db.Query(
		`SELECT resource_id FROM resources
		 WHERE owner_id IS NOT NULL AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?`,
		now.UnixMilli(),
	)
	if err != nil {
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return
		}
		ids = append(ids, id)
	}
	rows.Close()

	for _, id := range ids {
		tx, err := s.db.Begin()
		if err != nil {
			return
		}
		row, err := readRow(tx, id)
		if err != nil {
			tx.Rollback()
			continue
		}
		if row.hasOwner() && row.isExpired(now) {
			if err := promoteExpired(tx, id, row, now); err != nil {
				tx.Rollback()
				continue
			}
		}
		if err := tx.Commit(); err != nil {
			tx.Rollback()
		}
	}
}

const schema = `
CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS resources (
	resource_id      TEXT PRIMARY KEY,
	owner_id         TEXT,
	lease_expires_at INTEGER,
	reentry_count    INTEGER NOT NULL DEFAULT 0,
	expired_owner_id TEXT
);

CREATE TABLE IF NOT EXISTS waiting_requests (
	id                INTEGER PRIMARY KEY AUTOINCREMENT,
	resource_id       TEXT NOT NULL,
	owner_id          TEXT NOT NULL,
	lease_seconds     INTEGER NOT NULL,
	UNIQUE(resource_id, owner_id),
	FOREIGN KEY(resource_id) REFERENCES resources(resource_id)
);

CREATE INDEX IF NOT EXISTS idx_waiting_resource ON waiting_requests(resource_id, id);
`

// SetClock overrides the time source. It is intended for deterministic tests and
// must be called before the store is shared across goroutines.
func (s *Store) SetClock(clock func() time.Time) { s.now.Store(&clock) }
