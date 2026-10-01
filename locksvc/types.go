package locksvc

import "time"

// Outcome status values. ACQUIRED/REENTRY/WAITING/RELEASED/REENTRY_DECREMENTED keep
// the semantics of the existing resource-lock entry points; RENEWED is returned by
// Renew.
const (
	StatusAcquired         = "ACQUIRED"
	StatusReentry          = "REENTRY"
	StatusWaiting          = "WAITING"
	StatusReleased         = "RELEASED"
	StatusReentryDecrement = "REENTRY_DECREMENTED"
	StatusRenewed          = "RENEWED"
)

// Waiter is one queued request, in enqueue order.
type Waiter struct {
	OwnerID               string `json:"ownerId"`
	RequestedLeaseSeconds int64  `json:"requestedLeaseSeconds"`
}

// ResourceState is the full, current view of one resource.
//
// Exists is false for a resource that has never appeared. After a lease expires
// with no waiters the record stays around as a former-owner marker and Exists is
// still true while Locked is false; after a voluntary release with an empty queue
// the record is removed. When Locked is false, OwnerID is empty, LeaseExpiresAt is
// the zero time and ReentryCount is 0.
type ResourceState struct {
	ResourceID     string    `json:"resourceId"`
	Exists         bool      `json:"exists"`
	Locked         bool      `json:"locked"`
	OwnerID        string    `json:"ownerId"`
	LeaseExpiresAt time.Time `json:"leaseExpiresAt"`
	ReentryCount   int64     `json:"reentryCount"`
	Waiting        []Waiter  `json:"waitingOwners"`
}

// HeldResource is one resource currently held by a queried owner.
type HeldResource struct {
	ResourceID     string    `json:"resourceId"`
	LeaseSeconds   int64     `json:"leaseSeconds"`
	LeaseExpiresAt time.Time `json:"leaseExpiresAt"`
	ReentryCount   int64     `json:"reentryCount"`
}

// HolderState answers an owner query.
type HolderState struct {
	OwnerID string         `json:"ownerId"`
	Holds   []HeldResource `json:"holds"`
}

// Outcome is the result of a state-changing operation.
type Outcome struct {
	Status string
	State  ResourceState
}

// entry is the mutable record of one resource.
type entry struct {
	ownerID        string
	leaseExpiresAt time.Time
	leaseSeconds   int64
	reentryCount   int64
	waiting        []Waiter
	// formerOwner remembers the holder whose lease expired so its late release or
	// renewal keeps returning ErrLeaseExpired. It is cleared when that owner starts
	// a fresh holding or enqueues on the resource again.
	formerOwner string
}

func (e *entry) locked() bool { return e.ownerID != "" }

func (e *entry) expired(now time.Time) bool {
	return e.ownerID != "" && !now.Before(e.leaseExpiresAt)
}
