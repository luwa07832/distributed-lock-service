// Package locksvc is an in-process, queryable lock-state service.
//
// It records one record per resource: the current holder, the lease expiry, the
// reentry count, and the waiting queue. All state lives in memory for the lifetime
// of the process; no persistence files are written and losing state on shutdown is
// not an error. Identifiers are used exactly as supplied, without case folding or
// truncation.
package locksvc

import "errors"

// Sentinel errors carry the names required by the service contract. The canonical
// names use the Error suffix; Err-prefixed aliases match the existing store layer.
var (
	ErrInvalidLeaseDuration  = errors.New("invalid lease duration")
	ErrInvalidReentryCount   = errors.New("invalid reentry count")
	ErrInvalidLockIdentity   = errors.New("invalid lock identity")
	ErrInvalidHolderIdentity = errors.New("invalid holder identity")
	ErrNotLockOwner          = errors.New("not lock owner")
	ErrLeaseExpired          = errors.New("lease expired")
	ErrDuplicateWaiter       = errors.New("duplicate waiter")
	ErrDuplicateHold         = errors.New("duplicate hold")

	InvalidLeaseDurationError  = ErrInvalidLeaseDuration
	InvalidReentryCountError   = ErrInvalidReentryCount
	InvalidLockIdentityError   = ErrInvalidLockIdentity
	InvalidHolderIdentityError = ErrInvalidHolderIdentity
	NotLockOwnerError          = ErrNotLockOwner
	LeaseExpiredError          = ErrLeaseExpired
	DuplicateWaiterError       = ErrDuplicateWaiter
	DuplicateHoldError         = ErrDuplicateHold
)
