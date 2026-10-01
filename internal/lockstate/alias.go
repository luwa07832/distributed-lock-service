// Package lockstate re-exports the top-level in-process lock-state service so the
// feature is available under the internal import path as well.
package lockstate

import (
	"time"

	base "github.com/luwa07832/distributed-lock-service/locksvc"
)

// Sentinel errors.
var (
	ErrInvalidLeaseDuration  = base.ErrInvalidLeaseDuration
	ErrInvalidReentryCount   = base.ErrInvalidReentryCount
	ErrInvalidLockIdentity   = base.ErrInvalidLockIdentity
	ErrInvalidHolderIdentity = base.ErrInvalidHolderIdentity
	ErrNotLockOwner          = base.ErrNotLockOwner
	ErrLeaseExpired          = base.ErrLeaseExpired
	ErrDuplicateWaiter       = base.ErrDuplicateWaiter
	ErrDuplicateHold         = base.ErrDuplicateHold

	InvalidLeaseDurationError  = base.InvalidLeaseDurationError
	InvalidReentryCountError   = base.InvalidReentryCountError
	InvalidLockIdentityError   = base.InvalidLockIdentityError
	InvalidHolderIdentityError = base.InvalidHolderIdentityError
	NotLockOwnerError          = base.NotLockOwnerError
	LeaseExpiredError          = base.LeaseExpiredError
	DuplicateWaiterError       = base.DuplicateWaiterError
	DuplicateHoldError         = base.DuplicateHoldError
)

// Outcome status values.
const (
	StatusAcquired         = base.StatusAcquired
	StatusReentry          = base.StatusReentry
	StatusWaiting          = base.StatusWaiting
	StatusReleased         = base.StatusReleased
	StatusReentryDecrement = base.StatusReentryDecrement
	StatusRenewed          = base.StatusRenewed
)

// Types.
type (
	Waiter        = base.Waiter
	ResourceState = base.ResourceState
	HeldResource  = base.HeldResource
	HolderState   = base.HolderState
	Outcome       = base.Outcome
	Service       = base.Service
	Option        = base.Option
)

// New creates an empty lock-state service.
func New(opts ...Option) *Service { return base.New(opts...) }

// WithClock overrides the time source.
func WithClock(clock func() time.Time) Option { return base.WithClock(clock) }
