package lockstate

import "fmt"

// InvalidLeaseDurationError reports a lease duration that is not greater than zero.
type InvalidLeaseDurationError struct {
	LeaseSeconds int64
}

func (e *InvalidLeaseDurationError) Error() string {
	return fmt.Sprintf("invalid lease duration: %d", e.LeaseSeconds)
}

// InvalidReentryCountError reports a negative reentry count.
type InvalidReentryCountError struct {
	ReentryCount int64
}

func (e *InvalidReentryCountError) Error() string {
	return fmt.Sprintf("invalid reentry count: %d", e.ReentryCount)
}

// InvalidLockIdentityError reports an empty resource identity.
type InvalidLockIdentityError struct{}

func (e *InvalidLockIdentityError) Error() string { return "resource identity must not be empty" }

// InvalidHolderIdentityError reports an empty holder identity.
type InvalidHolderIdentityError struct{}

func (e *InvalidHolderIdentityError) Error() string { return "holder identity must not be empty" }

// NotLockOwnerError reports a hold-scoped call from a caller that is not the
// current holder of the resource.
type NotLockOwnerError struct {
	ResourceID string
	HolderID   string
}

func (e *NotLockOwnerError) Error() string {
	return fmt.Sprintf("%q is not the holder of %q", e.HolderID, e.ResourceID)
}

// LeaseExpiredError reports a late operation from a holder whose lease already
// expired and was transferred.
type LeaseExpiredError struct {
	ResourceID string
	HolderID   string
}

func (e *LeaseExpiredError) Error() string {
	return fmt.Sprintf("lease of %q on %q has expired", e.HolderID, e.ResourceID)
}

// DuplicateWaiterError reports a second waiting request from a holder that is
// already queued for the same resource.
type DuplicateWaiterError struct {
	ResourceID string
	HolderID   string
}

func (e *DuplicateWaiterError) Error() string {
	return fmt.Sprintf("%q is already waiting for %q", e.HolderID, e.ResourceID)
}

// DuplicateHoldError reports an attempt to establish a hold the caller already
// owns with an explicit reentry count, which would create the same hold twice
// and bypass the reentry counting.
type DuplicateHoldError struct {
	ResourceID string
	HolderID   string
}

func (e *DuplicateHoldError) Error() string {
	return fmt.Sprintf("%q already holds %q", e.HolderID, e.ResourceID)
}
