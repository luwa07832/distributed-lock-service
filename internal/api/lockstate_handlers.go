package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/distributed-lock-service/internal/lockstate"
)

// lockStateAcquireRequest is the body of POST /lock-state/locks.
type lockStateAcquireRequest struct {
	ResourceID   string `json:"resourceId"`
	HolderID     string `json:"holderId"`
	OwnerID      string `json:"ownerId"`
	LeaseSeconds int64  `json:"leaseSeconds"`
	ReentryCount int64  `json:"reentryCount"`
}

// lockStateHolderRequest is the body of the hold-scoped lock-state endpoints.
type lockStateHolderRequest struct {
	ResourceID string `json:"resourceId"`
	HolderID   string `json:"holderId"`
	OwnerID    string `json:"ownerId"`
}

// lockStateRenewRequest is the body of POST /lock-state/locks/renew.
type lockStateRenewRequest struct {
	ResourceID   string `json:"resourceId"`
	HolderID     string `json:"holderId"`
	OwnerID      string `json:"ownerId"`
	LeaseSeconds int64  `json:"leaseSeconds"`
}

// holder returns the holder identity, accepting ownerId as an alias for
// callers of the persistent lock API.
func holderOf(holderID, ownerID string) string {
	if holderID != "" {
		return holderID
	}
	return ownerID
}

// lockStateView is the fixed, public shape of one resource in the lock-state
// service. Unknown or holderless resources report null holder fields.
type lockStateView struct {
	Exists         bool                `json:"exists"`
	ResourceID     string              `json:"resourceId"`
	HolderID       *string             `json:"holderId"`
	LeaseExpiresAt *string             `json:"leaseExpiresAt"`
	ReentryCount   int64               `json:"reentryCount"`
	WaitingHolders []waitingHolderView `json:"waitingHolders"`
}

type waitingHolderView struct {
	HolderID              string `json:"holderId"`
	RequestedLeaseSeconds int64  `json:"requestedLeaseSeconds"`
}

// lockStateResponse renders a state-changing outcome and the read view
// together, mirroring the persistent lock API shape.
type lockStateResponse struct {
	Status string        `json:"status"`
	State  lockStateView `json:"state"`
}

type heldLockView struct {
	ResourceID     string `json:"resourceId"`
	LeaseExpiresAt string `json:"leaseExpiresAt"`
}

type holderLocksResponse struct {
	HolderID string         `json:"holderId"`
	Locks    []heldLockView `json:"locks"`
}

func renderLockState(view lockstate.ResourceView) lockStateView {
	out := lockStateView{
		Exists:         view.Exists,
		ResourceID:     view.ResourceID,
		ReentryCount:   view.ReentryCount,
		WaitingHolders: []waitingHolderView{},
	}
	if view.HolderID != "" {
		holder := view.HolderID
		out.HolderID = &holder
	}
	if !view.LeaseExpiresAt.IsZero() {
		expires := view.LeaseExpiresAt.UTC().Format(leaseTimeLayout)
		out.LeaseExpiresAt = &expires
	}
	for _, queued := range view.Waiters {
		out.WaitingHolders = append(out.WaitingHolders, waitingHolderView{
			HolderID:              queued.HolderID,
			RequestedLeaseSeconds: queued.RequestedLeaseSeconds,
		})
	}
	return out
}

// mapLockStateError turns a lockstate error into the published code and HTTP
// status. The codes are the error names from the service contract.
func mapLockStateError(err error) (int, string, string) {
	var invalidLease *lockstate.InvalidLeaseDurationError
	var invalidReentry *lockstate.InvalidReentryCountError
	var invalidIdentity *lockstate.InvalidLockIdentityError
	var invalidHolder *lockstate.InvalidHolderIdentityError
	var notOwner *lockstate.NotLockOwnerError
	var leaseExpired *lockstate.LeaseExpiredError
	var duplicateWaiter *lockstate.DuplicateWaiterError
	var duplicateHold *lockstate.DuplicateHoldError
	switch {
	case errors.As(err, &invalidLease):
		return http.StatusBadRequest, "InvalidLeaseDurationError", "leaseSeconds must be greater than zero"
	case errors.As(err, &invalidReentry):
		return http.StatusBadRequest, "InvalidReentryCountError", "reentryCount must not be negative"
	case errors.As(err, &invalidIdentity):
		return http.StatusBadRequest, "InvalidLockIdentityError", "resourceId is required"
	case errors.As(err, &invalidHolder):
		return http.StatusBadRequest, "InvalidHolderIdentityError", "holderId is required"
	case errors.As(err, &notOwner):
		return http.StatusForbidden, "NotLockOwnerError", "caller is not the current holder"
	case errors.As(err, &leaseExpired):
		return http.StatusConflict, "LeaseExpiredError", "lease has expired"
	case errors.As(err, &duplicateWaiter):
		return http.StatusConflict, "DuplicateWaiterError", "holder already has a pending waiting request"
	case errors.As(err, &duplicateHold):
		return http.StatusConflict, "DuplicateHoldError", "hold already exists and must not be established twice"
	default:
		return http.StatusInternalServerError, "internal_error", "request failed"
	}
}

func writeLockStateError(c *gin.Context, err error) {
	status, code, message := mapLockStateError(err)
	writeError(c, status, code, message)
}

func lockStateAcquireHandler(svc *lockstate.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req lockStateAcquireRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "request body must be valid JSON")
			return
		}
		outcome, err := svc.Acquire(req.ResourceID, holderOf(req.HolderID, req.OwnerID), req.LeaseSeconds, req.ReentryCount)
		if err != nil {
			writeLockStateError(c, err)
			return
		}
		c.JSON(http.StatusOK, lockStateResponse{Status: outcome.Status, State: renderLockState(outcome.State)})
	}
}

func lockStateReenterHandler(svc *lockstate.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req lockStateHolderRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "request body must be valid JSON")
			return
		}
		outcome, err := svc.Reenter(req.ResourceID, holderOf(req.HolderID, req.OwnerID))
		if err != nil {
			writeLockStateError(c, err)
			return
		}
		c.JSON(http.StatusOK, lockStateResponse{Status: outcome.Status, State: renderLockState(outcome.State)})
	}
}

func lockStateReleaseHandler(svc *lockstate.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req lockStateHolderRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "request body must be valid JSON")
			return
		}
		outcome, err := svc.Release(req.ResourceID, holderOf(req.HolderID, req.OwnerID))
		if err != nil {
			writeLockStateError(c, err)
			return
		}
		c.JSON(http.StatusOK, lockStateResponse{Status: outcome.Status, State: renderLockState(outcome.State)})
	}
}

func lockStateRenewHandler(svc *lockstate.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req lockStateRenewRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "request body must be valid JSON")
			return
		}
		outcome, err := svc.Renew(req.ResourceID, holderOf(req.HolderID, req.OwnerID), req.LeaseSeconds)
		if err != nil {
			writeLockStateError(c, err)
			return
		}
		c.JSON(http.StatusOK, lockStateResponse{Status: outcome.Status, State: renderLockState(outcome.State)})
	}
}

func lockStateResourceHandler(svc *lockstate.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		view, err := svc.QueryResource(c.Param("resourceId"))
		if err != nil {
			writeLockStateError(c, err)
			return
		}
		c.JSON(http.StatusOK, renderLockState(view))
	}
}

func lockStateHolderHandler(svc *lockstate.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		view, err := svc.QueryHolder(c.Param("holderId"))
		if err != nil {
			writeLockStateError(c, err)
			return
		}
		response := holderLocksResponse{HolderID: view.HolderID, Locks: []heldLockView{}}
		for _, held := range view.Locks {
			response.Locks = append(response.Locks, heldLockView{
				ResourceID:     held.ResourceID,
				LeaseExpiresAt: held.LeaseExpiresAt.UTC().Format(leaseTimeLayout),
			})
		}
		c.JSON(http.StatusOK, response)
	}
}
