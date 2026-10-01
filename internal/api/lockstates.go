package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/distributed-lock-service/locksvc"
)

// RouterOption customizes the router.
type RouterOption func(*routerConfig)

type routerConfig struct {
	lockStates *locksvc.Service
}

// WithLockStateService attaches the in-process lock-state service and exposes it
// under /v2/locks. Without this option only the legacy SQLite-backed routes exist.
func WithLockStateService(svc *locksvc.Service) RouterOption {
	return func(cfg *routerConfig) {
		cfg.lockStates = svc
	}
}

// lockStateView is the fixed, public shape of a queryable lock state.
type lockStateView struct {
	ResourceID     string             `json:"resourceId"`
	Exists         bool               `json:"exists"`
	Locked         bool               `json:"locked"`
	OwnerID        *string            `json:"ownerId"`
	LeaseExpiresAt *string            `json:"leaseExpiresAt"`
	ReentryCount   int64              `json:"reentryCount"`
	WaitingOwners  []waitingOwnerView `json:"waitingOwners"`
}

type heldResourceView struct {
	ResourceID     string `json:"resourceId"`
	LeaseSeconds   int64  `json:"leaseSeconds"`
	LeaseExpiresAt string `json:"leaseExpiresAt"`
	ReentryCount   int64  `json:"reentryCount"`
}

type holderStateView struct {
	OwnerID string             `json:"ownerId"`
	Holds   []heldResourceView `json:"holds"`
}

type lockStateResponse struct {
	Status string        `json:"status"`
	State  lockStateView `json:"state"`
}

func renderLockState(state locksvc.ResourceState) lockStateView {
	view := lockStateView{
		ResourceID:    state.ResourceID,
		Exists:        state.Exists,
		Locked:        state.Locked,
		ReentryCount:  state.ReentryCount,
		WaitingOwners: []waitingOwnerView{},
	}
	if state.OwnerID != "" {
		owner := state.OwnerID
		view.OwnerID = &owner
	}
	if !state.LeaseExpiresAt.IsZero() {
		expires := state.LeaseExpiresAt.UTC().Format(leaseTimeLayout)
		view.LeaseExpiresAt = &expires
	}
	for _, waiter := range state.Waiting {
		view.WaitingOwners = append(view.WaitingOwners, waitingOwnerView{
			OwnerID:              waiter.OwnerID,
			RequestedLeaseSecond: waiter.RequestedLeaseSeconds,
		})
	}
	return view
}

func renderHolderState(state locksvc.HolderState) holderStateView {
	view := holderStateView{OwnerID: state.OwnerID, Holds: []heldResourceView{}}
	for _, held := range state.Holds {
		view.Holds = append(view.Holds, heldResourceView{
			ResourceID:     held.ResourceID,
			LeaseSeconds:   held.LeaseSeconds,
			LeaseExpiresAt: held.LeaseExpiresAt.UTC().Format(leaseTimeLayout),
			ReentryCount:   held.ReentryCount,
		})
	}
	return view
}

// mapLockStateError maps the new sentinel errors to published codes and statuses.
func mapLockStateError(err error) (int, string, string) {
	switch {
	case errors.Is(err, locksvc.ErrInvalidLeaseDuration):
		return http.StatusBadRequest, "INVALID_LEASE_DURATION", "leaseSeconds must be greater than zero"
	case errors.Is(err, locksvc.ErrInvalidReentryCount):
		return http.StatusBadRequest, "INVALID_REENTRY_COUNT", "reentryCount must not be negative"
	case errors.Is(err, locksvc.ErrInvalidLockIdentity):
		return http.StatusBadRequest, "INVALID_LOCK_IDENTITY", "resourceId is required"
	case errors.Is(err, locksvc.ErrInvalidHolderIdentity):
		return http.StatusBadRequest, "INVALID_HOLDER_IDENTITY", "ownerId is required"
	case errors.Is(err, locksvc.ErrNotLockOwner):
		return http.StatusForbidden, "NOT_LOCK_OWNER", "caller is not the current holder"
	case errors.Is(err, locksvc.ErrLeaseExpired):
		return http.StatusConflict, "LEASE_EXPIRED", "lease has expired"
	case errors.Is(err, locksvc.ErrDuplicateWaiter):
		return http.StatusConflict, "DUPLICATE_WAITER", "owner already has a pending waiting request"
	case errors.Is(err, locksvc.ErrDuplicateHold):
		return http.StatusConflict, "DUPLICATE_HOLD", "owner already holds another resource"
	default:
		return http.StatusInternalServerError, "internal_error", "request failed"
	}
}

type lockAcquireRequest struct {
	ResourceID   string `json:"resourceId"`
	OwnerID      string `json:"ownerId"`
	LeaseSeconds int64  `json:"leaseSeconds"`
	ReentryCount *int64 `json:"reentryCount,omitempty"`
}

type lockIdentityRequest struct {
	ResourceID   string `json:"resourceId"`
	OwnerID      string `json:"ownerId"`
	LeaseSeconds int64  `json:"leaseSeconds,omitempty"`
}

func registerLockStateRoutes(router *gin.Engine, svc *locksvc.Service) {
	group := router.Group("/v2/locks")

	group.POST("/acquire", func(c *gin.Context) {
		var req lockAcquireRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "request body must be valid JSON")
			return
		}
		var outcome locksvc.Outcome
		var err error
		if req.ReentryCount != nil {
			outcome, err = svc.AcquireWithReentry(req.ResourceID, req.OwnerID, req.LeaseSeconds, *req.ReentryCount)
		} else {
			outcome, err = svc.Acquire(req.ResourceID, req.OwnerID, req.LeaseSeconds)
		}
		if err != nil {
			status, code, message := mapLockStateError(err)
			writeError(c, status, code, message)
			return
		}
		c.JSON(http.StatusOK, lockStateResponse{Status: outcome.Status, State: renderLockState(outcome.State)})
	})

	handleIdentity := func(action func(resourceID, ownerID string) (locksvc.Outcome, error)) gin.HandlerFunc {
		return func(c *gin.Context) {
			var req lockIdentityRequest
			if err := c.ShouldBindJSON(&req); err != nil {
				writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "request body must be valid JSON")
				return
			}
			outcome, err := action(req.ResourceID, req.OwnerID)
			if err != nil {
				status, code, message := mapLockStateError(err)
				writeError(c, status, code, message)
				return
			}
			c.JSON(http.StatusOK, lockStateResponse{Status: outcome.Status, State: renderLockState(outcome.State)})
		}
	}

	group.POST("/release", handleIdentity(svc.Release))
	group.POST("/reenter", handleIdentity(svc.Reenter))

	group.POST("/renew", func(c *gin.Context) {
		var req lockIdentityRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "request body must be valid JSON")
			return
		}
		outcome, err := svc.Renew(req.ResourceID, req.OwnerID, req.LeaseSeconds)
		if err != nil {
			status, code, message := mapLockStateError(err)
			writeError(c, status, code, message)
			return
		}
		c.JSON(http.StatusOK, lockStateResponse{Status: outcome.Status, State: renderLockState(outcome.State)})
	})

	group.GET("/resources/:resourceId", func(c *gin.Context) {
		state, err := svc.GetResource(c.Param("resourceId"))
		if err != nil {
			status, code, message := mapLockStateError(err)
			writeError(c, status, code, message)
			return
		}
		c.JSON(http.StatusOK, renderLockState(state))
	})

	group.GET("/holders/:ownerId", func(c *gin.Context) {
		state, err := svc.GetHolder(c.Param("ownerId"))
		if err != nil {
			status, code, message := mapLockStateError(err)
			writeError(c, status, code, message)
			return
		}
		c.JSON(http.StatusOK, renderHolderState(state))
	})
}
