package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/distributed-lock-service/internal/store"
)

// acquireRequest is the body of POST /locks.
type acquireRequest struct {
	ResourceID   string `json:"resourceId"`
	OwnerID      string `json:"ownerId"`
	LeaseSeconds int64  `json:"leaseSeconds"`
}

// ownerRequest identifies the resource and caller for release and reenter.
type ownerRequest struct {
	ResourceID string `json:"resourceId"`
	OwnerID    string `json:"ownerId"`
}

// stateView is the fixed, public shape of a resource state response.
type stateView struct {
	ResourceID     string             `json:"resourceId"`
	OwnerID        *string            `json:"ownerId"`
	LeaseExpiresAt *string            `json:"leaseExpiresAt"`
	ReentryCount   int64              `json:"reentryCount"`
	WaitingOwners  []waitingOwnerView `json:"waitingOwners"`
}

type waitingOwnerView struct {
	OwnerID              string `json:"ownerId"`
	RequestedLeaseSecond int64  `json:"requestedLeaseSeconds"`
}

// stateResponse renders the outcome and the read view together.
type stateResponse struct {
	Status string    `json:"status"`
	State  stateView `json:"state"`
}

const leaseTimeLayout = "2006-01-02T15:04:05.000Z07:00"

func renderState(state store.State) stateView {
	view := stateView{
		ResourceID:    state.ResourceID,
		ReentryCount:  state.ReentryCount,
		WaitingOwners: []waitingOwnerView{},
	}
	if state.OwnerID != "" {
		owner := state.OwnerID
		view.OwnerID = &owner
	}
	if !state.LeaseExpiresAt.IsZero() {
		expires := state.LeaseExpiresAt.Format(leaseTimeLayout)
		view.LeaseExpiresAt = &expires
	}
	for _, waiting := range state.WaitingOwners {
		view.WaitingOwners = append(view.WaitingOwners, waitingOwnerView{
			OwnerID:              waiting.OwnerID,
			RequestedLeaseSecond: waiting.RequestedLeaseSecond,
		})
	}
	return view
}

func acquireHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req acquireRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "request body must be valid JSON")
			return
		}
		if req.ResourceID == "" {
			writeError(c, http.StatusBadRequest, "MISSING_RESOURCE_ID", "resourceId is required")
			return
		}
		if req.OwnerID == "" {
			writeError(c, http.StatusBadRequest, "MISSING_OWNER_ID", "ownerId is required")
			return
		}
		if req.LeaseSeconds <= 0 {
			writeError(c, http.StatusBadRequest, "INVALID_LEASE_SECONDS", "leaseSeconds must be greater than zero")
			return
		}

		outcome, err := st.Acquire(req.ResourceID, req.OwnerID, req.LeaseSeconds)
		if err != nil {
			status, code, message := mapStoreError(err)
			writeError(c, status, code, message)
			return
		}
		c.JSON(http.StatusOK, stateResponse{Status: outcome.Status, State: renderState(outcome.State)})
	}
}

func releaseHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req ownerRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "request body must be valid JSON")
			return
		}
		if req.ResourceID == "" {
			writeError(c, http.StatusBadRequest, "MISSING_RESOURCE_ID", "resourceId is required")
			return
		}
		if req.OwnerID == "" {
			writeError(c, http.StatusBadRequest, "MISSING_OWNER_ID", "ownerId is required")
			return
		}

		outcome, err := st.Release(req.ResourceID, req.OwnerID)
		if err != nil {
			status, code, message := mapStoreError(err)
			writeError(c, status, code, message)
			return
		}
		c.JSON(http.StatusOK, stateResponse{Status: outcome.Status, State: renderState(outcome.State)})
	}
}

func reenterHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req ownerRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "request body must be valid JSON")
			return
		}
		if req.ResourceID == "" {
			writeError(c, http.StatusBadRequest, "MISSING_RESOURCE_ID", "resourceId is required")
			return
		}
		if req.OwnerID == "" {
			writeError(c, http.StatusBadRequest, "MISSING_OWNER_ID", "ownerId is required")
			return
		}

		outcome, err := st.Reenter(req.ResourceID, req.OwnerID)
		if err != nil {
			status, code, message := mapStoreError(err)
			writeError(c, status, code, message)
			return
		}
		c.JSON(http.StatusOK, stateResponse{Status: outcome.Status, State: renderState(outcome.State)})
	}
}

func stateHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		resourceID := c.Param("resourceId")
		state, err := st.GetState(resourceID)
		if err != nil {
			status, code, message := mapStoreError(err)
			writeError(c, status, code, message)
			return
		}
		c.JSON(http.StatusOK, renderState(state))
	}
}
