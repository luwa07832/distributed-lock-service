package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/distributed-lock-service/internal/store"
)

// NewRouter wires the public HTTP surface. Every entry keeps the error shape
// published in README.md: a single top-level "error" object with string "code"
// and "message" fields, never exposing SQL, stack traces or file paths.
func NewRouter(st *store.Store) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/healthz", healthz(st))
	router.POST("/locks/:resourceId/acquire", acquire(st))
	router.POST("/locks/:resourceId/reenter", reenter(st))
	router.POST("/locks/:resourceId/release", release(st))
	router.GET("/locks/:resourceId", query(st))

	router.NoRoute(func(c *gin.Context) {
		writeError(c, http.StatusNotFound, "route_not_found", "no route matches this path")
	})
	return router
}

func healthz(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		if err := st.Ping(); err != nil {
			writeError(c, http.StatusServiceUnavailable, "storage_unavailable", "database is not available")
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "database": "ok"})
	}
}

type acquireRequest struct {
	OwnerID      string `json:"ownerId"`
	LeaseSeconds *int64 `json:"leaseSeconds"`
}

type ownerRequest struct {
	OwnerID string `json:"ownerId"`
}

type waitingOwnerView struct {
	OwnerID               string `json:"ownerId"`
	RequestedLeaseSeconds int64  `json:"requestedLeaseSeconds"`
}

// stateView is the wire form of store.State. Pointer fields render as null
// when the resource is unlocked, and WaitingOwners always renders as an array
// so an empty queue is observable as [].
type stateView struct {
	ResourceID     string             `json:"resourceId"`
	OwnerID        *string            `json:"ownerId"`
	LeaseExpiresAt *string            `json:"leaseExpiresAt"`
	ReentryCount   int64              `json:"reentryCount"`
	WaitingOwners  []waitingOwnerView `json:"waitingOwners"`
}

type actionView struct {
	Status string `json:"status"`
	stateView
}

func acquire(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request acquireRequest
		if !bind(c, &request) {
			return
		}
		if request.LeaseSeconds == nil {
			writeError(c, http.StatusBadRequest, store.CodeInvalidLease, "leaseSeconds must be greater than zero")
			return
		}
		state, status, err := st.Acquire(c.Request.Context(), c.Param("resourceId"), request.OwnerID, *request.LeaseSeconds)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusOK, actionView{Status: status, stateView: toStateView(state)})
	}
}

func reenter(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request ownerRequest
		if !bind(c, &request) {
			return
		}
		state, err := st.Reenter(c.Request.Context(), c.Param("resourceId"), request.OwnerID)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusOK, actionView{Status: "REENTERED", stateView: toStateView(state)})
	}
}

func release(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request ownerRequest
		if !bind(c, &request) {
			return
		}
		state, status, err := st.Release(c.Request.Context(), c.Param("resourceId"), request.OwnerID)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusOK, actionView{Status: status, stateView: toStateView(state)})
	}
}

func query(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		state, err := st.Query(c.Request.Context(), c.Param("resourceId"))
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusOK, toStateView(state))
	}
}

func bind(c *gin.Context, target any) bool {
	if c.Request.Body == nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST_BODY", "request body must be valid JSON containing the required fields")
		return false
	}
	decoder := json.NewDecoder(c.Request.Body)
	if err := decoder.Decode(target); err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST_BODY", "request body must be valid JSON containing the required fields")
		return false
	}
	return true
}

func toStateView(state store.State) stateView {
	view := stateView{
		ResourceID:    state.ResourceID,
		ReentryCount:  state.ReentryCount,
		WaitingOwners: []waitingOwnerView{},
	}
	if state.OwnerID != "" {
		ownerID := state.OwnerID
		expiresAt := state.LeaseExpiresAt
		view.OwnerID = &ownerID
		view.LeaseExpiresAt = &expiresAt
	}
	for _, waiter := range state.WaitingOwners {
		view.WaitingOwners = append(view.WaitingOwners, waitingOwnerView{
			OwnerID:               waiter.OwnerID,
			RequestedLeaseSeconds: waiter.RequestedLeaseSeconds,
		})
	}
	return view
}

func writeServiceError(c *gin.Context, err error) {
	var serviceErr *store.ServiceError
	if errors.As(err, &serviceErr) {
		writeError(c, statusForCode(serviceErr.Code), serviceErr.Code, serviceErr.Message)
		return
	}
	writeError(c, http.StatusInternalServerError, "storage_unavailable", "database operation failed")
}

func statusForCode(code string) int {
	switch code {
	case store.CodeMissingResourceID, store.CodeMissingOwnerID, store.CodeInvalidLease:
		return http.StatusBadRequest
	case store.CodeResourceNotFound:
		return http.StatusNotFound
	case store.CodeNotOwner:
		return http.StatusForbidden
	default:
		// LEASE_EXPIRED and DUPLICATE_WAITING conflict with the resource's
		// current stable state.
		return http.StatusConflict
	}
}

func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}
