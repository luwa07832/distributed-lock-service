// Package api exposes the distributed lock service over HTTP.
package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/distributed-lock-service/internal/lockstate"
	"github.com/luwa07832/distributed-lock-service/internal/store"
)

// NewRouter wires the public HTTP surface. Every entry keeps the error shape in
// README.md: one top-level "error" object with string "code" and "message".
//
// The queryable lock-state service is mounted alongside the persistent lock
// entries under /lock-state. Callers may pass a prepared service to share one
// across routers; otherwise a fresh in-memory service is created.
func NewRouter(st *store.Store, lockStates ...*lockstate.Service) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	lockStateService := lockstate.NewService()
	if len(lockStates) > 0 && lockStates[0] != nil {
		lockStateService = lockStates[0]
	}

	router.GET("/healthz", func(c *gin.Context) {
		if err := st.Ping(); err != nil {
			writeError(c, http.StatusServiceUnavailable, "storage_unavailable", "database is not available")
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "database": "ok"})
	})

	locks := router.Group("/locks")
	{
		locks.POST("", acquireHandler(st))
		locks.POST("/release", releaseHandler(st))
		locks.POST("/reenter", reenterHandler(st))
		locks.POST("/cancel-waiting", cancelWaitingHandler(st))
		locks.GET("/:resourceId", stateHandler(st))
	}

	lockState := router.Group("/lock-state")
	{
		lockState.POST("/locks", lockStateAcquireHandler(lockStateService))
		lockState.POST("/locks/reenter", lockStateReenterHandler(lockStateService))
		lockState.POST("/locks/release", lockStateReleaseHandler(lockStateService))
		lockState.POST("/locks/renew", lockStateRenewHandler(lockStateService))
		lockState.GET("/locks/:resourceId", lockStateResourceHandler(lockStateService))
		lockState.GET("/holders/:holderId", lockStateHolderHandler(lockStateService))
	}

	router.NoRoute(func(c *gin.Context) {
		writeError(c, http.StatusNotFound, "route_not_found", "no route matches this path")
	})
	return router
}

func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}

// mapStoreError turns a store sentinel error into the published code and HTTP
// status. The returned status is 0 when err is nil.
func mapStoreError(err error) (int, string, string) {
	switch {
	case errors.Is(err, store.ErrResourceNotFound):
		return http.StatusNotFound, "RESOURCE_NOT_FOUND", "resource does not exist"
	case errors.Is(err, store.ErrNotOwner):
		return http.StatusForbidden, "NOT_OWNER", "caller is not the current holder"
	case errors.Is(err, store.ErrLeaseExpired):
		return http.StatusConflict, "LEASE_EXPIRED", "lease has expired"
	case errors.Is(err, store.ErrDuplicateWaiting):
		return http.StatusConflict, "DUPLICATE_WAITING", "owner already has a pending waiting request"
	case errors.Is(err, store.ErrNotWaiting):
		return http.StatusConflict, "NOT_WAITING", "caller has no pending waiting request"
	default:
		return http.StatusInternalServerError, "internal_error", "request failed"
	}
}
