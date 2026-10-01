// Package api exposes the distributed lock service over HTTP.
package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/distributed-lock-service/internal/store"
)

// NewRouter wires the public HTTP surface. Every entry keeps the error shape in
// README.md: one top-level "error" object with string "code" and "message".
func NewRouter(st *store.Store) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

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
		return http.StatusConflict, "NOT_WAITING", "caller is not a pending waiter for this resource"
	default:
		return http.StatusInternalServerError, "internal_error", "request failed"
	}
}
