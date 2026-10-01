package api

import (
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luwa07832/distributed-lock-service/internal/store"
	"github.com/luwa07832/distributed-lock-service/locksvc"
)

type testClock struct {
	base    time.Time
	advance atomic.Int64
}

func (c *testClock) now() time.Time      { return c.base.Add(time.Duration(c.advance.Load())) }
func (c *testClock) add(d time.Duration) { c.advance.Add(int64(d)) }

func newLockStateRouter(t *testing.T) (http.Handler, *testClock) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	clock := &testClock{base: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	svc := locksvc.New(locksvc.WithClock(clock.now))
	return NewRouter(st, WithLockStateService(svc)), clock
}

func TestLockStateAcquireReentryReleaseFlow(t *testing.T) {
	router, _ := newLockStateRouter(t)

	first := doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r", "ownerId": "a", "leaseSeconds": 30,
	})
	if first.Code != http.StatusOK {
		t.Fatalf("acquire status = %d body = %s", first.Code, first.Body.String())
	}
	body := decode(t, first)
	if body["status"] != "ACQUIRED" {
		t.Fatalf("status = %v", body["status"])
	}
	state := body["state"].(map[string]any)
	if state["resourceId"] != "r" || state["ownerId"] != "a" {
		t.Fatalf("state = %v", state)
	}
	if state["reentryCount"].(float64) != 1 {
		t.Fatalf("reentryCount = %v", state["reentryCount"])
	}
	if state["locked"] != true || state["exists"] != true {
		t.Fatalf("flags = %v", state)
	}
	expiry := state["leaseExpiresAt"].(string)
	if expiry != "2026-01-01T12:00:30.000Z" {
		t.Fatalf("expiry = %q", expiry)
	}

	reentry := doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r", "ownerId": "a", "leaseSeconds": 30,
	})
	if decode(t, reentry)["status"] != "REENTRY" {
		t.Fatalf("reentry body = %s", reentry.Body.String())
	}

	release := doJSON(t, router, http.MethodPost, "/v2/locks/release", map[string]any{
		"resourceId": "r", "ownerId": "a",
	})
	releaseBody := decode(t, release)
	if releaseBody["status"] != "REENTRY_DECREMENTED" {
		t.Fatalf("release = %s", release.Body.String())
	}

	done := doJSON(t, router, http.MethodPost, "/v2/locks/release", map[string]any{
		"resourceId": "r", "ownerId": "a",
	})
	if decode(t, done)["status"] != "RELEASED" {
		t.Fatalf("done = %s", done.Body.String())
	}

	query := doJSON(t, router, http.MethodGet, "/v2/locks/resources/r", nil)
	queryBody := decode(t, query)
	if queryBody["exists"] != false || queryBody["locked"] != false {
		t.Fatalf("query = %s", query.Body.String())
	}
	if queryBody["ownerId"] != nil || queryBody["leaseExpiresAt"] != nil {
		t.Fatalf("free resource leaked fields: %s", query.Body.String())
	}
}

func TestLockStateQueuesAndRejectsDuplicates(t *testing.T) {
	router, _ := newLockStateRouter(t)

	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r", "ownerId": "a", "leaseSeconds": 10,
	})
	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r", "ownerId": "b", "leaseSeconds": 20,
	})
	dup := doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r", "ownerId": "b", "leaseSeconds": 20,
	})
	if dup.Code != http.StatusConflict || errorCode(dup) != "DUPLICATE_WAITER" {
		t.Fatalf("dup = %d %s", dup.Code, dup.Body.String())
	}
}

func TestLockStateDuplicateHoldAndOwnerErrors(t *testing.T) {
	router, _ := newLockStateRouter(t)

	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r1", "ownerId": "a", "leaseSeconds": 10,
	})
	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r2", "ownerId": "b", "leaseSeconds": 10,
	})
	dup := doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r2", "ownerId": "a", "leaseSeconds": 10,
	})
	if dup.Code != http.StatusConflict || errorCode(dup) != "DUPLICATE_HOLD" {
		t.Fatalf("dup hold = %d %s", dup.Code, dup.Body.String())
	}

	notOwner := doJSON(t, router, http.MethodPost, "/v2/locks/release", map[string]any{
		"resourceId": "r2", "ownerId": "a",
	})
	if notOwner.Code != http.StatusForbidden || errorCode(notOwner) != "NOT_LOCK_OWNER" {
		t.Fatalf("not owner = %d %s", notOwner.Code, notOwner.Body.String())
	}

	badLease := doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r3", "ownerId": "c", "leaseSeconds": 0,
	})
	if badLease.Code != http.StatusBadRequest || errorCode(badLease) != "INVALID_LEASE_DURATION" {
		t.Fatalf("bad lease = %d %s", badLease.Code, badLease.Body.String())
	}

	emptyResource := doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "", "ownerId": "c", "leaseSeconds": 5,
	})
	if emptyResource.Code != http.StatusBadRequest || errorCode(emptyResource) != "INVALID_LOCK_IDENTITY" {
		t.Fatalf("empty resource = %d %s", emptyResource.Code, emptyResource.Body.String())
	}

	emptyOwner := doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r3", "ownerId": "", "leaseSeconds": 5,
	})
	if emptyOwner.Code != http.StatusBadRequest || errorCode(emptyOwner) != "INVALID_HOLDER_IDENTITY" {
		t.Fatalf("empty owner = %d %s", emptyOwner.Code, emptyOwner.Body.String())
	}

	negativeReentry := doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r3", "ownerId": "c", "leaseSeconds": 5, "reentryCount": -2,
	})
	if negativeReentry.Code != http.StatusBadRequest || errorCode(negativeReentry) != "INVALID_REENTRY_COUNT" {
		t.Fatalf("negative reentry = %d %s", negativeReentry.Code, negativeReentry.Body.String())
	}
}

func TestLockStateExpiryTransfersAndRenews(t *testing.T) {
	router, clock := newLockStateRouter(t)

	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r", "ownerId": "a", "leaseSeconds": 10,
	})
	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "r", "ownerId": "b", "leaseSeconds": 20,
	})

	clock.add(10 * time.Second)

	late := doJSON(t, router, http.MethodPost, "/v2/locks/release", map[string]any{
		"resourceId": "r", "ownerId": "a",
	})
	if late.Code != http.StatusConflict || errorCode(late) != "LEASE_EXPIRED" {
		t.Fatalf("late release = %d %s", late.Code, late.Body.String())
	}

	state := decode(t, doJSON(t, router, http.MethodGet, "/v2/locks/resources/r", nil))
	if state["ownerId"] != "b" {
		t.Fatalf("owner = %v", state["ownerId"])
	}
	if state["leaseExpiresAt"].(string) != "2026-01-01T12:00:30.000Z" {
		t.Fatalf("expiry = %v", state["leaseExpiresAt"])
	}

	holderA := decode(t, doJSON(t, router, http.MethodGet, "/v2/locks/holders/a", nil))
	if len(holderA["holds"].([]any)) != 0 {
		t.Fatalf("old owner holds = %v", holderA["holds"])
	}
	holderB := decode(t, doJSON(t, router, http.MethodGet, "/v2/locks/holders/b", nil))
	holds := holderB["holds"].([]any)
	if len(holds) != 1 || holds[0].(map[string]any)["resourceId"] != "r" {
		t.Fatalf("holder b = %v", holderB["holds"])
	}

	clock.add(5 * time.Second)
	renew := doJSON(t, router, http.MethodPost, "/v2/locks/renew", map[string]any{
		"resourceId": "r", "ownerId": "b", "leaseSeconds": 15,
	})
	renewBody := decode(t, renew)
	if renewBody["status"] != "RENEWED" {
		t.Fatalf("renew = %s", renew.Body.String())
	}
	newExpiry := renewBody["state"].(map[string]any)["leaseExpiresAt"].(string)
	if newExpiry != "2026-01-01T12:00:30.000Z" {
		t.Fatalf("renewed expiry = %q", newExpiry)
	}
}

func TestLockStateUnknownResourceReturnsEmptyResult(t *testing.T) {
	router, _ := newLockStateRouter(t)

	recorder := doJSON(t, router, http.MethodGet, "/v2/locks/resources/ghost", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	body := decode(t, recorder)
	if body["exists"] != false || body["locked"] != false {
		t.Fatalf("body = %s", recorder.Body.String())
	}
	if _, ok := body["waitingOwners"]; !ok {
		t.Fatalf("waitingOwners missing: %s", recorder.Body.String())
	}

	holder := doJSON(t, router, http.MethodGet, "/v2/locks/holders/ghost", nil)
	holderBody := decode(t, holder)
	if holderBody["holds"] == nil || len(holderBody["holds"].([]any)) != 0 {
		t.Fatalf("holder body = %s", holder.Body.String())
	}
}
