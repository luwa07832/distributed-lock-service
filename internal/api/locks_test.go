package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/distributed-lock-service/internal/store"
)

type lockClient struct {
	t      *testing.T
	engine *gin.Engine
}

func newLockClient(t *testing.T) *lockClient {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return &lockClient{t: t, engine: NewRouter(st)}
}

type stateResponse struct {
	ResourceID     string  `json:"resourceId"`
	OwnerID        *string `json:"ownerId"`
	LeaseExpiresAt *string `json:"leaseExpiresAt"`
	ReentryCount   int64   `json:"reentryCount"`
	WaitingOwners  []struct {
		OwnerID               string `json:"ownerId"`
		RequestedLeaseSeconds int64  `json:"requestedLeaseSeconds"`
	} `json:"waitingOwners"`
}

type httpActionResponse struct {
	Status string `json:"status"`
	stateResponse
}

func owner(value string) *string { return &value }

func (c *lockClient) call(method, path, body string) *httptest.ResponseRecorder {
	c.t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	c.engine.ServeHTTP(recorder, request)
	return recorder
}

func decodeAction(t *testing.T, recorder *httptest.ResponseRecorder) httpActionResponse {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var response httpActionResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v body=%s", err, recorder.Body.String())
	}
	return response
}

func decodeState(t *testing.T, recorder *httptest.ResponseRecorder) stateResponse {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var response stateResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v body=%s", err, recorder.Body.String())
	}
	return response
}

func assertErrorCode(t *testing.T, recorder *httptest.ResponseRecorder, want string) {
	t.Helper()
	if recorder.Code == http.StatusOK {
		t.Fatalf("expected error response, got 200: %s", recorder.Body.String())
	}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Error.Code != want {
		t.Fatalf("error code = %q, want %q", body.Error.Code, want)
	}
	if body.Error.Message == "" {
		t.Fatalf("error message empty")
	}
}

func TestLifecycleOverHTTP(t *testing.T) {
	client := newLockClient(t)

	recorder := client.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"alice","leaseSeconds":5}`)
	var acquired httpActionResponse
	acquired = decodeAction(t, recorder)
	if acquired.Status != "ACQUIRED" || acquired.OwnerID == nil || *acquired.OwnerID != "alice" ||
		acquired.ReentryCount != 1 || acquired.LeaseExpiresAt == nil {
		t.Fatalf("acquire = %+v", acquired)
	}
	firstExpiry := *acquired.LeaseExpiresAt
	if len(acquired.WaitingOwners) != 0 {
		t.Fatalf("waitingOwners = %v", acquired.WaitingOwners)
	}

	recorder = client.call(http.MethodPost, "/locks/doc/reenter", `{"ownerId":"alice"}`)
	var reentered httpActionResponse
	reentered = decodeAction(t, recorder)
	if reentered.Status != "REENTERED" || reentered.ReentryCount != 2 ||
		*reentered.LeaseExpiresAt != firstExpiry {
		t.Fatalf("reenter = %+v", reentered)
	}

	recorder = client.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"bob","leaseSeconds":3}`)
	var bob httpActionResponse
	bob = decodeAction(t, recorder)
	if bob.Status != "WAITING" || *bob.OwnerID != "alice" || len(bob.WaitingOwners) != 1 ||
		bob.WaitingOwners[0].OwnerID != "bob" || bob.WaitingOwners[0].RequestedLeaseSeconds != 3 {
		t.Fatalf("bob waiting = %+v", bob)
	}

	recorder = client.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"carol","leaseSeconds":4}`)
	var carol httpActionResponse
	carol = decodeAction(t, recorder)
	if carol.Status != "WAITING" || len(carol.WaitingOwners) != 2 ||
		carol.WaitingOwners[1].OwnerID != "carol" {
		t.Fatalf("carol waiting = %+v", carol)
	}

	recorder = client.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"bob","leaseSeconds":3}`)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("duplicate waiting status = %d", recorder.Code)
	}
	assertErrorCode(t, recorder, "DUPLICATE_WAITING")

	recorder = client.call(http.MethodGet, "/locks/doc", "")
	queried := decodeState(t, recorder)
	if *queried.OwnerID != "alice" || queried.ReentryCount != 2 || len(queried.WaitingOwners) != 2 {
		t.Fatalf("query = %+v", queried)
	}

	recorder = client.call(http.MethodPost, "/locks/doc/release", `{"ownerId":"alice"}`)
	var firstRelease httpActionResponse
	firstRelease = decodeAction(t, recorder)
	if firstRelease.Status != "RELEASED" || *firstRelease.OwnerID != "alice" ||
		firstRelease.ReentryCount != 1 {
		t.Fatalf("first release = %+v", firstRelease)
	}

	recorder = client.call(http.MethodPost, "/locks/doc/release", `{"ownerId":"alice"}`)
	var secondRelease httpActionResponse
	secondRelease = decodeAction(t, recorder)
	if secondRelease.Status != "RELEASED" || *secondRelease.OwnerID != "bob" ||
		secondRelease.ReentryCount != 1 || len(secondRelease.WaitingOwners) != 1 ||
		secondRelease.WaitingOwners[0].OwnerID != "carol" {
		t.Fatalf("second release = %+v", secondRelease)
	}
}

func TestHTTPValidationAndOwnershipErrors(t *testing.T) {
	client := newLockClient(t)

	recorder := client.call(http.MethodPost, "/locks/%20/acquire", `{"ownerId":"alice","leaseSeconds":5}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("blank resource status = %d", recorder.Code)
	}
	assertErrorCode(t, recorder, "MISSING_RESOURCE_ID")

	recorder = client.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"","leaseSeconds":5}`)
	assertErrorCode(t, recorder, "MISSING_OWNER_ID")

	recorder = client.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"alice","leaseSeconds":0}`)
	assertErrorCode(t, recorder, "INVALID_LEASE_SECONDS")

	recorder = client.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"alice","leaseSeconds":-2}`)
	assertErrorCode(t, recorder, "INVALID_LEASE_SECONDS")

	recorder = client.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"alice"}`)
	assertErrorCode(t, recorder, "INVALID_LEASE_SECONDS")

	recorder = client.call(http.MethodPost, "/locks/doc/acquire", `{bad json`)
	assertErrorCode(t, recorder, "INVALID_REQUEST_BODY")

	client.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"alice","leaseSeconds":5}`)

	recorder = client.call(http.MethodPost, "/locks/doc/release", `{"ownerId":"bob"}`)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("non-owner release status = %d", recorder.Code)
	}
	assertErrorCode(t, recorder, "NOT_OWNER")

	recorder = client.call(http.MethodPost, "/locks/doc/reenter", `{"ownerId":"bob"}`)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("non-owner reenter status = %d", recorder.Code)
	}
	assertErrorCode(t, recorder, "NOT_OWNER")
}

func TestHTTPResourceQuerySemantics(t *testing.T) {
	client := newLockClient(t)

	recorder := client.call(http.MethodGet, "/locks/never-existed", "")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("missing resource status = %d", recorder.Code)
	}
	assertErrorCode(t, recorder, "RESOURCE_NOT_FOUND")

	client.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"alice","leaseSeconds":5}`)
	client.call(http.MethodPost, "/locks/doc/release", `{"ownerId":"alice"}`)

	recorder = client.call(http.MethodGet, "/locks/doc", "")
	state := decodeState(t, recorder)
	if state.OwnerID != nil {
		t.Fatalf("ownerId = %v, want null", state.OwnerID)
	}
	if state.LeaseExpiresAt != nil {
		t.Fatalf("leaseExpiresAt = %v, want null", state.LeaseExpiresAt)
	}
	if state.ReentryCount != 0 || state.WaitingOwners == nil || len(state.WaitingOwners) != 0 {
		t.Fatalf("unlocked resource view = %+v", state)
	}
}

type controllableStoreClient struct {
	engine *gin.Engine
	now    time.Time
}

func newControllableClient(t *testing.T) *controllableStoreClient {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	client := &controllableStoreClient{now: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)}
	st.SetClockForTest(func() time.Time { return client.now })
	client.engine = NewRouter(st)
	return client
}

func (c *controllableStoreClient) call(method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	c.engine.ServeHTTP(recorder, request)
	return recorder
}

func TestHTTPQueryIsReadOnlyAcrossExpiry(t *testing.T) {
	c := newControllableClient(t)

	c.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"alice","leaseSeconds":1}`)
	bobRecorder := c.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"bob","leaseSeconds":3}`)
	bob := decodeAction(t, bobRecorder)
	if bob.Status != "WAITING" {
		t.Fatalf("bob should queue: %+v", bob)
	}

	// Move past the lease; only queries happen until the next mutating call.
	c.now = c.now.Add(2 * time.Second)

	recorder := c.call(http.MethodGet, "/locks/doc", "")
	state := decodeState(t, recorder)
	if state.OwnerID == nil || *state.OwnerID != "alice" {
		t.Fatalf("query changed expired state: %+v", state)
	}

	// A competing acquire drives the deterministic transition: bob, queued
	// earlier with his own lease, becomes holder and dave queues behind him.
	recorder = c.call(http.MethodPost, "/locks/doc/acquire", `{"ownerId":"dave","leaseSeconds":7}`)
	dave := decodeAction(t, recorder)
	if dave.Status != "WAITING" || dave.OwnerID == nil || *dave.OwnerID != "bob" {
		t.Fatalf("post-expiry acquire = %+v", dave)
	}
	if len(dave.WaitingOwners) != 1 || dave.WaitingOwners[0].OwnerID != "dave" {
		t.Fatalf("post-expiry queue = %+v", dave.WaitingOwners)
	}

	// Late operations from alice are rejected and cannot override bob.
	recorder = c.call(http.MethodPost, "/locks/doc/release", `{"ownerId":"alice"}`)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("late release status = %d", recorder.Code)
	}
	assertErrorCode(t, recorder, "LEASE_EXPIRED")

	recorder = c.call(http.MethodPost, "/locks/doc/reenter", `{"ownerId":"alice"}`)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("late reenter status = %d", recorder.Code)
	}
	assertErrorCode(t, recorder, "LEASE_EXPIRED")
}
