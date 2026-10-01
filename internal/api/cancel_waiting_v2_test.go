package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func doRawJSON(t *testing.T, router http.Handler, method, target, raw string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, bytes.NewReader([]byte(raw)))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestV2CancelWaitingRemovesWaiter(t *testing.T) {
	router, _ := newLockStateRouter(t)

	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "doc-1", "ownerId": "a", "leaseSeconds": 30,
	})
	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "doc-1", "ownerId": "b", "leaseSeconds": 45,
	})
	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "doc-1", "ownerId": "c", "leaseSeconds": 60,
	})

	recorder := doJSON(t, router, http.MethodPost, "/v2/locks/cancel-waiting", map[string]any{
		"resourceId": "doc-1", "ownerId": "b",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	body := decode(t, recorder)
	if body["status"] != "CANCELED" {
		t.Fatalf("status = %v", body["status"])
	}
	state := body["state"].(map[string]any)
	if state["resourceId"] != "doc-1" || state["ownerId"] != "a" || state["exists"] != true || state["locked"] != true {
		t.Fatalf("state = %v", state)
	}
	if state["leaseExpiresAt"] != "2026-01-01T12:00:30.000Z" {
		t.Fatalf("leaseExpiresAt = %v", state["leaseExpiresAt"])
	}
	if state["reentryCount"].(float64) != 1 {
		t.Fatalf("reentryCount = %v", state["reentryCount"])
	}
	waiting := state["waitingOwners"].([]any)
	if len(waiting) != 1 {
		t.Fatalf("waiting = %v", waiting)
	}
	remaining := waiting[0].(map[string]any)
	if remaining["ownerId"] != "c" || remaining["requestedLeaseSeconds"].(float64) != 60 {
		t.Fatalf("remaining waiter = %v", remaining)
	}

	view := decode(t, doJSON(t, router, http.MethodGet, "/v2/locks/resources/doc-1", nil))
	if got := len(view["waitingOwners"].([]any)); got != 1 {
		t.Fatalf("GET view waiting len = %d", got)
	}
}

func TestV2CancelWaitingErrors(t *testing.T) {
	router, _ := newLockStateRouter(t)

	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "doc-1", "ownerId": "a", "leaseSeconds": 30,
	})

	cases := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"invalid json", `{not json`, http.StatusBadRequest, "INVALID_REQUEST"},
		{"empty resource", `{"resourceId":"","ownerId":"b"}`, http.StatusBadRequest, "INVALID_LOCK_IDENTITY"},
		{"empty owner", `{"resourceId":"doc-1","ownerId":""}`, http.StatusBadRequest, "INVALID_HOLDER_IDENTITY"},
		{"unknown resource", `{"resourceId":"nope","ownerId":"b"}`, http.StatusNotFound, "RESOURCE_NOT_FOUND"},
		{"not waiting", `{"resourceId":"doc-1","ownerId":"a"}`, http.StatusConflict, "NOT_WAITING"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := doRawJSON(t, router, http.MethodPost, "/v2/locks/cancel-waiting", tc.body)
			if recorder.Code != tc.status {
				t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
			}
			if got := errorCode(recorder); got != tc.code {
				t.Fatalf("code = %q, want %q", got, tc.code)
			}
			if msg := decode(t, recorder)["error"].(map[string]any)["message"]; msg == "" {
				t.Fatalf("missing message: %s", recorder.Body.String())
			}
		})
	}

	// A failed cancel changes nothing.
	view := decode(t, doJSON(t, router, http.MethodGet, "/v2/locks/resources/doc-1", nil))
	if view["ownerId"] != "a" {
		t.Fatalf("state changed: %v", view)
	}
}

func TestV2CancelWaitingAfterPromotionKeepsNewHolder(t *testing.T) {
	router, clock := newLockStateRouter(t)

	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "doc-1", "ownerId": "a", "leaseSeconds": 30,
	})
	doJSON(t, router, http.MethodPost, "/v2/locks/acquire", map[string]any{
		"resourceId": "doc-1", "ownerId": "b", "leaseSeconds": 45,
	})
	clock.add(31 * time.Second)

	recorder := doJSON(t, router, http.MethodPost, "/v2/locks/cancel-waiting", map[string]any{
		"resourceId": "doc-1", "ownerId": "b",
	})
	if recorder.Code != http.StatusConflict || errorCode(recorder) != "NOT_WAITING" {
		t.Fatalf("response = %d %s", recorder.Code, recorder.Body.String())
	}
	view := decode(t, doJSON(t, router, http.MethodGet, "/v2/locks/resources/doc-1", nil))
	if view["ownerId"] != "b" {
		t.Fatalf("holder = %v, want b", view["ownerId"])
	}
}
