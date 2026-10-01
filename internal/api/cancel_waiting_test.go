package api

import (
	"net/http"
	"testing"
)

func queueDoc1(router http.Handler, t *testing.T) {
	t.Helper()
	doJSON(t, router, http.MethodPost, "/locks", map[string]any{"resourceId": "doc-1", "ownerId": "node-a", "leaseSeconds": 30})
	doJSON(t, router, http.MethodPost, "/locks", map[string]any{"resourceId": "doc-1", "ownerId": "node-b", "leaseSeconds": 45})
	doJSON(t, router, http.MethodPost, "/locks", map[string]any{"resourceId": "doc-1", "ownerId": "node-c", "leaseSeconds": 60})
}

func TestCancelWaitingSuccess(t *testing.T) {
	router := newRouter(t)
	queueDoc1(router, t)

	recorder := doJSON(t, router, http.MethodPost, "/locks/cancel-waiting",
		map[string]any{"resourceId": "doc-1", "ownerId": "node-b"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	body := decode(t, recorder)
	if body["status"] != "CANCELED" {
		t.Fatalf("status = %v, want CANCELED", body["status"])
	}
	state, ok := body["state"].(map[string]any)
	if !ok {
		t.Fatalf("state missing: %v", body)
	}
	if state["resourceId"] != "doc-1" || state["ownerId"] != "node-a" {
		t.Fatalf("holder view changed: %v", state)
	}
	if state["reentryCount"].(float64) != 1 {
		t.Fatalf("reentryCount = %v, want 1", state["reentryCount"])
	}
	if state["leaseExpiresAt"] == nil {
		t.Fatalf("leaseExpiresAt missing: %v", state)
	}
	waiting, ok := state["waitingOwners"].([]any)
	if !ok || len(waiting) != 1 {
		t.Fatalf("waitingOwners = %v, want one entry", state["waitingOwners"])
	}
	only := waiting[0].(map[string]any)
	if only["ownerId"] != "node-c" {
		t.Fatalf("queue order = %v", waiting)
	}
	if only["requestedLeaseSeconds"].(float64) != 60 {
		t.Fatalf("c requested seconds changed: %v", only["requestedLeaseSeconds"])
	}
}

func TestCancelWaitingValidationAndErrors(t *testing.T) {
	router := newRouter(t)
	queueDoc1(router, t)

	cases := []struct {
		name       string
		body       map[string]any
		wantStatus int
		wantCode   string
	}{
		{"missing resource", map[string]any{"ownerId": "node-b"}, http.StatusBadRequest, "MISSING_RESOURCE_ID"},
		{"missing owner", map[string]any{"resourceId": "doc-1"}, http.StatusBadRequest, "MISSING_OWNER_ID"},
		{"unknown resource", map[string]any{"resourceId": "nope", "ownerId": "node-b"}, http.StatusNotFound, "RESOURCE_NOT_FOUND"},
		{"not waiting", map[string]any{"resourceId": "doc-1", "ownerId": "node-a"}, http.StatusConflict, "NOT_WAITING"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := doJSON(t, router, http.MethodPost, "/locks/cancel-waiting", tc.body)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			if got := errorCode(recorder); got != tc.wantCode {
				t.Fatalf("code = %q, want %q", got, tc.wantCode)
			}
		})
	}

	// Repeating a successful cancel must not change anything.
	first := doJSON(t, router, http.MethodPost, "/locks/cancel-waiting",
		map[string]any{"resourceId": "doc-1", "ownerId": "node-b"})
	if first.Code != http.StatusOK {
		t.Fatalf("first cancel = %d %s", first.Code, first.Body.String())
	}
	second := doJSON(t, router, http.MethodPost, "/locks/cancel-waiting",
		map[string]any{"resourceId": "doc-1", "ownerId": "node-b"})
	if second.Code != http.StatusConflict || errorCode(second) != "NOT_WAITING" {
		t.Fatalf("repeat cancel = %d %s", second.Code, second.Body.String())
	}
	stateResp := doJSON(t, router, http.MethodGet, "/locks/doc-1", nil)
	state := decode(t, stateResp)
	if state["ownerId"] != "node-a" {
		t.Fatalf("holder changed after repeat cancel: %v", state["ownerId"])
	}
	waiting := state["waitingOwners"].([]any)
	if len(waiting) != 1 || waiting[0].(map[string]any)["ownerId"] != "node-c" {
		t.Fatalf("queue after repeat cancel = %v", waiting)
	}
}

func TestCancelWaitingThenReacquireAtTail(t *testing.T) {
	router := newRouter(t)
	queueDoc1(router, t)

	doJSON(t, router, http.MethodPost, "/locks/cancel-waiting",
		map[string]any{"resourceId": "doc-1", "ownerId": "node-b"})

	recorder := doJSON(t, router, http.MethodPost, "/locks",
		map[string]any{"resourceId": "doc-1", "ownerId": "node-b", "leaseSeconds": 90})
	body := decode(t, recorder)
	if body["status"] != "WAITING" {
		t.Fatalf("status = %v, want WAITING", body["status"])
	}
	waiting := body["state"].(map[string]any)["waitingOwners"].([]any)
	if len(waiting) != 2 || waiting[1].(map[string]any)["ownerId"] != "node-b" ||
		waiting[1].(map[string]any)["requestedLeaseSeconds"].(float64) != 90 {
		t.Fatalf("node-b not requeued at tail with new seconds: %v", waiting)
	}
}
