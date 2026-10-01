package api

import (
	"net/http"
	"testing"
)

func TestCancelWaitingValidationErrors(t *testing.T) {
	router := newRouter(t)

	cases := []struct {
		name string
		body map[string]any
		code string
	}{
		{"missing resource", map[string]any{"ownerId": "node-b"}, "MISSING_RESOURCE_ID"},
		{"missing owner", map[string]any{"resourceId": "doc-1"}, "MISSING_OWNER_ID"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := doJSON(t, router, http.MethodPost, "/locks/cancel-waiting", tc.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d", recorder.Code)
			}
			if got := errorCode(recorder); got != tc.code {
				t.Fatalf("code = %q, want %q", got, tc.code)
			}
		})
	}
}

func TestCancelWaitingRemovesWaiterOverHTTP(t *testing.T) {
	router := newRouter(t)

	doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-a", "leaseSeconds": 60,
	})
	doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-b", "leaseSeconds": 45,
	})
	doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-c", "leaseSeconds": 30,
	})
	before := decode(t, doJSON(t, router, http.MethodGet, "/locks/doc-1", nil))

	recorder := doJSON(t, router, http.MethodPost, "/locks/cancel-waiting", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-b",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	body := decode(t, recorder)
	if body["status"] != "CANCELED" {
		t.Fatalf("status = %v", body["status"])
	}
	state := body["state"].(map[string]any)
	if state["resourceId"] != "doc-1" || state["ownerId"] != "node-a" {
		t.Fatalf("state = %v", state)
	}
	if state["leaseExpiresAt"] != before["leaseExpiresAt"] {
		t.Fatalf("lease changed: %v vs %v", state["leaseExpiresAt"], before["leaseExpiresAt"])
	}
	if state["reentryCount"].(float64) != 1 {
		t.Fatalf("reentryCount = %v", state["reentryCount"])
	}
	queue := state["waitingOwners"].([]any)
	if len(queue) != 1 {
		t.Fatalf("queue = %v", queue)
	}
	remaining := queue[0].(map[string]any)
	if remaining["ownerId"] != "node-c" || remaining["requestedLeaseSeconds"].(float64) != 30 {
		t.Fatalf("remaining waiter = %v", remaining)
	}

	// Repeated cancel: 409 NOT_WAITING, holder and other waiters untouched.
	again := doJSON(t, router, http.MethodPost, "/locks/cancel-waiting", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-b",
	})
	if again.Code != http.StatusConflict || errorCode(again) != "NOT_WAITING" {
		t.Fatalf("repeat = %d %s", again.Code, again.Body.String())
	}
	view := decode(t, doJSON(t, router, http.MethodGet, "/locks/doc-1", nil))
	if view["ownerId"] != "node-a" || len(view["waitingOwners"].([]any)) != 1 {
		t.Fatalf("repeat cancel changed state: %v", view)
	}
}

func TestCancelWaitingNotFoundAndNotWaiting(t *testing.T) {
	router := newRouter(t)

	missing := doJSON(t, router, http.MethodPost, "/locks/cancel-waiting", map[string]any{
		"resourceId": "nope", "ownerId": "node-b",
	})
	if missing.Code != http.StatusNotFound || errorCode(missing) != "RESOURCE_NOT_FOUND" {
		t.Fatalf("missing = %d %s", missing.Code, missing.Body.String())
	}

	doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-a", "leaseSeconds": 60,
	})
	holder := doJSON(t, router, http.MethodPost, "/locks/cancel-waiting", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-a",
	})
	if holder.Code != http.StatusConflict || errorCode(holder) != "NOT_WAITING" {
		t.Fatalf("holder cancel = %d %s", holder.Code, holder.Body.String())
	}
}

func TestCancelThenReacquireOverHTTP(t *testing.T) {
	router := newRouter(t)

	doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-a", "leaseSeconds": 60,
	})
	doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-b", "leaseSeconds": 45,
	})
	doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-c", "leaseSeconds": 30,
	})
	doJSON(t, router, http.MethodPost, "/locks/cancel-waiting", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-b",
	})

	// Still held by someone else: node-b rejoins at the tail of the queue.
	rejoin := doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "doc-1", "ownerId": "node-b", "leaseSeconds": 50,
	})
	if decode(t, rejoin)["status"] != "WAITING" {
		t.Fatalf("rejoin = %s", rejoin.Body.String())
	}
	queue := decode(t, rejoin)["state"].(map[string]any)["waitingOwners"].([]any)
	if len(queue) != 2 || queue[1].(map[string]any)["ownerId"] != "node-b" {
		t.Fatalf("queue = %v", queue)
	}

	// Holderless after release: node-b acquires directly.
	doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "doc-2", "ownerId": "node-a", "leaseSeconds": 60,
	})
	doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "doc-2", "ownerId": "node-b", "leaseSeconds": 45,
	})
	doJSON(t, router, http.MethodPost, "/locks/cancel-waiting", map[string]any{
		"resourceId": "doc-2", "ownerId": "node-b",
	})
	doJSON(t, router, http.MethodPost, "/locks/release", map[string]any{
		"resourceId": "doc-2", "ownerId": "node-a",
	})
	direct := doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "doc-2", "ownerId": "node-b", "leaseSeconds": 45,
	})
	directBody := decode(t, direct)
	if directBody["status"] != "ACQUIRED" {
		t.Fatalf("direct = %s", direct.Body.String())
	}
	if directBody["state"].(map[string]any)["ownerId"] != "node-b" {
		t.Fatalf("owner = %v", directBody["state"])
	}
}
