package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/luwa07832/distributed-lock-service/internal/store"
)

func newRouter(t *testing.T) http.Handler {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewRouter(st)
}

func doJSON(t *testing.T, router http.Handler, method, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, reader)
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	return recorder
}

func decode(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode %q: %v", recorder.Body.String(), err)
	}
	return got
}

func errorCode(recorder *httptest.ResponseRecorder) string {
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		return ""
	}
	return envelope.Error.Code
}

func TestAcquireValidationErrors(t *testing.T) {
	router := newRouter(t)

	cases := []struct {
		name string
		body map[string]any
		code string
	}{
		{"missing resource", map[string]any{"ownerId": "a", "leaseSeconds": 5}, "MISSING_RESOURCE_ID"},
		{"missing owner", map[string]any{"resourceId": "r", "leaseSeconds": 5}, "MISSING_OWNER_ID"},
		{"zero lease", map[string]any{"resourceId": "r", "ownerId": "a", "leaseSeconds": 0}, "INVALID_LEASE_SECONDS"},
		{"negative lease", map[string]any{"resourceId": "r", "ownerId": "a", "leaseSeconds": -3}, "INVALID_LEASE_SECONDS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := doJSON(t, router, http.MethodPost, "/locks", tc.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d", recorder.Code)
			}
			if got := errorCode(recorder); got != tc.code {
				t.Fatalf("code = %q, want %q", got, tc.code)
			}
		})
	}
}

func TestAcquireQueueDuplicateAndState(t *testing.T) {
	router := newRouter(t)

	first := doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "r", "ownerId": "a", "leaseSeconds": 60,
	})
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d body = %s", first.Code, first.Body.String())
	}
	firstBody := decode(t, first)
	if firstBody["status"] != "ACQUIRED" {
		t.Fatalf("status = %v", firstBody["status"])
	}
	state := firstBody["state"].(map[string]any)
	if state["ownerId"] != "a" || state["reentryCount"].(float64) != 1 {
		t.Fatalf("state = %v", state)
	}
	if state["waitingOwners"].([]any) == nil {
		t.Fatalf("waitingOwners must be present")
	}

	waiting := doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "r", "ownerId": "b", "leaseSeconds": 30,
	})
	if waiting.Code != http.StatusOK || decode(t, waiting)["status"] != "WAITING" {
		t.Fatalf("waiting = %d %s", waiting.Code, waiting.Body.String())
	}

	dup := doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "r", "ownerId": "b", "leaseSeconds": 30,
	})
	if dup.Code != http.StatusConflict || errorCode(dup) != "DUPLICATE_WAITING" {
		t.Fatalf("dup = %d %s", dup.Code, dup.Body.String())
	}

	got := doJSON(t, router, http.MethodGet, "/locks/r", nil)
	if got.Code != http.StatusOK {
		t.Fatalf("state status = %d", got.Code)
	}
	view := decode(t, got)
	if view["resourceId"] != "r" || view["ownerId"] != "a" {
		t.Fatalf("view = %v", view)
	}
	queue := view["waitingOwners"].([]any)
	if len(queue) != 1 {
		t.Fatalf("queue len = %d", len(queue))
	}
	firstWaiting := queue[0].(map[string]any)
	if firstWaiting["ownerId"] != "b" || firstWaiting["requestedLeaseSeconds"].(float64) != 30 {
		t.Fatalf("waiting = %v", firstWaiting)
	}

	missing := doJSON(t, router, http.MethodGet, "/locks/unknown", nil)
	if missing.Code != http.StatusNotFound || errorCode(missing) != "RESOURCE_NOT_FOUND" {
		t.Fatalf("missing = %d %s", missing.Code, missing.Body.String())
	}
}

func TestReentryDoesNotChangeLeaseOverHTTP(t *testing.T) {
	router := newRouter(t)

	first := doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "r", "ownerId": "a", "leaseSeconds": 60,
	})
	lease := decode(t, first)["state"].(map[string]any)["leaseExpiresAt"]

	again := doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "r", "ownerId": "a", "leaseSeconds": 600,
	})
	againState := decode(t, again)["state"].(map[string]any)
	if againState["leaseExpiresAt"] != lease {
		t.Fatalf("lease changed: %v vs %v", againState["leaseExpiresAt"], lease)
	}
	if againState["reentryCount"].(float64) != 2 {
		t.Fatalf("count = %v", againState["reentryCount"])
	}

	reenter := doJSON(t, router, http.MethodPost, "/locks/reenter", map[string]any{
		"resourceId": "r", "ownerId": "a",
	})
	if decode(t, reenter)["state"].(map[string]any)["leaseExpiresAt"] != lease {
		t.Fatalf("reenter extended lease")
	}

	notOwner := doJSON(t, router, http.MethodPost, "/locks/reenter", map[string]any{
		"resourceId": "r", "ownerId": "z",
	})
	if notOwner.Code != http.StatusForbidden || errorCode(notOwner) != "NOT_OWNER" {
		t.Fatalf("notOwner = %d %s", notOwner.Code, notOwner.Body.String())
	}
}

func TestLeaseExpiresAndTransfersEndToEnd(t *testing.T) {
	router := newRouter(t)

	doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "r", "ownerId": "a", "leaseSeconds": 1,
	})
	doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "r", "ownerId": "b", "leaseSeconds": 90,
	})

	time.Sleep(1150 * time.Millisecond)

	view := decode(t, doJSON(t, router, http.MethodGet, "/locks/r", nil))
	if view["ownerId"] != "b" {
		t.Fatalf("owner = %v, want b (after background transfer)", view["ownerId"])
	}
	if view["reentryCount"].(float64) != 1 {
		t.Fatalf("count = %v", view["reentryCount"])
	}

	late := doJSON(t, router, http.MethodPost, "/locks/release", map[string]any{
		"resourceId": "r", "ownerId": "a",
	})
	if late.Code != http.StatusConflict || errorCode(late) != "LEASE_EXPIRED" {
		t.Fatalf("late release = %d %s", late.Code, late.Body.String())
	}
	view = decode(t, doJSON(t, router, http.MethodGet, "/locks/r", nil))
	if view["ownerId"] != "b" {
		t.Fatalf("late release overwrote holder: %v", view["ownerId"])
	}
}

func TestQueryDoesNotExtendOrTransfer(t *testing.T) {
	router := newRouter(t)

	acquired := decode(t, doJSON(t, router, http.MethodPost, "/locks", map[string]any{
		"resourceId": "r", "ownerId": "a", "leaseSeconds": 10,
	}))
	fixedLease := acquired["state"].(map[string]any)["leaseExpiresAt"]

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		view := decode(t, doJSON(t, router, http.MethodGet, "/locks/r", nil))
		if view["leaseExpiresAt"] != fixedLease {
			t.Fatalf("query changed the lease: %v vs %v", view["leaseExpiresAt"], fixedLease)
		}
		if view["ownerId"] != "a" || view["reentryCount"].(float64) != 1 {
			t.Fatalf("query changed the holder state: %v", view)
		}
		time.Sleep(30 * time.Millisecond)
	}
}
