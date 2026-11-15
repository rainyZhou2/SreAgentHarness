package securityapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/security/revocation"
)

var testTime = time.Date(2026, 11, 15, 2, 0, 0, 0, time.UTC)

type fakeRevoker struct {
	event revocation.Event
	err   error
}

func (f *fakeRevoker) Revoke(_ context.Context, event revocation.Event) (revocation.Event, error) {
	f.event = event
	event.ID = "018f0000-0000-7000-8000-000000000001"
	event.Version = 1
	return event, f.err
}

func testHandler(t *testing.T, extension api.Extension) http.Handler {
	t.Helper()
	server := &api.Server{}
	server.Extend(extension)
	return server.Handler()
}

func performRequest(handler http.Handler, actor, role, body string, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/api/admin/security/revocations", strings.NewReader(body))
	request.Header.Set("X-Tenant-ID", "tenant-root")
	request.Header.Set("X-Actor-ID", actor)
	request.Header.Set("X-Roles", role)
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestRevocationAPI(t *testing.T) {
	revoker := &fakeRevoker{}
	handler := testHandler(t, &API{Revocations: revoker, Clock: func() time.Time { return testTime }})
	response := performRequest(handler, "root", "admin", `{"scope":"work","scope_id":"w-1","reason":"active incident"}`, map[string]string{"Idempotency-Key": "rev-1"})
	if response.Code != http.StatusOK || revoker.event.TenantID != "tenant-root" || revoker.event.ActorID != "root" || revoker.event.ScopeID != "w-1" {
		t.Fatalf("%d %s %+v", response.Code, response.Body.String(), revoker.event)
	}
	response = performRequest(handler, "alice", "operator", `{"scope":"tenant","reason":"stop"}`, map[string]string{"Idempotency-Key": "rev-2"})
	if response.Code != http.StatusForbidden {
		t.Fatalf("operator must be forbidden: %d", response.Code)
	}
	response = performRequest(handler, "root", "admin", `{"scope":"work","scope_id":"w-1","reason":"x"}`, nil)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("missing idempotency key: %d", response.Code)
	}
	response = performRequest(handler, "root", "admin", `{"scope":"work","scope_id":"w-1","reason":"x","extra":true}`, map[string]string{"Idempotency-Key": "rev-1"})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown field: %d", response.Code)
	}
}

func TestRevocationAPIErrorMapping(t *testing.T) {
	for _, test := range []struct {
		err  error
		code int
	}{
		{revocation.ErrRevocationConflict, http.StatusConflict},
		{revocation.ErrInvalidRevocation, http.StatusUnprocessableEntity},
		{revocation.ErrRevocationUnavailable, http.StatusServiceUnavailable},
		{errors.New("database unavailable"), http.StatusServiceUnavailable},
	} {
		revoker := &fakeRevoker{err: test.err}
		handler := testHandler(t, &API{Revocations: revoker, Clock: func() time.Time { return testTime }})
		response := performRequest(handler, "root", "admin", `{"scope":"work","scope_id":"w-1","reason":"incident"}`, map[string]string{"Idempotency-Key": "rev-1"})
		if response.Code != test.code {
			t.Fatalf("error=%v status=%d body=%s", test.err, response.Code, response.Body.String())
		}
	}
}
