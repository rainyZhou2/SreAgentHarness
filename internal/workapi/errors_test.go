package workapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-sre-harness/internal/impact"
	"go-sre-harness/internal/observability"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/quota"
	"go-sre-harness/internal/runbook"
)

func TestUnifiedDomainErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		status     int
		kind       string
		retryAfter string
	}{
		{name: "overloaded", err: orchestration.ErrOverloaded, status: http.StatusTooManyRequests, kind: "overloaded", retryAfter: "30"},
		{name: "observability missing", err: observability.ErrNotFound, status: http.StatusNotFound, kind: "not_found"},
		{name: "runbook version", err: runbook.ErrVersionConflict, status: http.StatusPreconditionFailed, kind: "version_mismatch"},
		{name: "impact invalid", err: impact.ErrInvalid, status: http.StatusBadRequest, kind: "invalid_request"},
		{name: "quota invalid", err: quota.ErrInvalid, status: http.StatusBadRequest, kind: "invalid_request"},
	}
	a := &API{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/api/test", nil)
			r.Header.Set("If-Match", `"1"`)
			w := httptest.NewRecorder()
			a.fail(w, r, tc.err)
			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d; body = %s", w.Code, tc.status, w.Body.String())
			}
			if got := w.Header().Get("Retry-After"); got != tc.retryAfter {
				t.Fatalf("Retry-After = %q, want %q", got, tc.retryAfter)
			}
			var body struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Error != tc.kind {
				t.Fatalf("error kind = %q, want %q", body.Error, tc.kind)
			}
		})
	}
}
