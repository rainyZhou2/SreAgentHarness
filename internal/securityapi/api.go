package securityapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/security/revocation"
)

type Revoker interface {
	Revoke(ctx context.Context, event revocation.Event) (revocation.Event, error)
}

type API struct {
	Revocations Revoker
	Clock       func() time.Time
}

func (a *API) RouteRoles() map[string]auth.Role {
	return map[string]auth.Role{"POST /api/admin/security/revocations": auth.Admin}
}

func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/admin/security/revocations", a.revoke)
}

func (a *API) revoke(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.FromContext(r.Context())
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthenticated", "authentication required")
		return
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 200 {
		writeErr(w, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key is required")
		return
	}
	var body struct {
		Scope   string `json:"scope"`
		ScopeID string `json:"scope_id"`
		Reason  string `json:"reason"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_body", "invalid revocation request")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "invalid_body", "request must contain one JSON object")
		return
	}
	if a.Revocations == nil {
		writeErr(w, http.StatusServiceUnavailable, "security_policy_unavailable", "revocation service is unavailable")
		return
	}
	now := time.Now
	if a.Clock != nil {
		now = a.Clock
	}
	event, err := a.Revocations.Revoke(r.Context(), revocation.Event{
		TenantID: principal.Tenant, Scope: body.Scope, ScopeID: body.ScopeID, IdempotencyKey: key,
		ActorID: principal.Subject, Reason: body.Reason, CreatedAt: now().UTC(),
	})
	switch {
	case errors.Is(err, revocation.ErrRevocationConflict):
		writeErr(w, http.StatusConflict, "revocation_conflict", "idempotency key belongs to a different revocation")
	case errors.Is(err, revocation.ErrInvalidRevocation):
		writeErr(w, http.StatusUnprocessableEntity, "revocation_rejected", "revocation request is invalid")
	case err != nil:
		writeErr(w, http.StatusServiceUnavailable, "security_policy_unavailable", "revocation store is unavailable")
	default:
		writeJSON(w, http.StatusOK, event)
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeErr(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"error": code, "message": message})
}
