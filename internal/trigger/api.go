package trigger

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"go-sre-harness/internal/auth"
)

// MaxBody bounds a webhook delivery.
const MaxBody = 1 << 20

// API exposes the trigger routes:
//
//	POST /v1/triggers/{name}   deliver an event (operator)
//	GET  /v1/triggers          list configured triggers (viewer)
type API struct {
	Service *Service
}

func (a *API) RouteRoles() map[string]auth.Role {
	return map[string]auth.Role{"POST /v1/triggers/{name}": auth.Operator, "GET /v1/triggers": auth.Viewer}
}

func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/triggers", func(w http.ResponseWriter, r *http.Request) {
		p, ok := auth.FromContext(r.Context())
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthenticated"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"triggers": a.Service.List(p.Tenant)})
	})
	mux.HandleFunc("POST /v1/triggers/{name}", a.deliver)
}

func (a *API) deliver(w http.ResponseWriter, r *http.Request) {
	p, ok := auth.FromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthenticated"})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBody))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "payload_too_large"})
		return
	}
	res, err := a.Service.Handle(r.Context(), p.Tenant, p.Subject, r.PathValue("name"), body, r.Header)
	switch {
	case errors.Is(err, ErrUnknownTrigger):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown_trigger"})
		return
	case errors.Is(err, ErrSignature):
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid_signature"})
		return
	case errors.Is(err, ErrPayload):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_payload", "message": err.Error()})
		return
	case errors.Is(err, ErrMisconfigured):
		a.Service.Log.Error("trigger misconfigured", "trigger", r.PathValue("name"), "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "trigger_misconfigured"})
		return
	case err != nil:
		a.Service.Log.Error("trigger", "trigger", r.PathValue("name"), "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	code := http.StatusAccepted
	failed, limited := 0, 0
	for _, it := range res.Items {
		switch it.Action {
		case ActionError:
			failed++
		case ActionRateLimited:
			limited++
		}
	}
	switch {
	case len(res.Items) > 0 && failed == len(res.Items):
		// Every event failed: ask the sender to redeliver.
		code = http.StatusServiceUnavailable
	case len(res.Items) > 0 && limited+failed == len(res.Items) && limited > 0:
		code = http.StatusTooManyRequests
	}
	writeJSON(w, code, res)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
