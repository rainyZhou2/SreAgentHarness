package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
)

// Auditor writes security_audit_log entries.
type Auditor interface {
	Audit(ctx context.Context, e pg.AuditEntry) error
}

// routeRoles maps "METHOD pattern" to the minimum role. GET routes default to
// viewer; every other route must be listed (default deny for writes).
var routeRoles = map[string]auth.Role{
	"POST /v1/runs":                           auth.Operator,
	"POST /v1/runs/{run_id}/commands":         auth.Viewer, // per-command role checked in the handler
	"POST /v1/runs/{run_id}/fork":             auth.Operator,
	"POST /v1/sessions":                       auth.Operator,
	"POST /v1/sessions/{session_id}/turns":    auth.Operator,
	"PATCH /v1/sessions/{session_id}":         auth.Operator,
	"POST /v1/sessions/{session_id}/branches": auth.Operator,
	"POST /v1/admin/role-bindings":            auth.Admin,
	"DELETE /v1/admin/role-bindings":          auth.Admin,
	"GET /v1/admin/audit":                     auth.Admin,
	"GET /v1/admin/plugins":                   auth.Admin,
	"POST /v1/graph/relations":                auth.Admin,
	"DELETE /v1/graph/relations":              auth.Admin,
}

var publicRoutes = map[string]bool{"GET /healthz": true, "GET /metrics": true}

// RequiredRole returns the role a core route needs ("" = public).
func RequiredRole(pattern string) (auth.Role, bool) { return (&Server{}).requiredRole(pattern) }

// requiredRole consults the core table first, then roles declared by
// extensions. Core routes cannot be redeclared by an extension.
func (s *Server) requiredRole(pattern string) (auth.Role, bool) {
	if publicRoutes[pattern] {
		return "", true
	}
	if r, ok := routeRoles[pattern]; ok {
		return r, true
	}
	if r, ok := s.extRoles[pattern]; ok && auth.ValidRole(r) {
		return r, true
	}
	if strings.HasPrefix(pattern, "GET ") {
		return auth.Viewer, true
	}
	return "", false // unknown write route: deny
}

// commandRoles: per-command roles; commands not listed need operator.
var commandRoles = map[domain.CommandType]auth.Role{
	domain.CmdApproveTool:         auth.Approver,
	domain.CmdRejectTool:          auth.Approver,
	domain.CmdResolveUnknown:      auth.Approver,
	domain.CmdOverrideResult:      auth.Approver,
	domain.CmdMigrateProfile:      auth.Admin,
	domain.CmdReplacePendingInput: auth.Operator,
}

func (s *Server) authMiddleware(mux *http.ServeMux) http.Handler {
	chain := s.Auth
	if chain == nil {
		chain = &auth.Chain{Authenticators: []auth.Authenticator{auth.DevHeaders{}}}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		_, pattern := mux.Handler(r)
		rec := &statusRecorder{ResponseWriter: w, code: 200}
		defer func() {
			if s.Metrics != nil {
				s.Metrics.ObserveHTTP(r.Method, pattern, rec.code, time.Since(start))
			}
		}()
		role, known := s.requiredRole(pattern)
		if pattern == "" {
			mux.ServeHTTP(rec, r) // 404 / 405 from the mux
			return
		}
		if known && role == "" {
			mux.ServeHTTP(rec, r)
			return
		}
		p, err := chain.Authenticate(r)
		if err != nil {
			code, kind := http.StatusUnauthorized, "unauthenticated"
			if errors.Is(err, auth.ErrForbidden) {
				code, kind = http.StatusForbidden, "forbidden"
			}
			s.audit(r.Context(), pg.AuditEntry{TenantID: orDash(r.Header.Get("X-Tenant-ID")), ActorType: "user",
				ActorID: "anonymous", Action: "authenticate", ResourceType: "api", ResourceID: pattern, Outcome: "denied",
				Details: map[string]any{"reason": err.Error(), "remote": r.RemoteAddr}})
			if code == http.StatusUnauthorized {
				w.Header().Set("WWW-Authenticate", `Bearer realm="sre-harness"`)
			}
			writeErr(rec, code, kind, err.Error())
			return
		}
		if !known || !p.Has(role) {
			s.audit(r.Context(), pg.AuditEntry{TenantID: p.Tenant, ActorType: "user", ActorID: p.Subject,
				Action: "authorize", ResourceType: "api", ResourceID: pattern, Outcome: "denied",
				Details: map[string]any{"required_role": role, "roles": p.Roles}})
			writeErr(rec, http.StatusForbidden, "forbidden", "role "+string(role)+" required")
			return
		}
		ctx := auth.WithPrincipal(r.Context(), p)
		ctx = pg.WithTenant(ctx, p.Tenant)
		mux.ServeHTTP(rec, r.WithContext(ctx))
	})
}

func orDash(s string) string {
	if s == "" || len(s) > 64 {
		return "-"
	}
	return s
}

func (s *Server) audit(ctx context.Context, e pg.AuditEntry) {
	if s.Audit == nil {
		return
	}
	// The audit write must not depend on the request's tenant scoping or cancellation.
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if err := s.Audit.Audit(pg.WithTenant(actx, e.TenantID), e); err != nil && s.Log != nil {
		s.Log.Error("audit write failed", "err", err, "action", e.Action)
	}
}

// requireCommandRole checks per-command roles; it writes the response on failure.
func (s *Server) requireCommandRole(w http.ResponseWriter, r *http.Request, id identity, t domain.CommandType) bool {
	need, ok := commandRoles[t]
	if !ok {
		need = auth.Operator
	}
	p, _ := auth.FromContext(r.Context())
	if p.Has(need) {
		return true
	}
	s.audit(r.Context(), pg.AuditEntry{TenantID: id.tenant, ActorType: "user", ActorID: id.actor, Action: "command." + string(t),
		ResourceType: "run", ResourceID: r.PathValue("run_id"), Outcome: "denied", Details: map[string]any{"required_role": need}})
	writeErr(w, http.StatusForbidden, "forbidden", "command "+string(t)+" requires role "+string(need))
	return false
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }

// Flush keeps SSE working through the recorder.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
