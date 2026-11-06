package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/store/pg"
)

func (s *Server) whoami(w http.ResponseWriter, r *http.Request) {
	p, ok := auth.FromContext(r.Context())
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthenticated", "authentication required")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

type roleBody struct {
	PrincipalID string    `json:"principal_id"`
	Role        auth.Role `json:"role"`
}

func (s *Server) roleBinding(w http.ResponseWriter, r *http.Request, bind bool) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b roleBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&b); err != nil || b.PrincipalID == "" || !auth.ValidRole(b.Role) {
		writeErr(w, http.StatusBadRequest, "invalid_body", "principal_id and a valid role are required")
		return
	}
	var err error
	action := "role.bind"
	if bind {
		err = s.Admin.BindRole(r.Context(), id.tenant, b.PrincipalID, b.Role, id.actor)
	} else {
		action = "role.unbind"
		err = s.Admin.UnbindRole(r.Context(), id.tenant, b.PrincipalID, b.Role)
	}
	outcome := "applied"
	if err != nil {
		outcome = "rejected"
	}
	s.audit(r.Context(), pg.AuditEntry{TenantID: id.tenant, ActorType: "user", ActorID: id.actor, Action: action,
		ResourceType: "principal", ResourceID: b.PrincipalID, Outcome: outcome, Details: map[string]any{"role": b.Role},
		TraceID: obs.TraceID(r.Context())})
	if err != nil {
		s.storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (s *Server) bindRole(w http.ResponseWriter, r *http.Request)   { s.roleBinding(w, r, true) }
func (s *Server) unbindRole(w http.ResponseWriter, r *http.Request) { s.roleBinding(w, r, false) }

func (s *Server) listAudit(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	es, err := s.Admin.ListAudit(r.Context(), id.tenant, limit)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": es})
}
