package api

import (
	"errors"
	"net/http"

	"go-sre-harness/internal/domain"
)

// getFlow returns the orchestration state of a coordinator run.
func (s *Server) getFlow(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	v, err := s.Store.LoadFlow(r.Context(), id.tenant, r.PathValue("run_id"))
	if errors.Is(err, domain.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "this run has no flow state")
		return
	}
	if err != nil {
		s.Log.Error("load flow", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal", "cannot load the flow state")
		return
	}
	writeJSON(w, http.StatusOK, v)
}
