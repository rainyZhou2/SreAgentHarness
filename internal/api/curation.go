package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/curate"
)

// Memory curation: proposals to set aside duplicate, superseded, refuted or
// unused long-term memories (package curate). Nothing is changed until an
// approver applies a proposal.

func (m *MemoryAPI) curationRoutes() map[string]auth.Role {
	if m.Curator == nil {
		return nil
	}
	return map[string]auth.Role{
		"POST /v1/memory/curation/run":         auth.Operator,
		"POST /v1/memory/curation/{id}/decide": auth.Approver,
	}
}

func (m *MemoryAPI) registerCuration(mux *http.ServeMux) {
	if m.Curator == nil {
		return
	}
	mux.HandleFunc("GET /v1/memory/curation", m.listCuration)
	mux.HandleFunc("POST /v1/memory/curation/run", m.runCuration)
	mux.HandleFunc("POST /v1/memory/curation/{id}/decide", m.decideCuration)
}

func (m *MemoryAPI) listCuration(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	list, err := m.Curator.Store.ListProposals(r.Context(), id.tenant, r.URL.Query().Get("status"), limit)
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	if list == nil {
		list = []curate.Proposal{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"proposals": list})
}

// runCuration runs one pass for the caller's tenant and reports what it found.
func (m *MemoryAPI) runCuration(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	rep, err := m.Curator.Run(r.Context(), id.tenant)
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	if rep.Skipped == nil {
		rep.Skipped = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": rep.Job, "examined": rep.Examined, "created": rep.Created,
		"new_proposals": rep.Total(), "already_proposed": rep.Existing, "skipped": rep.Skipped})
}

func (m *MemoryAPI) decideCuration(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		Decision string `json:"decision"` // apply | dismiss
		Reason   string `json:"reason"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&b); err != nil || (b.Decision != "apply" && b.Decision != "dismiss") {
		writeErr(w, http.StatusBadRequest, "invalid_body", "decision must be apply or dismiss")
		return
	}
	pid := r.PathValue("id")
	var p curate.Proposal
	var err error
	if b.Decision == "apply" {
		p, err = curate.Apply(r.Context(), m.Curator.Store, m.Curator.Memories, id.tenant, pid, id.actor, b.Reason)
	} else {
		p, err = curate.Dismiss(r.Context(), m.Curator.Store, id.tenant, pid, id.actor, b.Reason)
	}
	switch {
	case errors.Is(err, curate.ErrOutdated):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "proposal_outdated",
			"message": err.Error() + "; run curation again", "proposal": p})
		return
	case err != nil:
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}
