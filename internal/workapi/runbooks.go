package workapi

import (
	"net/http"
	"strings"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/runbook"
)

// ---- runbooks ----

func (a *API) listRunbooks(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	all, err := a.Store.ListRunbooks(r.Context(), p.Tenant)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	q := r.URL.Query()
	if q.Get("subject") != "" || q.Get("alert_type") != "" {
		all = runbook.Match(all, q.Get("subject"), q.Get("alert_type"), q.Get("environment"), a.Clock.Now())
	}
	type row struct {
		runbook.Runbook
		Problems []string `json:"problems,omitempty"`
	}
	out := make([]row, len(all))
	for i, rb := range all {
		out[i] = row{Runbook: rb, Problems: rb.Problems("", a.Clock.Now())}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (a *API) putRunbook(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	var req struct {
		runbook.Spec
		Status string `json:"status,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	req.Spec.ID = r.PathValue("id")
	if _, found := a.Service.Catalog.Get(req.Lifecycle.TemplateID); !found {
		writeErr(w, http.StatusBadRequest, "invalid_request", "lifecycle.template_id is not a known template")
		return
	}
	rb, err := a.Store.PutRunbook(r.Context(), p.Tenant, req.Spec, req.Status, version)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.audit(r.Context(), p, "runbook.put", "runbook", rb.ID, "applied", map[string]any{"version": rb.Version, "status": rb.Status})
	etag(w, rb.RowVersion)
	writeJSON(w, http.StatusOK, rb)
}

func (a *API) rehearseRunbook(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	var req struct {
		Version int    `json:"version"`
		Comment string `json:"comment,omitempty"`
		// WorkID is the rehearsal Work (a dry run or a game-day exercise).
		WorkID string `json:"work_id,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	cur, err := a.Store.GetRunbook(r.Context(), p.Tenant, r.PathValue("id"), req.Version)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	now := a.Clock.Now()
	comment := cpkit.Truncate(req.Comment, 500)
	if req.WorkID != "" {
		if _, err := a.Store.GetWork(r.Context(), p.Tenant, req.WorkID); err != nil {
			a.fail(w, r, err)
			return
		}
		comment = strings.TrimSpace("rehearsal work " + req.WorkID + ". " + comment)
	}
	rb, err := a.Store.RecordRehearsal(r.Context(), p.Tenant, cur.ID, cur.Version, now,
		&runbook.Approval{Approver: p.Subject, Comment: comment, At: now})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.audit(r.Context(), p, "runbook.rehearsal", "runbook", rb.ID, "applied", map[string]any{"version": rb.Version, "work_id": req.WorkID})
	writeJSON(w, http.StatusOK, rb)
}
