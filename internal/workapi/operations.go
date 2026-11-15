package workapi

import (
	"net/http"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/impact"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/work"
)

// ---- templates, governance, quotas ----

func (a *API) dryRunTemplate(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	var req struct {
		Kind work.Kind     `json:"kind,omitempty"`
		Goal work.GoalSpec `json:"goal"`
	}
	if !decode(w, r, &req) {
		return
	}
	tpl, found := a.Service.Catalog.Get(r.PathValue("id"))
	if !found {
		writeErr(w, http.StatusNotFound, "not_found", "unknown template")
		return
	}
	var actions []action.Descriptor
	if a.Actions != nil {
		actions = a.Actions.Registry.Descriptors()
	}
	for _, ref := range req.Goal.SubjectRefs {
		if !p.AllowsResource(ref) {
			a.outOfScope(w, r, p, "template.dry_run", "subject", ref, "subject is outside the caller's scope")
			return
		}
	}
	plan := orchestration.DryRun(tpl, req.Kind, req.Goal, actions, a.Actions != nil)
	writeJSON(w, http.StatusOK, plan)
}

func (a *API) attestation(w http.ResponseWriter, r *http.Request) {
	if _, ok := principal(w, r); !ok {
		return
	}
	if a.Attestation == nil {
		writeErr(w, http.StatusServiceUnavailable, "disabled", "no attestation is available")
		return
	}
	writeJSON(w, http.StatusOK, a.Attestation())
}

func (a *API) quotas(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	bs, err := a.Store.Budgets(r.Context(), p.Tenant)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	held, err := a.Store.ActiveReservations(r.Context(), p.Tenant, 200)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"budgets": bs, "reservations": held})
}

func (a *API) reviewBreakGlass(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !a.actionsEnabled(w) {
		return
	}
	var req struct {
		Comment string `json:"comment,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	if p.Method == "dev" {
		a.outOfScope(w, r, p, "action.break_glass_review", "action", r.PathValue("id"), "a review requires an authenticated identity")
		return
	}
	rec, err := a.Actions.ReviewBreakGlass(r.Context(), p.Tenant, r.PathValue("id"), p.Subject, req.Comment)
	if err != nil {
		a.audit(r.Context(), p, "action.break_glass_review", "action", r.PathValue("id"), "rejected", map[string]any{"reason": err.Error()})
		a.fail(w, r, err)
		return
	}
	a.audit(r.Context(), p, "action.break_glass_review", "action", rec.ID, "applied", map[string]any{"work_id": rec.WorkID,
		"released_by": rec.BreakGlass.Actor, "incident_ref": rec.BreakGlass.IncidentRef})
	writeJSON(w, http.StatusOK, rec)
}

// ---- intents and changes ----

func (a *API) workIntent(w http.ResponseWriter, r *http.Request) {
	if _, ok := principal(w, r); !ok {
		return
	}
	var req struct {
		Text string `json:"text"`
	}
	if !decode(w, r, &req) {
		return
	}
	writeJSON(w, http.StatusOK, orchestration.RouteIntent(cpkit.Truncate(req.Text, 4000)))
}

func (a *API) listChanges(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	subject, err := cpkit.NormalizeRef(r.URL.Query().Get("subject"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", "subject must be a resource ref")
		return
	}
	now := a.Clock.Now()
	since, ok := queryTime(w, r, "since", now.Add(-24*time.Hour))
	if !ok {
		return
	}
	until, ok := queryTime(w, r, "until", now)
	if !ok {
		return
	}
	cs, err := a.Store.ChangesFor(r.Context(), p.Tenant, []string{subject}, since, until, queryInt(r, "limit", 100, 500))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": cs})
}

// correlateChanges ranks the changes around a symptom. The answer is a
// list of leads with the checks that would confirm or dismiss each; it
// never names a cause.
func (a *API) correlateChanges(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	var req struct {
		Subject   string         `json:"subject"`
		StartedAt time.Time      `json:"started_at"`
		Window    cpkit.Duration `json:"window,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	leads, err := impact.Leads(r.Context(), a.Store, a.World, p.Tenant, impact.Symptom{Subject: req.Subject,
		StartedAt: req.StartedAt, Window: req.Window.D()}, a.Clock.Now())
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"leads": leads,
		"note": "Leads are ordered by proximity in time and topology. None of them is a root cause until an independent check confirms it."})
}

func (a *API) collectors(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	hs, err := a.Store.CollectorHealth(r.Context(), p.Tenant)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	now := a.Clock.Now()
	out := make([]map[string]any, 0, len(hs))
	for _, h := range hs {
		out = append(out, map[string]any{"collector": h.Collector, "state": h.State(now), "health": h})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}
