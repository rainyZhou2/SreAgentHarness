package workapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/impact"
	"go-sre-harness/internal/observability"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/protocol"
	"go-sre-harness/internal/quota"
	"go-sre-harness/internal/runbook"
	"go-sre-harness/internal/work"
)

// v3 routes (design §13.3). They follow the rules of the existing ones:
// the tenant is the authenticated principal's, state changes need an
// Idempotency-Key or an If-Match version, and every route has a minimum
// role in RouteRoles.
//
//	GET  /v1/work-graphs/{id}                      Work tree and dependency edges
//	POST /v1/works/{id}/children                   create a child Work within the parent's ceiling
//	POST /v1/works/{id}/dependencies               add a dependency edge (cycles refused)
//	GET  /v1/works/{id}/view                       unified lifecycle view
//	GET  /v1/works/{id}/findings                   validated agent findings
//	POST /v1/works/{id}/improvements               open Improvement Works from a closed Work
//	GET  /v1/observability/sources                 Source Catalog
//	POST /v1/observability/sources                 create or update a source (admin; If-Match)
//	POST /v1/observability/sources/{id}/rollback   re-activate an earlier revision (admin)
//	POST /v1/observability/sources/{id}/probe      reachability and authorization probe
//	POST /v1/observability/query                   authorized read-only query
//	GET  /v1/observability/evidence/{id}           evidence metadata
//	POST /v1/work-templates/{id}/dry-run           simulate a template without side effects
//	GET  /v1/governance/attestation                deployment tier, stage and gates
//	GET  /v1/quotas                                cluster-wide budgets and reservations
//	POST /v1/actions/{id}/break-glass/review       second-person review of an emergency release
//	GET  /v1/runbooks | PUT /v1/runbooks/{id} | POST /v1/runbooks/{id}/rehearsals
//	POST /v1/work-intents                          classify a request (kind, template, ceiling)
//	GET  /v1/changes | POST /v1/changes/correlate  change ledger and leads for a symptom
//	GET  /v1/world/collectors                      collector health
func (a *API) v3Roles() map[string]auth.Role {
	return map[string]auth.Role{
		"GET /v1/work-graphs/{id}":                     auth.Viewer,
		"POST /v1/works/{id}/children":                 auth.Operator,
		"POST /v1/works/{id}/dependencies":             auth.Operator,
		"GET /v1/works/{id}/view":                      auth.Viewer,
		"GET /v1/works/{id}/findings":                  auth.Viewer,
		"POST /v1/works/{id}/improvements":             auth.Operator,
		"GET /v1/observability/sources":                auth.Viewer,
		"POST /v1/observability/sources":               auth.Admin,
		"POST /v1/observability/sources/{id}/rollback": auth.Admin,
		"POST /v1/observability/sources/{id}/probe":    auth.Operator,
		"POST /v1/observability/query":                 auth.Operator,
		"GET /v1/observability/evidence/{id}":          auth.Viewer,
		"POST /v1/work-templates/{id}/dry-run":         auth.Operator,
		"GET /v1/governance/attestation":               auth.Viewer,
		"GET /v1/quotas":                               auth.Viewer,
		"POST /v1/actions/{id}/break-glass/review":     auth.Approver,
		"GET /v1/runbooks":                             auth.Viewer,
		"PUT /v1/runbooks/{id}":                        auth.Admin,
		"POST /v1/runbooks/{id}/rehearsals":            auth.Operator,
		"POST /v1/work-intents":                        auth.Viewer,
		"GET /v1/changes":                              auth.Viewer,
		"POST /v1/changes/correlate":                   auth.Viewer,
		"GET /v1/world/collectors":                     auth.Viewer,
	}
}

func (a *API) registerV3(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/work-graphs/{id}", a.workGraph)
	mux.HandleFunc("POST /v1/works/{id}/children", a.createChild)
	mux.HandleFunc("POST /v1/works/{id}/dependencies", a.addDependency)
	mux.HandleFunc("GET /v1/works/{id}/view", a.workView)
	mux.HandleFunc("GET /v1/works/{id}/findings", a.sub(func(ctx context.Context, t, id string) (any, error) {
		return a.Store.ListFindings(ctx, t, id, 500)
	}))
	mux.HandleFunc("POST /v1/works/{id}/improvements", a.createImprovements)
	mux.HandleFunc("GET /v1/observability/sources", a.listSources)
	mux.HandleFunc("POST /v1/observability/sources", a.putSource)
	mux.HandleFunc("POST /v1/observability/sources/{id}/rollback", a.rollbackSource)
	mux.HandleFunc("POST /v1/observability/sources/{id}/probe", a.probeSource)
	mux.HandleFunc("POST /v1/observability/query", a.obsQuery)
	mux.HandleFunc("GET /v1/observability/evidence/{id}", a.obsEvidence)
	mux.HandleFunc("POST /v1/work-templates/{id}/dry-run", a.dryRunTemplate)
	mux.HandleFunc("GET /v1/governance/attestation", a.attestation)
	mux.HandleFunc("GET /v1/quotas", a.quotas)
	mux.HandleFunc("POST /v1/actions/{id}/break-glass/review", a.reviewBreakGlass)
	mux.HandleFunc("GET /v1/runbooks", a.listRunbooks)
	mux.HandleFunc("PUT /v1/runbooks/{id}", a.putRunbook)
	mux.HandleFunc("POST /v1/runbooks/{id}/rehearsals", a.rehearseRunbook)
	mux.HandleFunc("POST /v1/work-intents", a.workIntent)
	mux.HandleFunc("GET /v1/changes", a.listChanges)
	mux.HandleFunc("POST /v1/changes/correlate", a.correlateChanges)
	mux.HandleFunc("GET /v1/world/collectors", a.collectors)
}

// failV3 maps the v3 domain errors; everything else goes to fail.
func (a *API) failV3(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, orchestration.ErrOverloaded):
		w.Header().Set("Retry-After", "30")
		writeErr(w, http.StatusTooManyRequests, "overloaded", err.Error())
	case errors.Is(err, observability.ErrNotFound), errors.Is(err, runbook.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", "not found")
	case errors.Is(err, observability.ErrVersionConflict), errors.Is(err, runbook.ErrVersionConflict):
		writeErr(w, http.StatusPreconditionFailed, "version_mismatch", "the resource changed; reload and retry")
	case errors.Is(err, observability.ErrInvalid), errors.Is(err, runbook.ErrInvalid), errors.Is(err, impact.ErrInvalid),
		errors.Is(err, quota.ErrInvalid):
		writeErr(w, http.StatusBadRequest, "invalid_request", err.Error())
	default:
		a.fail(w, r, err)
	}
}

// ---- work graph ----

func (a *API) workGraph(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	g, err := a.Store.WorkGraph(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, g)
}

func (a *API) createChild(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	var req orchestration.ChildRequest
	if !decode(w, r, &req) {
		return
	}
	for _, ref := range req.Goal.SubjectRefs {
		if !p.AllowsResource(ref) {
			a.outOfScope(w, r, p, "work.create_child", "subject", ref, "subject "+ref+" is outside the caller's scope")
			return
		}
	}
	req.TenantID, req.ParentID, req.ActorID, req.ActorType, req.IdempotencyKey = p.Tenant, r.PathValue("id"), p.Subject, "user", key
	wk, created, err := a.Service.CreateChild(r.Context(), req)
	if err != nil {
		a.failV3(w, r, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	etag(w, wk.Version)
	w.Header().Set("Location", "/v1/works/"+wk.ID)
	writeJSON(w, code, wk)
}

func (a *API) addDependency(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	var req struct {
		UpstreamID   string              `json:"upstream_work_id"`
		Kind         work.DependencyKind `json:"kind,omitempty"`
		PredicateRef string              `json:"predicate_ref,omitempty"`
		Optional     bool                `json:"optional,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	dep := work.Dependency{ParentWorkID: req.UpstreamID, ChildWorkID: r.PathValue("id"), Kind: req.Kind,
		PredicateRef: req.PredicateRef, Required: !req.Optional}
	if err := a.Service.AddDependency(r.Context(), p.Tenant, dep, version); err != nil {
		a.failV3(w, r, err)
		return
	}
	g, err := a.Store.WorkGraph(r.Context(), p.Tenant, dep.ChildWorkID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, g)
}

// workView assembles the unified lifecycle view of a Work.
func (a *API) workView(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	wk, err := a.Store.GetWork(ctx, p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	unmet, err := a.Store.UnmetDependencies(ctx, p.Tenant, wk.ID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	v := protocol.View{SchemaVersion: protocol.SchemaVersion, ContractVersion: protocol.ContractVersion, MinimumReaderVersion: protocol.MinimumReaderVersion, Work: wk, WaitingOn: work.WaitingOn(wk, len(unmet) > 0 && wk.Status == work.StatusNew),
		Controls: protocol.Controls(wk.Status), LastSeq: wk.LastSeq}
	for i, st := range wk.Template.Stages {
		status := "pending"
		switch {
		case i < wk.Stage || wk.Status == work.StatusSucceeded:
			status = "done"
		case i == wk.Stage && !wk.Status.Terminal():
			status = "current"
		}
		v.Stages = append(v.Stages, protocol.StageView{ID: st.ID, Type: string(st.Type), Phase: st.Phase, Status: status})
	}
	if v.Runs, err = a.Store.ListRunLinks(ctx, p.Tenant, wk.ID); err != nil {
		a.fail(w, r, err)
		return
	}
	acts, err := a.Store.ListActions(ctx, p.Tenant, wk.ID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	dispatched := false
	for _, x := range acts {
		v.Actions = append(v.Actions, protocol.ActionView{ID: x.ID, Name: x.ActionName, Target: x.TargetRef, State: string(x.State),
			Risk: string(x.Risk), MayHaveDispatched: x.MayHaveDispatched, DispatchClass: x.DispatchClass,
			NeedsHuman: x.State == action.StateWaitingApproval || x.State == action.StateEffectUnknown || x.State == action.StateManualRecovery})
		dispatched = dispatched || x.MayHaveDispatched
	}
	if dispatched {
		v.CancelNote = "Cancelling stops further scheduling. Actions already dispatched are not undone; their effects stay tracked until they are verified or resolved by a human."
	}
	outs, err := a.Store.ListOutcomes(ctx, p.Tenant, wk.ID, 1)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if len(outs) > 0 {
		v.Outcome = &protocol.OutcomeView{Verdict: string(outs[0].Verdict), EvaluatedAt: outs[0].EvaluatedAt, Reasons: outs[0].Reasons,
			EvidenceID: outs[0].ID}
	}
	kids, err := a.Store.Children(ctx, p.Tenant, wk.ID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	for _, k := range kids {
		n := work.GraphNode{ID: k.ID, ParentID: wk.ID, Depth: k.Depth, Kind: k.Kind, Status: k.Status, Title: k.Title, Queue: k.Queue,
			Reason: k.Reason}
		n.WaitingOn = work.WaitingOn(k, strings.HasPrefix(k.Reason, "held"))
		v.Children = append(v.Children, n)
	}
	etag(w, wk.Version)
	writeJSON(w, http.StatusOK, v)
}

func (a *API) createImprovements(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	src, err := a.Store.GetWork(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	for _, ref := range src.Goal.SubjectRefs {
		if !p.AllowsResource(ref) {
			a.outOfScope(w, r, p, "work.improvements", "work", src.ID, "the Work is outside the caller's scope")
			return
		}
	}
	var req struct {
		// Items overrides the follow-ups derived from the postmortem report.
		Items []orchestration.Improvement `json:"items,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	if len(req.Items) == 0 {
		if a.Postmortems == nil {
			writeErr(w, http.StatusServiceUnavailable, "disabled", "postmortem reports are not configured; pass items")
			return
		}
		rep, err := a.Postmortems.Build(r.Context(), p.Tenant, src.ID)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		for _, f := range rep.FollowUps {
			req.Items = append(req.Items, orchestration.Improvement{Title: f.Title, Why: f.Why, Priority: f.Priority})
		}
		if len(req.Items) == 0 {
			writeJSON(w, http.StatusOK, map[string]any{"items": []any{}, "note": "the report derived no follow-ups"})
			return
		}
	}
	works, err := a.Service.CreateImprovements(r.Context(), src, p.Subject, req.Items)
	if err != nil {
		a.failV3(w, r, err)
		return
	}
	a.audit(r.Context(), p, "work.improvements", "work", src.ID, "applied", map[string]any{"count": len(works)})
	writeJSON(w, http.StatusCreated, map[string]any{"items": works})
}

// ---- observability ----

func (a *API) brokerEnabled(w http.ResponseWriter) bool {
	if a.Broker == nil {
		writeErr(w, http.StatusServiceUnavailable, "disabled", "the observability broker is not configured")
		return false
	}
	return true
}

func (a *API) listSources(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	srcs, err := a.Store.ListSources(r.Context(), p.Tenant, observability.Filter{Kind: observability.Signal(q.Get("kind")),
		Environment: q.Get("environment")})
	if err != nil {
		a.failV3(w, r, err)
		return
	}
	if id := q.Get("revisions_of"); id != "" {
		revs, err := a.Store.SourceRevisions(r.Context(), p.Tenant, id, 100)
		if err != nil {
			a.failV3(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": srcs, "revisions": revs})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": srcs})
}

// putSource creates (no If-Match) or updates (If-Match: version) a source.
// The body names an endpoint_ref and a credential_id the server must know;
// it cannot carry a URL or a secret, and its tenant is the caller's.
func (a *API) putSource(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !a.brokerEnabled(w) {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	var req struct {
		observability.Source
		Reason string `json:"reason,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	src := req.Source
	src.TenantID = p.Tenant
	if _, known := a.Broker.Endpoints[src.EndpointRef]; !known && src.Backend != "ledger" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "endpoint_ref is not an endpoint configured on this server")
		return
	}
	if src.CredentialID != "" {
		if _, err := a.Broker.Credentials.Resolve(r.Context(), p.Tenant, src.CredentialID); err != nil {
			// Say that it is unusable, not why in detail: the reply must not
			// become an oracle for which credentials exist.
			writeErr(w, http.StatusBadRequest, "invalid_request", "credential_id is not usable by this tenant on this server")
			return
		}
	}
	out, err := a.Store.PutSource(r.Context(), src, version, p.Subject, req.Reason)
	if err != nil {
		a.failV3(w, r, err)
		return
	}
	a.audit(r.Context(), p, "observability.source.put", "source", out.ID, "applied", map[string]any{"version": out.Version,
		"state": out.State, "backend": out.Backend, "endpoint_ref": out.EndpointRef, "reason": req.Reason})
	etag(w, out.Version)
	writeJSON(w, http.StatusOK, out)
}

func (a *API) rollbackSource(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	var req struct {
		ToVersion int64  `json:"to_version"`
		Reason    string `json:"reason,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	out, err := observability.Rollback(r.Context(), a.Store, p.Tenant, r.PathValue("id"), req.ToVersion, p.Subject, req.Reason)
	if err != nil {
		a.failV3(w, r, err)
		return
	}
	a.audit(r.Context(), p, "observability.source.rollback", "source", out.ID, "applied", map[string]any{"to_version": req.ToVersion,
		"version": out.Version})
	etag(w, out.Version)
	writeJSON(w, http.StatusOK, out)
}

func (a *API) probeSource(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !a.brokerEnabled(w) {
		return
	}
	h, err := a.Broker.Probe(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.failV3(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, h)
}

func (a *API) obsQuery(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !a.brokerEnabled(w) {
		return
	}
	var req struct {
		observability.Query
		WorkID string `json:"work_id,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.SubjectRef != "" && !p.AllowsResource(req.SubjectRef) {
		a.outOfScope(w, r, p, "observability.query", "subject", req.SubjectRef, "subject is outside the caller's scope")
		return
	}
	c := observability.Caller{RC: cpkit.RequestContext{TenantID: p.Tenant, PrincipalID: p.Subject, PrincipalType: "user",
		DataClass: "confidential"}, Expert: p.Has(auth.Admin)}
	if req.WorkID != "" {
		wk, err := a.Store.GetWork(r.Context(), p.Tenant, req.WorkID)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		c.RC.WorkID, c.Subjects = wk.ID, wk.Goal.SubjectRefs
	}
	res := a.Broker.Query(r.Context(), c, req.Query)
	out := map[string]any{"result": res}
	if c.RC.WorkID != "" {
		if rec, ok := observability.EvidenceOf(p.Tenant, c.RC.WorkID, res, ""); ok {
			if stored, _, err := a.Store.AddEvidence(r.Context(), rec); err == nil {
				out["evidence_ref"] = "evidence:" + stored.ID
			}
		}
	}
	if res.Status == observability.StatusDenied {
		a.audit(r.Context(), p, "observability.query", "source", req.SourceID, "denied", map[string]any{"reason": strings.Join(res.Warnings, "; ")})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) obsEvidence(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	rec, err := a.Store.GetEvidence(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

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

// ---- runbooks ----

func (a *API) listRunbooks(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	all, err := a.Store.ListRunbooks(r.Context(), p.Tenant)
	if err != nil {
		a.failV3(w, r, err)
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
		a.failV3(w, r, err)
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
		a.failV3(w, r, err)
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
		a.failV3(w, r, err)
		return
	}
	a.audit(r.Context(), p, "runbook.rehearsal", "runbook", rb.ID, "applied", map[string]any{"version": rb.Version, "work_id": req.WorkID})
	writeJSON(w, http.StatusOK, rb)
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
		a.failV3(w, r, err)
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
		a.failV3(w, r, err)
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
