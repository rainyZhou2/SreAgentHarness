package workapi

import (
	"net/http"
	"strings"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/incident"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/protocol"
	"go-sre-harness/internal/work"
)

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
		a.fail(w, r, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	etag(w, wk.Version)
	w.Header().Set("Location", "/api/works/"+wk.ID)
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
		a.fail(w, r, err)
		return
	}
	g, err := a.Store.WorkGraph(r.Context(), p.Tenant, dep.ChildWorkID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, g)
}

func (a *API) linkIncidentCause(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	if strings.TrimSpace(r.Header.Get("If-Match")) == "" {
		writeErr(w, http.StatusPreconditionRequired, "if_match_required", "If-Match must contain the current Work version")
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	var req orchestration.LinkCauseRequest
	if !decode(w, r, &req) {
		return
	}
	target, err := a.Store.GetWork(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	for _, ref := range target.Goal.SubjectRefs {
		if !p.AllowsResource(ref) {
			a.outOfScope(w, r, p, "work.incident_cause.link", "work", target.ID, "the Work is outside the caller's scope")
			return
		}
	}
	req.TenantID, req.WorkID, req.ActorID = p.Tenant, target.ID, p.Subject
	req.IdempotencyKey, req.ExpectedVersion = key, version
	updated, changed, err := a.Service.LinkIncidentCause(r.Context(), req)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	outcome := "unchanged"
	if changed {
		outcome = "applied"
	}
	a.audit(r.Context(), p, "work.incident_cause.link", "work", target.ID, outcome, map[string]any{
		"incident_id": req.IncidentID, "action_id": req.ActionID, "expected_version": version, "version": updated.Version})
	etag(w, updated.Version)
	writeJSON(w, http.StatusOK, updated)
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
	if wk.Kind == work.KindIncident {
		projection, err := incident.Project(ctx, a.Store, wk, a.Clock.Now().UTC())
		if err != nil {
			a.fail(w, r, err)
			return
		}
		v.Incident = &projection
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
		a.fail(w, r, err)
		return
	}
	a.audit(r.Context(), p, "work.improvements", "work", src.ID, "applied", map[string]any{"count": len(works)})
	writeJSON(w, http.StatusCreated, map[string]any{"items": works})
}
