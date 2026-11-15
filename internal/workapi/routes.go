package workapi

import (
	"context"
	"fmt"
	"net/http"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/work"
)

type route struct {
	pattern string
	role    auth.Role
	handler http.HandlerFunc
}

// routes is the single source of truth for HTTP registration and authorization.
func (a *API) routes() []route {
	return []route{
		{pattern: "POST /api/works", role: auth.Operator, handler: a.createWork},
		{pattern: "GET /api/works", role: auth.Viewer, handler: a.listWorks},
		{pattern: "GET /api/works/{id}", role: auth.Viewer, handler: a.getWork},
		{pattern: "GET /api/works/{id}/events", role: auth.Viewer, handler: a.workEvents},
		{pattern: "GET /api/works/{id}/events/stream", role: auth.Viewer, handler: a.streamEvents},
		{pattern: "GET /api/works/{id}/runs", role: auth.Viewer, handler: a.sub(func(ctx context.Context, t, id string) (any, error) {
			return a.Store.ListRunLinks(ctx, t, id)
		})},
		{pattern: "GET /api/works/{id}/evidence", role: auth.Viewer, handler: a.sub(func(ctx context.Context, t, id string) (any, error) {
			return a.Store.ListEvidence(ctx, t, id, 500)
		})},
		{pattern: "GET /api/works/{id}/hypotheses", role: auth.Viewer, handler: a.sub(func(ctx context.Context, t, id string) (any, error) {
			w, err := a.Store.GetWork(ctx, t, id)
			if err != nil {
				return nil, err
			}
			hs, err := a.Store.ListHypotheses(ctx, t, id)
			if err != nil {
				return nil, err
			}
			ledger, err := a.Store.ListEvidence(ctx, t, id, evidence.MaxDecisionEvidence)
			if err != nil {
				return nil, err
			}
			scope := evidence.DecisionScope{TenantID: t, WorkID: id, Generation: w.Generation}
			type view struct {
				evidence.Hypothesis
				DecisionCurrent bool `json:"decision_current"`
			}
			out := make([]view, 0, len(hs))
			at := a.Clock.Now().UTC()
			for _, h := range hs {
				out = append(out, view{Hypothesis: h, DecisionCurrent: h.ValidDecisionFor(scope, ledger, at)})
			}
			return out, nil
		})},
		{pattern: "GET /api/works/{id}/decisions", role: auth.Viewer, handler: a.sub(func(ctx context.Context, t, id string) (any, error) {
			w, err := a.Store.GetWork(ctx, t, id)
			if err != nil {
				return nil, err
			}
			hs, err := a.Store.ListHypotheses(ctx, t, id)
			if err != nil {
				return nil, err
			}
			ledger, err := a.Store.ListEvidence(ctx, t, id, evidence.MaxDecisionEvidence)
			if err != nil {
				return nil, err
			}
			decisions := []evidence.DecisionVerdict{}
			scope := evidence.DecisionScope{TenantID: t, WorkID: id, Generation: w.Generation}
			at := a.Clock.Now().UTC()
			for _, h := range hs {
				if h.ValidDecisionFor(scope, ledger, at) {
					decisions = append(decisions, *h.Decision)
				}
			}
			return decisions, nil
		})},
		{pattern: "GET /api/works/{id}/watches", role: auth.Viewer, handler: a.sub(func(ctx context.Context, t, id string) (any, error) {
			return a.Store.ListWatches(ctx, t, id, false)
		})},
		{pattern: "GET /api/works/{id}/evaluation", role: auth.Viewer, handler: a.sub(func(ctx context.Context, t, id string) (any, error) {
			if a.Eval == nil {
				return nil, fmt.Errorf("%w: work evaluation is not configured", work.ErrInvalid)
			}
			return a.Eval.Score(ctx, t, id)
		})},
		{pattern: "GET /api/works/{id}/outcomes", role: auth.Viewer, handler: a.sub(func(ctx context.Context, t, id string) (any, error) {
			return a.Store.ListOutcomes(ctx, t, id, 100)
		})},
		{pattern: "GET /api/works/{id}/actions", role: auth.Viewer, handler: a.sub(func(ctx context.Context, t, id string) (any, error) {
			return a.Store.ListActions(ctx, t, id)
		})},
		{pattern: "GET /api/works/{id}/assignments", role: auth.Viewer, handler: a.sub(func(ctx context.Context, t, id string) (any, error) {
			return a.assignments(ctx, t, id)
		})},
		{pattern: "GET /api/works/{id}/goals", role: auth.Viewer, handler: a.sub(func(ctx context.Context, t, id string) (any, error) {
			return a.Store.ListGoals(ctx, t, id)
		})},
		{pattern: "POST /api/works/{id}/commands", role: auth.Operator, handler: a.command},
		{pattern: "GET /api/work-templates", role: auth.Viewer, handler: func(w http.ResponseWriter, r *http.Request) {
			if _, ok := principal(w, r); ok {
				writeJSON(w, http.StatusOK, map[string]any{"templates": a.Service.Catalog.List()})
			}
		}},
		{pattern: "POST /api/events", role: auth.Service, handler: a.ingest},
		{pattern: "GET /api/events/{id}", role: auth.Viewer, handler: a.getEvent},
		{pattern: "GET /api/world/entities", role: auth.Viewer, handler: a.entities},
		{pattern: "GET /api/world/facts", role: auth.Viewer, handler: a.facts},
		{pattern: "POST /api/world/facts", role: auth.Operator, handler: a.assertFact},
		{pattern: "POST /api/world/snapshots/query", role: auth.Viewer, handler: a.snapshot},
		{pattern: "GET /api/world/snapshots/{id}", role: auth.Viewer, handler: a.getSnapshot},
		{pattern: "GET /api/actions/{id}", role: auth.Viewer, handler: a.getAction},
		{pattern: "GET /api/actions/{id}/effects", role: auth.Viewer, handler: a.actionEffects},
		{pattern: "POST /api/actions/{id}/approve", role: auth.Approver, handler: a.approve},
		{pattern: "POST /api/actions/{id}/reconcile", role: auth.Operator, handler: a.reconcile},
		{pattern: "POST /api/actions/{id}/resolve", role: auth.Approver, handler: a.resolve},
		{pattern: "POST /api/actions/{id}/break-glass", role: auth.BreakGlass, handler: a.breakGlass},
		{pattern: "POST /api/works/{id}/evidence", role: auth.Operator, handler: a.addEvidence},
		{pattern: "POST /api/works/{id}/playbook", role: auth.Operator, handler: a.proposePlaybook},
		{pattern: "GET /api/works/{id}/postmortem", role: auth.Viewer, handler: a.getPostmortem},
		{pattern: "POST /api/works/{id}/postmortem", role: auth.Operator, handler: a.createPostmortem},
		{pattern: "GET /api/events/shadow-report", role: auth.Viewer, handler: a.shadowReport},
		{pattern: "POST /api/sources/{source}/events", role: auth.Service, handler: a.sourceEvents},
		{pattern: "GET /api/work-graphs/{id}", role: auth.Viewer, handler: a.workGraph},
		{pattern: "POST /api/works/{id}/children", role: auth.Operator, handler: a.createChild},
		{pattern: "POST /api/works/{id}/dependencies", role: auth.Operator, handler: a.addDependency},
		{pattern: "POST /api/works/{id}/caused-by", role: auth.Operator, handler: a.linkIncidentCause},
		{pattern: "GET /api/works/{id}/view", role: auth.Viewer, handler: a.workView},
		{pattern: "GET /api/works/{id}/findings", role: auth.Viewer, handler: a.sub(func(ctx context.Context, t, id string) (any, error) {
			return a.Store.ListFindings(ctx, t, id, 500)
		})},
		{pattern: "POST /api/works/{id}/improvements", role: auth.Operator, handler: a.createImprovements},
		{pattern: "GET /api/observability/sources", role: auth.Viewer, handler: a.listSources},
		{pattern: "POST /api/observability/sources", role: auth.Admin, handler: a.putSource},
		{pattern: "POST /api/observability/sources/{id}/rollback", role: auth.Admin, handler: a.rollbackSource},
		{pattern: "POST /api/observability/sources/{id}/probe", role: auth.Operator, handler: a.probeSource},
		{pattern: "POST /api/observability/query", role: auth.Operator, handler: a.obsQuery},
		{pattern: "GET /api/observability/evidence/{id}", role: auth.Viewer, handler: a.obsEvidence},
		{pattern: "POST /api/work-templates/{id}/dry-run", role: auth.Operator, handler: a.dryRunTemplate},
		{pattern: "GET /api/governance/attestation", role: auth.Viewer, handler: a.attestation},
		{pattern: "GET /api/quotas", role: auth.Viewer, handler: a.quotas},
		{pattern: "POST /api/actions/{id}/break-glass/review", role: auth.Approver, handler: a.reviewBreakGlass},
		{pattern: "GET /api/runbooks", role: auth.Viewer, handler: a.listRunbooks},
		{pattern: "PUT /api/runbooks/{id}", role: auth.Admin, handler: a.putRunbook},
		{pattern: "POST /api/runbooks/{id}/rehearsals", role: auth.Operator, handler: a.rehearseRunbook},
		{pattern: "POST /api/work-intents", role: auth.Viewer, handler: a.workIntent},
		{pattern: "GET /api/changes", role: auth.Viewer, handler: a.listChanges},
		{pattern: "POST /api/changes/correlate", role: auth.Viewer, handler: a.correlateChanges},
		{pattern: "GET /api/world/collectors", role: auth.Viewer, handler: a.collectors},
	}
}

// RouteRoles declares the minimum role per route.
func (a *API) RouteRoles() map[string]auth.Role {
	routes := a.routes()
	out := make(map[string]auth.Role, len(routes))
	for _, rt := range routes {
		out[rt.pattern] = rt.role
	}
	return out
}

// Register mounts the routes.
func (a *API) Register(mux *http.ServeMux) {
	for _, rt := range a.routes() {
		mux.HandleFunc(rt.pattern, rt.handler)
	}
}
