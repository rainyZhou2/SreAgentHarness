package kernel

import (
	"context"
	"encoding/json"
	"fmt"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	"go-sre-harness/internal/planner"
)

// DefaultPlanMaxReplans is how many times a failed generated flow may be
// replaced by a new plan within one run.
const DefaultPlanMaxReplans = 1

// replanDoc is the input artifact of a replan step. Kind distinguishes it
// from the goal artifact an initial plan step points at.
type replanDoc struct {
	Kind   string         `json:"kind"`
	Replan planner.Replan `json:"replan"`
}

const replanKind = "replan"

// latestFlowPlan returns the newest committed plan artifact of this run whose
// mode is flow, or nil when the run never planned a flow (a profile flow or a
// subflow child run).
func (w *Worker) latestFlowPlan(ctx context.Context, lease domain.Lease) (*planArtifact, error) {
	steps, err := w.store.History(ctx, lease.TenantID, lease.RunID, 1<<62)
	if err != nil {
		return nil, err
	}
	for i := len(steps) - 1; i >= 0; i-- {
		st := steps[i]
		if st.Kind != domain.StepPlan || st.Status != domain.StepSucceeded || st.OutputRef == nil {
			continue
		}
		var p planArtifact
		if err := w.get(ctx, lease.TenantID, *st.OutputRef, &p); err != nil {
			return nil, err
		}
		if p.Mode != "flow" {
			return nil, nil
		}
		return &p, nil
	}
	return nil, nil
}

// replanAfterFailure replaces a failed generated flow with a planning step
// when the run still has replan budget. It returns ok=false when the failure
// must fail the run as before: no planner, a profile-defined flow, budget
// used up, or children of the failed generation still running.
//
// The failed generation's state is committed as FAILED and kept for audit;
// the planner sees which nodes ran, which had side effects, and excerpts of
// their outputs (as untrusted data), and its plan runs as generation+1.
func (w *Worker) replanAfterFailure(ctx context.Context, lease domain.Lease, spec *flow.Spec, st *flow.State,
	snap domain.FlowSnapshot, results map[string]flow.NodeResult, failure string, commit *domain.FlowCommit,
	events []domain.EmittedEvent) (domain.StepSuccess, bool, error) {
	if w.cfg.Planner == nil || w.cfg.PlanMaxReplans <= 0 || lease.FlowDepth > 0 {
		return domain.StepSuccess{}, false, nil
	}
	for _, n := range snap.Nodes {
		if n.Status == "PENDING" || n.Status == "RUNNING" {
			return domain.StepSuccess{}, false, nil
		}
	}
	plan, err := w.latestFlowPlan(ctx, lease)
	if err != nil || plan == nil {
		return domain.StepSuccess{}, false, err
	}
	if plan.Generation != snap.Generation || plan.Generation >= w.cfg.PlanMaxReplans {
		return domain.StepSuccess{}, false, nil
	}
	planHash := plan.PlanHash
	if planHash == "" {
		planHash = planner.PlanHash(plan.Mode, plan.FlowHash, plan.Input, plan.Generation)
	}
	rp := planner.Replan{
		Generation: plan.Generation + 1, PreviousPlanHash: planHash, PreviousFlowHash: plan.FlowHash,
		PreviousRationale: plan.Rationale, PreviousFallback: plan.Fallback, Failure: failure,
		Nodes: w.replanNodes(lease.TenantID, spec, st, results),
	}
	ref, hash, err := w.put(ctx, lease.TenantID, "replan", replanDoc{Kind: replanKind, Replan: rp})
	if err != nil {
		return domain.StepSuccess{}, false, err
	}
	commit.Status = "FAILED"
	sideEffects := 0
	for _, n := range rp.Nodes {
		if n.SideEffect && n.Status == string(flow.NodeSucceeded) {
			sideEffects++
		}
	}
	payload := map[string]any{"generation": rp.Generation, "previous_plan_hash": planHash,
		"previous_flow_hash": plan.FlowHash, "failure": truncate(failure, 500), "nodes": len(rp.Nodes),
		"side_effects_completed": sideEffects}
	summary := map[string]any{"mode": "flow", "status": "replan", "generation": rp.Generation,
		"plan_hash": planHash, "failure": truncate(failure, 500)}
	return domain.StepSuccess{
		OutputRef: ref, OutputHash: hash, Summary: mustJSON(summary), Flow: commit,
		Plan: []domain.PlannedStep{{Kind: domain.StepPlan, Executor: "planner", InputRef: ref, InputHash: hash,
			Status: domain.StepPending}},
		Events: append(events, domain.EmittedEvent{Type: "plan.replan_requested", Payload: payload}),
	}, true, nil
}

// replanNodes lists the nodes the failed generation reached, in spec order.
func (w *Worker) replanNodes(tenantID string, spec *flow.Spec, st *flow.State, results map[string]flow.NodeResult) []planner.ReplanNode {
	risky := map[string]bool{}
	anyRisky := false
	if w.tools != nil {
		for _, d := range w.tools.DescriptorsFor(tenantID) {
			if d.Risk.HasSideEffects() {
				risky[d.Name], anyRisky = true, true
			}
		}
	}
	var out []planner.ReplanNode
	for _, n := range spec.Nodes {
		ns := st.Nodes[n.ID]
		if ns == nil || ns.Status == "" || ns.Status == flow.NodePending {
			continue
		}
		rn := planner.ReplanNode{ID: n.ID, Type: string(n.Type), Status: string(ns.Status), Tool: n.Tool}
		switch n.Type {
		case flow.NodeTool:
			rn.SideEffect = risky[n.Tool]
		case flow.NodeAgent:
			if len(n.Tools) == 0 {
				// An agent without pinned tools could reach any visible tool.
				rn.SideEffect = anyRisky
			}
			for _, t := range n.Tools {
				rn.SideEffect = rn.SideEffect || risky[t]
			}
		case flow.NodeSubflow:
			rn.SideEffect = anyRisky
		}
		r, hasResult := results[n.ID]
		switch {
		case hasResult && r.Error != "":
			rn.Detail = "error: " + r.Error
		case ns.Error != "":
			rn.Detail = "error: " + ns.Error
		case hasResult && r.Output != nil:
			rn.Detail = excerpt(r.Output)
		case ns.Output != nil:
			rn.Detail = excerpt(ns.Output)
		}
		out = append(out, rn)
	}
	return out
}

func excerpt(v any) string {
	if s, ok := v.(string); ok {
		return truncate(s, 400)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return truncate(string(b), 400)
}
