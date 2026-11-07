package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/planner"
)

// planArtifact is the durable output of a plan step. A flow step points its
// InputRef at it, so the generated graph is pinned and replayable.
type planArtifact struct {
	Mode             string            `json:"mode"`
	Rationale        string            `json:"rationale"`
	Flow             json.RawMessage   `json:"flow,omitempty"`
	FlowHash         string            `json:"flow_hash,omitempty"`
	PlanHash         string            `json:"plan_hash,omitempty"`
	Input            map[string]any    `json:"input,omitempty"`
	Analysis         *planner.Analysis `json:"analysis,omitempty"`
	NeedsReview      bool              `json:"needs_review,omitempty"`
	ReviewRisk       string            `json:"review_risk,omitempty"`
	ModelRequestID   string            `json:"model_request_id,omitempty"`
	ManifestRef      string            `json:"manifest_ref,omitempty"`
	ResponseRef      string            `json:"response_ref,omitempty"`
	Repairs          int               `json:"repairs,omitempty"`
	ValidationErrors []string          `json:"validation_errors,omitempty"`
	Fallback         bool              `json:"fallback,omitempty"`
}

// executePlan turns one goal into either a generated flow step or a normal
// ReAct loop. Planning runs as a leased step, so model usage, retries, fences
// and audit all use the existing kernel machinery.
func (w *Worker) executePlan(ctx context.Context, lease domain.Lease) (domain.StepSuccess, error) {
	var g goalDoc
	if err := w.get(ctx, lease.TenantID, lease.GoalRef, &g); err != nil {
		return domain.StepSuccess{}, err
	}
	rules, err := w.rules(ctx, lease)
	if err != nil {
		return domain.StepSuccess{}, err
	}

	var history []planner.Message
	if cr, ok := w.store.(ConversationReader); ok {
		msgs, err := cr.RunConversation(ctx, lease.TenantID, lease.RunID)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		start := 0
		if len(msgs) > 8 {
			start = len(msgs) - 8
		}
		for _, m := range msgs[start:] {
			text := strings.TrimSpace(w.messageText(ctx, lease.TenantID, m))
			if text != "" {
				history = append(history, planner.Message{Role: m.Role, Text: text})
			}
		}
	}

	var capabilities []string
	if w.cfg.CapabilityMap != nil {
		if lines, err := w.cfg.CapabilityMap.CapabilityMap(ctx, lease.TenantID, g.Goal, 6); err == nil {
			capabilities = lines
		} else {
			w.log.Warn("planner capability map", "err", err)
		}
	}

	visible := rules.Visible(w.tools.DescriptorsFor(lease.TenantID))
	off := map[string]bool{}
	if w.cfg.ToolToggles != nil {
		var err error
		off, err = w.cfg.ToolToggles.DisabledTools(ctx, lease.TenantID)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		visible = withoutDisabled(visible, off)
	}
	allowTool := rules.AllowsTool
	if len(off) > 0 {
		allowTool = func(name string) bool { return rules.AllowsTool(name) && !off[name] }
	}

	if w.cfg.Planner == nil {
		return w.commitPlan(ctx, lease, planner.Result{Mode: "react", Rationale: "planner is not configured; using ReAct"})
	}
	scope := gw.Scope{
		TenantID: lease.TenantID, RunID: lease.RunID, StepID: lease.StepID, StepAttemptID: lease.AttemptID,
		WorkerID: lease.WorkerID, FenceToken: lease.FenceToken, ContextSnapshotID: lease.ContextSnapshotID,
	}
	res, err := w.cfg.Planner.Plan(ctx, scope, planner.Input{
		Goal: g.Goal, History: history, Capabilities: capabilities, Tools: visible,
		AllowTool: allowTool, MaxSteps: rules.MaxSteps,
	})
	if err != nil {
		return domain.StepSuccess{}, err
	}
	return w.commitPlan(ctx, lease, res)
}

func (w *Worker) commitPlan(ctx context.Context, lease domain.Lease, res planner.Result) (domain.StepSuccess, error) {
	planHash := res.PlanHash
	if planHash == "" {
		planHash = planner.PlanHash(res.Mode, res.Hash, res.Inputs)
	}
	needsReview := res.NeedsReview
	switch strings.ToLower(w.cfg.PlanApprovalMode) {
	case "always":
		needsReview = true
	case "never":
		needsReview = false
	default:
		if res.Analysis != nil && res.Analysis.RequiresReview {
			needsReview = true
		}
	}
	if res.Mode != "flow" {
		needsReview = false
	}
	reviewRisk := res.ReviewRisk
	if reviewRisk == "" && res.Analysis != nil {
		reviewRisk = res.Analysis.ReviewRisk()
	}
	art := map[string]any{
		"mode": res.Mode, "rationale": res.Rationale,
		"model_request_id": res.ModelRequestID, "manifest_ref": res.ManifestRef, "response_ref": res.ResponseRef,
		"repairs": res.Repairs, "validation_errors": res.ValidationErrors, "fallback": res.Fallback,
		"plan_hash": planHash, "needs_review": needsReview, "review_risk": reviewRisk,
	}
	if res.Analysis != nil {
		art["analysis"] = res.Analysis
	}
	if res.Mode == "flow" {
		if len(res.Canonical) > 0 {
			art["flow"] = json.RawMessage(res.Canonical)
		} else if res.Flow != nil {
			art["flow"] = res.Flow
		}
		art["flow_hash"] = res.Hash
		if len(res.Inputs) > 0 {
			art["input"] = res.Inputs
		}
	}
	ref, hash, err := w.put(ctx, lease.TenantID, "plan", art)
	if err != nil {
		return domain.StepSuccess{}, err
	}

	summary := map[string]any{
		"mode": res.Mode, "rationale": truncate(res.Rationale, 500),
		"repairs": res.Repairs, "fallback": res.Fallback, "plan_hash": planHash,
		"needs_review": needsReview,
	}
	if res.Analysis != nil {
		summary["risk"] = res.Analysis.HighestRisk
		summary["side_effects"] = res.Analysis.SideEffects
		summary["estimated_runs"] = res.Analysis.EstimatedRuns
		summary["estimated_model_calls"] = res.Analysis.EstimatedModelCalls
		summary["findings"] = len(res.Analysis.Findings)
	}
	if res.Mode == "flow" {
		summary["flow_hash"] = res.Hash
		if res.Flow != nil {
			summary["nodes"] = len(res.Flow.Nodes)
		}
	}
	if res.ModelRequestID != "" {
		summary["model_request_id"] = res.ModelRequestID
	}
	rationale := strings.TrimSpace(res.Rationale)
	if rationale == "" {
		rationale = "Planned orchestration"
	}
	thought := domain.Thought{
		Kind: domain.ThoughtPlan, Summary: rationale, DetailRef: ref, Status: domain.ThoughtOK, Structured: true,
		Attrs: map[string]any{"mode": res.Mode, "flow_hash": res.Hash, "plan_hash": planHash,
			"repairs": res.Repairs, "fallback": res.Fallback, "needs_review": needsReview},
	}
	events := []domain.EmittedEvent{{Type: "plan.generated", Payload: summary}}
	if len(res.ValidationErrors) > 0 {
		events = append(events, domain.EmittedEvent{Type: "plan.validation_failed", Payload: map[string]any{
			"errors": len(res.ValidationErrors), "last": truncate(strings.Join(res.ValidationErrors, "; "), 500)}})
	}
	if res.Repairs > 0 {
		events = append(events, domain.EmittedEvent{Type: "plan.repaired", Payload: map[string]any{
			"repairs": res.Repairs, "plan_hash": planHash}})
	}
	if res.Fallback {
		events = append(events, domain.EmittedEvent{Type: "plan.fallback", Payload: map[string]any{
			"mode": res.Mode, "flow_hash": res.Hash}})
	}

	var plan []domain.PlannedStep
	switch res.Mode {
	case "flow":
		if res.Flow == nil {
			return domain.StepSuccess{}, fmt.Errorf("planner returned flow mode without a flow spec")
		}
		if needsReview {
			step, err := w.planReviewStep(lease, ref, planHash, res)
			if err != nil {
				return domain.StepSuccess{}, err
			}
			plan = []domain.PlannedStep{step}
			events = append(events, domain.EmittedEvent{Type: "plan.approval_requested", Payload: map[string]any{
				"plan_hash": planHash, "risk": step.Intent.RiskClass, "approvers": step.RequiredApprovals}})
		} else {
			plan = []domain.PlannedStep{{Kind: domain.StepFlow, Executor: "flow", InputRef: ref, InputHash: hash,
				Status: domain.StepPending}}
		}
	case "react":
		if w.cfg.Memory != nil {
			plan = append(plan, domain.PlannedStep{Kind: domain.StepRetrieve, Executor: "memory",
				InputRef: lease.GoalRef, InputHash: lease.InputHash, Status: domain.StepPending})
		}
		plan = append(plan, domain.PlannedStep{Kind: domain.StepReason, Executor: "reasoner",
			InputRef: lease.GoalRef, InputHash: lease.InputHash, Status: domain.StepPending})
	default:
		return domain.StepSuccess{}, fmt.Errorf("planner returned unknown mode %q", res.Mode)
	}

	return domain.StepSuccess{
		OutputRef: ref, OutputHash: hash, ArtifactKind: "plan", Summary: mustJSON(summary),
		Plan: plan, Thoughts: []domain.Thought{thought}, Events: events,
	}, nil
}

// planReviewStep turns the generated plan into a human approval gate. The
// intent binds the plan hash, so any change to the graph, inputs or rationale
// invalidates the approval.
func (w *Worker) planReviewStep(lease domain.Lease, planRef, planHash string, res planner.Result) (domain.PlannedStep, error) {
	risk := res.ReviewRisk
	if risk == "" && res.Analysis != nil {
		risk = res.Analysis.ReviewRisk()
	}
	if risk == "" {
		risk = domain.RiskR2
	}
	approvers := w.cfg.PlanApprovers
	if approvers <= 0 {
		approvers = 1
	}
	if risk == domain.RiskR3 && approvers < 2 {
		approvers = 2
	}
	effects, _ := json.Marshal(map[string]any{"plan_hash": planHash, "flow_hash": res.Hash,
		"nodes": analysisNodes(res.Analysis), "risk": risk, "side_effects": analysisSideEffects(res.Analysis)})
	intent := domain.ActionIntent{
		Environment: "plan", Target: "plan:" + strings.TrimPrefix(planHash, "sha256:"), ToolName: "plan.review",
		ToolVersion: "1", InputHash: planHash,
		ActionHash: artifact.Hash([]byte(lease.TenantID + "\x00" + lease.RunID + "\x00" + planHash)),
		RiskClass:  domain.RiskClass(risk), PolicyVersion: "plan", Effects: effects,
	}
	pd, _ := json.Marshal(map[string]any{"verdict": "require_approval", "risk": risk,
		"reason": "generated plan review", "plan_hash": planHash})
	return domain.PlannedStep{Kind: domain.StepPlanReview, Executor: "planner", InputRef: planRef,
		InputHash: planHash, Status: domain.StepWaitingApproval, Intent: &intent,
		RequiredApprovals: approvers, PolicyDecision: pd}, nil
}

func analysisNodes(a *planner.Analysis) int {
	if a == nil {
		return 0
	}
	return a.Nodes
}

func analysisSideEffects(a *planner.Analysis) int {
	if a == nil {
		return 0
	}
	return a.SideEffects
}

// executePlanReview runs only after the plan approval was consumed by the
// claim path. It re-verifies the approved hash against the immutable artifact
// and then appends the flow step.
func (w *Worker) executePlanReview(ctx context.Context, lease domain.Lease) (domain.StepSuccess, error) {
	if lease.Intent == nil {
		return domain.StepSuccess{}, fmt.Errorf("plan review step without an approval intent")
	}
	var plan planArtifact
	if err := w.get(ctx, lease.TenantID, lease.InputRef, &plan); err != nil {
		return domain.StepSuccess{}, err
	}
	if plan.Mode != "flow" || len(plan.Flow) == 0 {
		return domain.StepSuccess{}, fmt.Errorf("plan review artifact is not a flow plan")
	}
	planHash := plan.PlanHash
	if planHash == "" {
		planHash = planner.PlanHash(plan.Mode, plan.FlowHash, plan.Input)
	}
	if planHash != lease.InputHash {
		return domain.StepSuccess{}, fmt.Errorf("plan changed after approval: approved %s, found %s", lease.InputHash, planHash)
	}
	if got := planner.PlanHash(plan.Mode, plan.FlowHash, plan.Input); got != planHash {
		return domain.StepSuccess{}, fmt.Errorf("plan artifact content does not match plan hash %s", planHash)
	}
	if lease.Intent.InputHash != planHash {
		return domain.StepSuccess{}, fmt.Errorf("plan approval does not bind the plan hash")
	}
	wantAction := artifact.Hash([]byte(lease.TenantID + "\x00" + lease.RunID + "\x00" + planHash))
	if lease.Intent.ActionHash != wantAction {
		return domain.StepSuccess{}, fmt.Errorf("plan approval action hash mismatch")
	}
	rules, err := w.rules(ctx, lease)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	_, _, flowHash, err := flow.Compile(plan.Flow, flow.CompileOptions{AllowTool: rules.AllowsTool})
	if err != nil {
		return domain.StepSuccess{}, fmt.Errorf("approved plan no longer compiles: %w", err)
	}
	if plan.FlowHash != "" && flowHash != plan.FlowHash {
		return domain.StepSuccess{}, fmt.Errorf("approved plan flow hash mismatch")
	}
	summary := map[string]any{"mode": "flow", "plan_hash": planHash, "flow_hash": flowHash, "approved": true}
	return domain.StepSuccess{
		OutputRef: lease.InputRef, OutputHash: lease.InputHash, Summary: mustJSON(summary),
		Plan: []domain.PlannedStep{{Kind: domain.StepFlow, Executor: "flow", InputRef: lease.InputRef,
			InputHash: lease.InputHash, Status: domain.StepPending}},
		Events: []domain.EmittedEvent{{Type: "plan.approved", Payload: summary}},
	}, nil
}
