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
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/planner"
	"go-sre-harness/internal/profile"
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
	// Generation is 0 for the first plan and n for the nth replan.
	Generation int                     `json:"generation,omitempty"`
	Injection  []planner.InjectionFlag `json:"injection,omitempty"`
	Review     *planner.Review         `json:"review,omitempty"`
}

// PlanMemorySource retrieves plan exemplars and reasoning patterns for a
// goal (implemented by memory.Retriever).
type PlanMemorySource interface {
	PlanMemories(ctx context.Context, tenantID, goal string, services []string, k int) ([]memory.PlanMemory, error)
}

// planHistoryMessages is how many recent conversation messages the planner
// may choose from; it keeps the recent ones and the relevant older ones.
const planHistoryMessages = 40

// plannerOverride converts the profile's planner section.
func plannerOverride(p *profile.PlannerSpec) *planner.Override {
	if p == nil {
		return nil
	}
	o := &planner.Override{ModelAlias: p.ModelAlias, ModelLadder: p.ModelLadder, ApprovalMode: p.ApprovalMode,
		MaxNodes: p.MaxNodes, MaxModelCalls: p.MaxModelCalls, MaxEstimatedTokens: p.MaxEstimatedTokens,
		MaxRepairs: p.MaxRepairs}
	for _, f := range p.Fallbacks {
		o.Fallbacks = append(o.Fallbacks, planner.FallbackRule{Name: f.Name, Services: f.Services,
			GoalPattern: f.GoalPattern, ProfileID: f.ProfileID, Version: f.Version})
	}
	return o
}

// maxReplans is the effective replan budget of a run: the profile may only
// lower the worker's.
func (w *Worker) maxReplans(rules profile.Rules) int {
	n := w.cfg.PlanMaxReplans
	if rules.Planner != nil && rules.Planner.MaxReplans != nil && *rules.Planner.MaxReplans < n {
		n = *rules.Planner.MaxReplans
	}
	return n
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
	// A replan step points at the failed generation's summary; an initial
	// plan step points at the goal.
	var replan *planner.Replan
	if lease.InputRef != "" && lease.InputRef != lease.GoalRef {
		var d replanDoc
		if err := w.get(ctx, lease.TenantID, lease.InputRef, &d); err != nil {
			return domain.StepSuccess{}, err
		}
		if d.Kind == replanKind {
			replan = &d.Replan
		}
	}

	var history []planner.Message
	if cr, ok := w.store.(ConversationReader); ok {
		msgs, err := cr.RunConversation(ctx, lease.TenantID, lease.RunID)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		start := 0
		if len(msgs) > planHistoryMessages {
			start = len(msgs) - planHistoryMessages
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
		res := planner.Result{Mode: "react", Rationale: "planner is not configured; using ReAct"}
		if replan != nil {
			res.Generation = replan.Generation
		}
		return w.commitPlan(ctx, lease, res, nil)
	}
	var services []string
	if g.Context != nil {
		services = domain.NormalizeServices(g.Context.Services)
	}
	exemplars, hits := w.planExemplars(ctx, lease, rules, g.Goal, services)
	scope := gw.Scope{
		TenantID: lease.TenantID, RunID: lease.RunID, StepID: lease.StepID, StepAttemptID: lease.AttemptID,
		WorkerID: lease.WorkerID, FenceToken: lease.FenceToken, ContextSnapshotID: lease.ContextSnapshotID,
	}
	res, err := w.cfg.Planner.Plan(ctx, scope, planner.Input{
		Goal: g.Goal, History: history, Capabilities: capabilities, Tools: visible,
		AllowTool: allowTool, MaxSteps: rules.MaxSteps, ModeHint: modeHint(g.Context), Replan: replan,
		Services: services, Exemplars: exemplars, Override: plannerOverride(rules.Planner),
	})
	if err != nil {
		return domain.StepSuccess{}, err
	}
	return w.commitPlan(ctx, lease, res, hits)
}

// planExemplars retrieves plan exemplars and reasoning patterns, honouring
// the profile's pattern exclusions. Retrieval failures only cost the hint.
// The returned hits are committed with the plan step, so the memories'
// effectiveness is measured like any other retrieval.
func (w *Worker) planExemplars(ctx context.Context, lease domain.Lease, rules profile.Rules, goal string, services []string) ([]planner.Exemplar, []domain.MemoryHit) {
	if w.cfg.PlanMemory == nil {
		return nil, nil
	}
	mctx := domain.WithMemoryExclude(ctx, rules.MemoryExcludeFor(lease.RunID))
	ms, err := w.cfg.PlanMemory.PlanMemories(mctx, lease.TenantID, goal, services, 2)
	if err != nil {
		w.log.Warn("planner exemplars", "err", err)
		return nil, nil
	}
	var ex []planner.Exemplar
	var hits []domain.MemoryHit
	for i, m := range ms {
		ex = append(ex, planner.Exemplar{MemoryID: m.ID, Kind: m.Kind, Title: m.Title, Content: m.Content})
		hits = append(hits, domain.MemoryHit{MemoryID: m.ID, Version: m.Version, Score: m.Score, Rank: i + 1})
	}
	return ex, hits
}

func (w *Worker) commitPlan(ctx context.Context, lease domain.Lease, res planner.Result, hits []domain.MemoryHit) (domain.StepSuccess, error) {
	planHash := res.PlanHash
	if planHash == "" {
		planHash = planner.PlanHash(res.Mode, res.Hash, res.Inputs, res.Generation)
	}
	needsReview := res.NeedsReview
	// The tenant's mode can only make the worker's stricter.
	switch strings.ToLower(planner.StricterApproval(w.cfg.PlanApprovalMode, res.ApprovalMode)) {
	case "always":
		needsReview = true
	case "never":
		needsReview = false
	default:
		if res.Analysis != nil && res.Analysis.RequiresReview {
			needsReview = true
		}
	}
	// Suspected injection in the planner's inputs always puts a human in
	// front of the generated graph, even when plan approval is disabled.
	if len(res.Injection) > 0 {
		needsReview = true
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
	if res.Generation > 0 {
		art["generation"] = res.Generation
	}
	if res.Cached {
		art["cached"] = true
	}
	if res.ModelAlias != "" {
		art["model_alias"] = res.ModelAlias
	}
	if len(res.Exemplars) > 0 {
		art["exemplars"] = res.Exemplars
	}
	if res.Review != nil {
		art["review"] = res.Review
	}
	if res.FallbackSource != "" {
		art["fallback_source"] = res.FallbackSource
	}
	if len(res.Injection) > 0 {
		art["injection"] = res.Injection
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
	if res.Generation > 0 {
		summary["generation"] = res.Generation
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
			"repairs": res.Repairs, "fallback": res.Fallback, "needs_review": needsReview,
			"generation": res.Generation},
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
	if res.Generation > 0 {
		events = append(events, domain.EmittedEvent{Type: "plan.replanned", Payload: map[string]any{
			"generation": res.Generation, "mode": res.Mode, "plan_hash": planHash}})
	}
	if len(res.Injection) > 0 {
		sources := make([]string, len(res.Injection))
		for i, f := range res.Injection {
			sources[i] = f.Source
		}
		events = append(events, domain.EmittedEvent{Type: "plan.injection_suspected", Payload: map[string]any{
			"sources": sources, "mode": res.Mode, "plan_hash": planHash, "needs_review": needsReview}})
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
			payload := map[string]any{"plan_hash": planHash, "risk": step.Intent.RiskClass,
				"approvers": step.RequiredApprovals}
			if res.Review != nil {
				payload["summary"] = truncate(res.Review.Summary, 500)
				payload["risks"] = len(res.Review.Risks)
				if res.Review.DiffBase != "" {
					payload["diff_base"], payload["diff"] = res.Review.DiffBase, len(res.Review.Diff)
				}
			}
			events = append(events, domain.EmittedEvent{Type: "plan.approval_requested", Payload: payload})
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
		Plan: plan, Thoughts: []domain.Thought{thought}, Events: events, MemoryHits: hits,
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
	eff := map[string]any{"plan_hash": planHash, "flow_hash": res.Hash,
		"nodes": analysisNodes(res.Analysis), "risk": risk, "side_effects": analysisSideEffects(res.Analysis)}
	if rv := res.Review; rv != nil {
		// What the approver sees: the readable plan, its risks and dry run,
		// and the diff against the plan it replaces or the runbook.
		eff["summary"] = rv.Summary
		eff["steps"] = capList(rv.Steps, 40)
		if len(rv.Risks) > 0 {
			eff["risks"] = capList(rv.Risks, 20)
		}
		if rv.DiffBase != "" {
			eff["diff_base"], eff["diff"] = rv.DiffBase, capList(rv.Diff, 40)
		}
		if rv.Simulation != nil {
			eff["dry_run"] = map[string]any{"completed": rv.Simulation.Completed, "path": capList(rv.Simulation.Path, 40),
				"approvals": rv.Simulation.Approvals, "side_effects": rv.Simulation.SideEffects}
		}
	}
	effects, _ := json.Marshal(eff)
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
		planHash = planner.PlanHash(plan.Mode, plan.FlowHash, plan.Input, plan.Generation)
	}
	if planHash != lease.InputHash {
		return domain.StepSuccess{}, fmt.Errorf("plan changed after approval: approved %s, found %s", lease.InputHash, planHash)
	}
	if got := planner.PlanHash(plan.Mode, plan.FlowHash, plan.Input, plan.Generation); got != planHash {
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

// modeHint returns the routing hint pinned in the goal context.
func modeHint(gc *domain.GoalContext) string {
	if gc == nil {
		return ""
	}
	return gc.Mode
}

func capList(xs []string, n int) []string {
	if len(xs) <= n {
		return xs
	}
	return append(append([]string(nil), xs[:n]...), fmt.Sprintf("… %d more", len(xs)-n))
}
