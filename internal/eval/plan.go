package eval

// Plan datasets record how an orchestration plan was produced and what it did.
// Replay compiles the pinned plan, re-runs its static analysis, and (for
// acyclic graphs) drives the flow state machine with the recorded node results.
// Offline replay never calls a tool and never approves a write.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/planner"
	"go-sre-harness/internal/tools"
)

// CostSummary mirrors the fields the harness exposes for one run.
type CostSummary struct {
	ModelRequests     int     `json:"model_requests"`
	InputTokens       int64   `json:"input_tokens"`
	OutputTokens      int64   `json:"output_tokens"`
	CachedInputTokens int64   `json:"cached_input_tokens"`
	CostMicros        int64   `json:"cost_micros"`
	CostUSD           float64 `json:"cost_usd"`
}

// PlanStepRecord is one step of the source run.
type PlanStepRecord struct {
	NodeID  string `json:"node_id,omitempty"`
	Kind    string `json:"kind"`
	Status  string `json:"status"`
	Attempt int    `json:"attempt,omitempty"`
}

// PlanApprovalRecord is one approval decision the plan needed.
type PlanApprovalRecord struct {
	Target   string `json:"target"`
	Risk     string `json:"risk_class"`
	Status   string `json:"status"`
	Required int    `json:"required_approvals"`
	Hash     string `json:"action_hash,omitempty"`
}

// PlanTrace is the replayable part of a run's plan.
type PlanTrace struct {
	Version       int                  `json:"version"`
	Goal          string               `json:"goal"`
	Mode          string               `json:"mode"`
	Variant       string               `json:"variant"`
	Plan          json.RawMessage      `json:"plan,omitempty"`
	PlanHash      string               `json:"plan_hash,omitempty"`
	FlowHash      string               `json:"flow_hash,omitempty"`
	Inputs        map[string]any       `json:"inputs,omitempty"`
	Analysis      json.RawMessage      `json:"analysis,omitempty"`
	Fallback      bool                 `json:"fallback,omitempty"`
	NeedsReview   bool                 `json:"needs_review,omitempty"`
	Repairs       int                  `json:"repairs,omitempty"`
	FlowState     json.RawMessage      `json:"flow_state,omitempty"`
	Steps         []PlanStepRecord     `json:"steps,omitempty"`
	Approvals     []PlanApprovalRecord `json:"approvals,omitempty"`
	Outcome       string               `json:"outcome"`
	FailureReason string               `json:"failure_reason,omitempty"`
	DurationMS    int64                `json:"duration_ms"`
	Cost          CostSummary          `json:"cost"`
}

// PlanExpectation is the ground truth stored with a plan case.
type PlanExpectation struct {
	Mode      string `json:"mode,omitempty"`
	Variant   string `json:"variant,omitempty"`
	PlanHash  string `json:"plan_hash,omitempty"`
	FlowHash  string `json:"flow_hash,omitempty"`
	Succeeded bool   `json:"succeeded"`
	MaxRisk   string `json:"max_risk,omitempty"`
	// ApprovalRequired is true when the recorded analysis required a plan
	// review. Replay marks a case unsafe if no approval was recorded.
	ApprovalRequired bool `json:"approval_required,omitempty"`
}

// PlanCaseResult is one replayed plan.
type PlanCaseResult struct {
	Key         string   `json:"case_key"`
	Variant     string   `json:"variant"`
	Mode        string   `json:"mode"`
	Compiled    bool     `json:"compiled"`
	HashStable  bool     `json:"hash_stable"`
	Safe        bool     `json:"safe"`
	ReplayRan   bool     `json:"replay_ran"`
	Completed   bool     `json:"completed"`
	Steps       int      `json:"steps"`
	DurationMS  int64    `json:"duration_ms"`
	CostUSD     float64  `json:"cost_usd"`
	UnsafeTools []string `json:"unsafe_tools,omitempty"`
	Warnings    []string `json:"warnings,omitempty"`
	Error       string   `json:"error,omitempty"`
}

// VariantSummary aggregates the comparison between ReAct, hand-authored
// runbooks and generated flows.
type VariantSummary struct {
	Cases         int     `json:"cases"`
	Compiled      int     `json:"compiled"`
	HashStable    int     `json:"hash_stable"`
	Safe          int     `json:"safe"`
	ReplayRan     int     `json:"replay_ran"`
	Completed     int     `json:"completed"`
	AvgSteps      float64 `json:"avg_steps"`
	AvgDurationMS float64 `json:"avg_duration_ms"`
	AvgCostUSD    float64 `json:"avg_cost_usd"`
}

// PlanReplayReport is returned by ReplayPlans and uploaded as an artifact so
// a release decision can cite the exact report.
type PlanReplayReport struct {
	DatasetID string                    `json:"dataset_id"`
	Split     string                    `json:"split"`
	Cases     []PlanCaseResult          `json:"cases"`
	ByVariant map[string]VariantSummary `json:"by_variant"`
	ReportRef string                    `json:"report_ref,omitempty"`
}

// RecordPlanFromRun turns a terminal run into a plan case. It keeps the plan
// artifact, the planner-produced analysis, the flow state, approvals and the
// run's outcome. Ground truth comes from the run itself (success/failure).
func (s *Service) RecordPlanFromRun(ctx context.Context, tenantID, datasetID, runID, split string) (Case, bool, error) {
	if split != "" && split != "train" && split != "holdout" {
		return Case{}, false, fmt.Errorf("%w: split must be train or holdout", ErrInvalid)
	}
	run, err := s.Store.GetRun(ctx, tenantID, runID)
	if err != nil {
		return Case{}, false, err
	}
	if !run.Status.Terminal() {
		return Case{}, false, fmt.Errorf("%w: run is not finished", ErrInvalid)
	}
	var goal struct {
		Goal string `json:"goal"`
	}
	if data, err := s.Artifacts.Get(ctx, tenantID, run.GoalRef); err == nil {
		_ = json.Unmarshal(data, &goal)
	}
	steps, err := s.Store.ListSteps(ctx, tenantID, runID)
	if err != nil {
		return Case{}, false, err
	}
	trace := PlanTrace{Version: 1, Goal: goal.Goal, Outcome: string(run.Status),
		DurationMS: run.UpdatedAt.Sub(run.CreatedAt).Milliseconds()}
	if run.FailureReason != nil {
		trace.FailureReason = *run.FailureReason
	}
	for _, st := range steps {
		if st.Kind == domain.StepPlan && st.OutputRef != nil {
			data, err := s.Artifacts.Get(ctx, tenantID, *st.OutputRef)
			if err == nil {
				var plan struct {
					Mode        string          `json:"mode"`
					Flow        json.RawMessage `json:"flow"`
					FlowHash    string          `json:"flow_hash"`
					PlanHash    string          `json:"plan_hash"`
					Input       map[string]any  `json:"input"`
					Analysis    json.RawMessage `json:"analysis"`
					Fallback    bool            `json:"fallback"`
					NeedsReview bool            `json:"needs_review"`
					Repairs     int             `json:"repairs"`
				}
				if json.Unmarshal(data, &plan) == nil {
					trace.Mode, trace.Plan, trace.FlowHash, trace.PlanHash = plan.Mode, data, plan.FlowHash, plan.PlanHash
					trace.Inputs, trace.Analysis, trace.Fallback, trace.Repairs = plan.Input, plan.Analysis, plan.Fallback, plan.Repairs
					trace.NeedsReview = plan.NeedsReview
				}
			}
		}
		rec := PlanStepRecord{Kind: string(st.Kind), Status: string(st.Status), Attempt: st.CurrentAttempt,
			NodeID: st.FlowNodeID}
		trace.Steps = append(trace.Steps, rec)
	}
	if trace.Mode == "" {
		for _, st := range steps {
			if st.Kind == domain.StepFlow {
				trace.Mode = "flow"
				break
			}
		}
		if trace.Mode == "" {
			trace.Mode = "react"
		}
	}
	switch {
	case trace.Mode == "react":
		trace.Variant = "react"
	case trace.Fallback:
		trace.Variant = "fallback_flow"
	case trace.PlanHash != "":
		trace.Variant = "generated_flow"
	default:
		trace.Variant = "profile_flow"
	}
	if snap, err := s.Store.LoadFlow(ctx, tenantID, runID); err == nil {
		trace.FlowState = snap.State
		if trace.FlowHash == "" {
			trace.FlowHash = snap.CompiledHash
		}
	}
	approvals, _ := s.Store.ListApprovals(ctx, tenantID, runID)
	for _, a := range approvals {
		trace.Approvals = append(trace.Approvals, PlanApprovalRecord{Target: a.Target, Risk: string(a.RiskClass),
			Status: a.Status, Required: a.RequiredApprovals, Hash: a.ActionHash})
	}
	if cost, err := s.Store.EvalRunCost(ctx, tenantID, runID); err == nil {
		trace.Cost = cost
	}

	gt := GroundTruth{
		RootCauseLabels: []string{},
		Plan: &PlanExpectation{Mode: trace.Mode, Variant: trace.Variant, PlanHash: trace.PlanHash,
			FlowHash: trace.FlowHash, Succeeded: run.Status == domain.RunCompleted,
			MaxRisk: traceHighestRisk(trace.Analysis)},
	}
	gt.Plan.ApprovalRequired = planNeedsApproval(trace)
	data, _ := json.Marshal(Recording{Goal: trace.Goal, Plan: &trace})
	redacted, _ := memory.Redact(string(data))
	ref, _, err := s.Artifacts.Put(ctx, tenantID, "plan-recording", []byte(redacted))
	if err != nil {
		return Case{}, false, err
	}
	var allowed []string
	if s.Registry != nil {
		for _, d := range s.Registry.Descriptors() {
			allowed = append(allowed, d.Name)
		}
	}
	key := "plan:" + runID
	if split == "" {
		split = SplitFor(key)
	}
	c := Case{Key: key, Split: split, ServiceID: orUnknown(memory.ServiceFromGoal(trace.Goal)), Environment: "production",
		Goal: trace.Goal, RecordingRef: ref, GroundTruth: gt, AllowedTools: allowed,
		OccurredAt: run.CreatedAt, SourceRunID: &run.ID}
	return s.Store.AddCase(ctx, tenantID, datasetID, c)
}

// ReplayPlans deterministically checks every recorded plan in a frozen split.
// It does not call a model or a tool.
func (s *Service) ReplayPlans(ctx context.Context, tenantID, datasetID, split string) (PlanReplayReport, error) {
	ds, err := s.Store.GetDataset(ctx, tenantID, datasetID)
	if err != nil {
		return PlanReplayReport{}, err
	}
	if ds.Status != "FROZEN" {
		return PlanReplayReport{}, fmt.Errorf("%w: dataset must be frozen before replay", ErrInvalid)
	}
	cases, err := s.Store.ListCases(ctx, tenantID, datasetID, split)
	if err != nil {
		return PlanReplayReport{}, err
	}
	report := PlanReplayReport{DatasetID: datasetID, Split: split, ByVariant: map[string]VariantSummary{}}
	for _, c := range cases {
		if c.Recording.Plan == nil && c.RecordingRef != "" && s.Artifacts != nil {
			if data, err := s.Artifacts.Get(ctx, tenantID, c.RecordingRef); err == nil {
				_ = json.Unmarshal(data, &c.Recording)
			}
		}
		if c.Recording.Plan == nil {
			continue
		}
		r := s.replayPlanCase(c)
		report.Cases = append(report.Cases, r)
	}
	sort.Slice(report.Cases, func(i, j int) bool { return report.Cases[i].Key < report.Cases[j].Key })
	report.ByVariant = summarizePlans(report.Cases)
	if s.Artifacts != nil {
		if data, err := json.Marshal(report); err == nil {
			if ref, _, err := s.Artifacts.Put(ctx, tenantID, "plan-replay-report", data); err == nil {
				report.ReportRef = ref
			}
		}
	}
	return report, nil
}

func (s *Service) replayPlanCase(c Case) PlanCaseResult {
	p := c.Recording.Plan
	r := PlanCaseResult{Key: c.Key, Variant: p.Variant, Mode: p.Mode, DurationMS: p.DurationMS, CostUSD: p.Cost.CostUSD}
	if p.Variant == "" && c.GroundTruth.Plan != nil {
		r.Variant = c.GroundTruth.Plan.Variant
	}
	if p.Mode == "" && c.GroundTruth.Plan != nil {
		r.Mode = c.GroundTruth.Plan.Mode
	}
	var plan struct {
		Flow json.RawMessage `json:"flow"`
	}
	if len(p.Plan) > 0 {
		_ = json.Unmarshal(p.Plan, &plan)
	}
	var spec flow.Spec
	if p.Mode == "flow" && len(plan.Flow) == 0 {
		r.Error = "plan artifact has no flow"
		return r
	}
	if p.Mode == "flow" && len(plan.Flow) > 0 {
		allowed := map[string]bool{}
		for _, t := range c.AllowedTools {
			allowed[t] = true
		}
		allow := func(name string) bool { return len(allowed) == 0 || allowed[name] }
		fs, _, hash, err := flow.Compile(plan.Flow, flow.CompileOptions{AllowTool: allow})
		if err != nil {
			r.Error = "compile: " + err.Error()
			return r
		}
		r.Compiled = true
		r.HashStable = p.FlowHash == "" || hash == p.FlowHash
		spec = fs
		var descs []tools.Descriptor
		if s.Registry != nil {
			descs = s.Registry.Descriptors()
		}
		analysis := planner.Analyze(fs, planner.AnalysisOptions{Tools: descs, Budget: permissiveBudget()})
		approved := hasPlanApproval(p.Approvals)
		r.Safe = len(analysis.Errors()) == 0 && (!analysis.RequiresReview || approved)
		for _, u := range analysis.Tools {
			if mustNot(c.GroundTruth.MustNotCall, u.Tool) {
				r.UnsafeTools = append(r.UnsafeTools, u.Tool)
				r.Safe = false
			}
		}
		r.Warnings = analysis.Warnings()
	} else {
		// ReAct plans have no graph to replay; they are still valid cases.
		r.Compiled, r.HashStable, r.Safe = true, true, true
	}
	if p.Mode == "flow" && len(p.FlowState) > 0 && !hasLoop(spec) {
		completed, steps, err := replayFlow(spec, p)
		r.ReplayRan = true
		r.Completed, r.Steps = completed, steps
		if err != nil {
			r.Error = err.Error()
		}
	} else if p.Mode == "flow" {
		r.Steps = len(p.Steps)
		if c.GroundTruth.Plan != nil {
			r.Completed = c.GroundTruth.Plan.Succeeded
		}
		if len(p.FlowState) == 0 {
			r.Error = "replay skipped: no recorded flow state"
		} else {
			r.Error = "replay skipped: bounded loops cannot be reconstructed from one recorded state"
		}
	} else {
		r.Steps = len(p.Steps)
		if c.GroundTruth.Plan != nil {
			r.Completed = c.GroundTruth.Plan.Succeeded
		}
	}
	return r
}

func replayFlow(spec flow.Spec, p *PlanTrace) (bool, int, error) {
	var st flow.State
	if err := json.Unmarshal(p.FlowState, &st); err != nil {
		return false, 0, fmt.Errorf("decode recorded flow state: %w", err)
	}
	inputs := p.Inputs
	if inputs == nil {
		inputs = map[string]any{"goal": p.Goal}
	}
	state, err := flow.NewState(spec, inputs, 0)
	if err != nil {
		return false, 0, err
	}
	results := map[string]flow.NodeResult{}
	steps := 0
	for i := 0; i < 1000; i++ {
		action, err := flow.Step(spec, state, results)
		results = map[string]flow.NodeResult{}
		if err != nil {
			return false, steps, err
		}
		switch action.Kind {
		case flow.ActionAgent, flow.ActionTool, flow.ActionSubflow:
			steps++
			for _, child := range action.Children {
				res, ok := recordedResult(&st, child.NodeID)
				if !ok {
					return false, steps, fmt.Errorf("missing recorded result for node %q", child.NodeID)
				}
				results[child.NodeID] = res
			}
		case flow.ActionParallel:
			steps++
			for _, child := range action.Children {
				res, ok := recordedResult(&st, child.NodeID)
				if !ok {
					return false, steps, fmt.Errorf("missing recorded result for branch %q", child.NodeID)
				}
				results[child.NodeID] = res
			}
		case flow.ActionApproval:
			steps++
			res, ok := recordedResult(&st, action.Node.ID)
			if !ok {
				return false, steps, fmt.Errorf("missing recorded approval for node %q", action.Node.ID)
			}
			if res.Status == string(flow.NodeSucceeded) || res.Approved {
				results[action.Node.ID] = flow.NodeResult{Status: "SUCCEEDED", Approved: true}
			} else {
				results[action.Node.ID] = flow.NodeResult{Status: "FAILED", Rejected: true, Error: "approval rejected"}
			}
		case flow.ActionInput:
			steps++
			res, ok := recordedResult(&st, action.Node.ID)
			if !ok || res.Status != string(flow.NodeSucceeded) {
				return false, steps, fmt.Errorf("missing recorded input for node %q", action.Node.ID)
			}
			results[action.Node.ID] = flow.NodeResult{Status: "SUCCEEDED", InputSet: true, Input: res.Output}
		case flow.ActionWait:
			for _, id := range action.WaitFor {
				if res, ok := recordedResult(&st, id); ok {
					results[id] = res
				}
			}
		case flow.ActionComplete:
			return true, steps, nil
		case flow.ActionFail:
			return false, steps, nil
		}
	}
	return false, steps, errors.New("replay exceeded the transition budget")
}

func recordedResult(st *flow.State, id string) (flow.NodeResult, bool) {
	if st == nil || st.Nodes == nil {
		return flow.NodeResult{}, false
	}
	ns := st.Nodes[id]
	if ns == nil {
		return flow.NodeResult{}, false
	}
	return flow.NodeResult{Status: string(ns.Status), Output: ns.Output, Error: ns.Error,
		Approved: ns.Approved, Rejected: ns.Status == flow.NodeFailed}, true
}

func hasLoop(spec flow.Spec) bool {
	for i := range spec.Nodes {
		if spec.Nodes[i].Type == flow.NodeLoop {
			return true
		}
		if spec.Nodes[i].Type == flow.NodeSubflow && spec.Nodes[i].Flow != nil && hasLoop(*spec.Nodes[i].Flow) {
			return true
		}
	}
	return false
}

func hasPlanApproval(as []PlanApprovalRecord) bool {
	for _, a := range as {
		if strings.HasPrefix(a.Target, "plan:") && (a.Status == "APPROVED" || a.Status == "CONSUMED") {
			return true
		}
	}
	return false
}

func planNeedsApproval(p PlanTrace) bool {
	if p.Mode != "flow" {
		return false
	}
	if p.NeedsReview {
		return true
	}
	var a planner.Analysis
	if len(p.Analysis) > 0 && json.Unmarshal(p.Analysis, &a) == nil {
		return a.RequiresReview
	}
	return false
}

func traceHighestRisk(raw json.RawMessage) string {
	var a planner.Analysis
	if len(raw) > 0 && json.Unmarshal(raw, &a) == nil {
		return string(a.HighestRisk)
	}
	return ""
}

func permissiveBudget() planner.Budget {
	return planner.Budget{MaxNodes: 200, MaxChildren: flow.MaxAllowedChildren, MaxTotalRuns: flow.MaxAllowedTotalRuns,
		MaxDepth: flow.MaxAllowedDepth, MaxIterations: flow.MaxAllowedIterations, MaxModelCalls: 1000}
}

func mustNot(xs []string, name string) bool {
	for _, x := range xs {
		if x == name {
			return true
		}
	}
	return false
}

func summarizePlans(cases []PlanCaseResult) map[string]VariantSummary {
	type acc struct {
		VariantSummary
		stepSum, durationSum, costSum float64
	}
	out := map[string]acc{}
	for _, c := range cases {
		v := c.Variant
		if v == "" {
			v = "unknown"
		}
		a := out[v]
		a.Cases++
		if c.Compiled {
			a.Compiled++
		}
		if c.HashStable {
			a.HashStable++
		}
		if c.Safe {
			a.Safe++
		}
		if c.ReplayRan {
			a.ReplayRan++
		}
		if c.Completed {
			a.Completed++
		}
		a.stepSum += float64(c.Steps)
		a.durationSum += float64(c.DurationMS)
		a.costSum += c.CostUSD
		out[v] = a
	}
	res := map[string]VariantSummary{}
	for v, a := range out {
		if a.Cases > 0 {
			a.AvgSteps = a.stepSum / float64(a.Cases)
			a.AvgDurationMS = a.durationSum / float64(a.Cases)
			a.AvgCostUSD = a.costSum / float64(a.Cases)
		}
		res[v] = a.VariantSummary
	}
	return res
}
