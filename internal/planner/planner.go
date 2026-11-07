package planner

// Package planner turns a goal into an explicit orchestration plan. The model
// proposes a flow DSL through the submit_flow tool; the plan is compiled and
// validated here before the kernel is allowed to execute it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/tools"
)

// SubmitFlowTool is the pseudo-tool through which the planner submits its
// plan. It is never executed by the tool executor.
const SubmitFlowTool = "submit_flow"

// ErrNoPlan is returned when the planner cannot produce any usable plan and
// fallback is disabled.
var ErrNoPlan = errors.New("planner could not produce a valid plan")

// Generator is the part of the model gateway the planner needs.
type Generator interface {
	Generate(ctx context.Context, scope gw.Scope, req gw.Request, sink gw.Sink) (gw.Response, error)
}

// ProfileLoader reads an immutable profile version for the fallback template.
type ProfileLoader interface {
	ProfileSpec(ctx context.Context, tenantID, profileID string, version int64) (json.RawMessage, error)
}

// Config controls the planning call and its fallbacks.
type Config struct {
	ModelAlias      string
	Effort          string
	MaxRepairs      int
	MaxNodes        int
	MaxOutputTokens int64
	// Budget bounds the generated graph and its worst-case execution.
	MaxChildren        int
	MaxTotalRuns       int
	MaxDepth           int
	MaxIterations      int
	MaxModelCalls      int
	MaxEstimatedTokens int64
	// ApprovalMode is auto (review plans with side effects), always or never.
	ApprovalMode      string
	ApprovalApprovers int
	// FallbackProfileID/Version name the hand-authored flow used when the
	// model cannot produce a valid plan (for example with the scripted model).
	FallbackProfileID string
	FallbackVersion   int64
	FallbackReact     bool
}

// Observer receives low-cardinality planning metrics.
type Observer interface {
	ObservePlan(mode, outcome, risk string, d time.Duration, repairs int, fallback bool)
	ObservePlanValidation(reason string)
	ObservePlanFallback(to string)
}

// Message is one prior conversation message shown to the planner.
type Message struct {
	Role string
	Text string
}

// Input is the bounded context the kernel hands to the planner.
type Input struct {
	Goal         string
	History      []Message
	Capabilities []string
	Tools        []tools.Descriptor
	AllowTool    func(string) bool
	MaxNodes     int
	MaxSteps     int
}

// Result is a validated plan. Mode is "react" or "flow"; Flow is set only for
// flow plans.
type Result struct {
	Mode             string
	Flow             *flow.Spec
	Canonical        []byte
	Hash             string // flow hash (flow plans only)
	PlanHash         string // binds the plan approval
	Rationale        string
	Inputs           map[string]any
	Analysis         *Analysis
	NeedsReview      bool
	ReviewRisk       domain.RiskClass
	ModelRequestID   string
	ManifestRef      string
	ResponseRef      string
	Repairs          int
	ValidationErrors []string
	Fallback         bool
}

// Service calls the model gateway and validates its plan.
type Service struct {
	Gateway   Generator
	Artifacts artifact.Store
	Profiles  ProfileLoader
	Log       *slog.Logger
	Observer  Observer
	Config    Config
}

// Plan asks the model for a plan, validates it, and retries malformed plans up
// to Config.MaxRepairs. Gateway failures are returned as retryable errors;
// planning/validation failures fall back to the configured template or ReAct.
func (s *Service) Plan(ctx context.Context, scope gw.Scope, in Input) (Result, error) {
	start := time.Now()
	cfg := s.Config
	if cfg.MaxRepairs < 0 {
		cfg.MaxRepairs = 0
	}
	if cfg.MaxNodes <= 0 {
		cfg.MaxNodes = 20
	}
	if cfg.MaxOutputTokens <= 0 {
		cfg.MaxOutputTokens = 8000
	}
	if cfg.ApprovalMode == "" {
		cfg.ApprovalMode = "auto"
	}
	if cfg.ApprovalApprovers <= 0 {
		cfg.ApprovalApprovers = 1
	}
	maxNodes := cfg.MaxNodes
	if in.MaxNodes > 0 && in.MaxNodes < maxNodes {
		maxNodes = in.MaxNodes
	}
	budget := s.budget(in, cfg, maxNodes)

	messages := []gw.Message{{Role: gw.RoleUser, Parts: []gw.Part{{
		Type: gw.PartText, Text: userPrompt(in, maxNodes, budget),
	}}}}
	var validation []string
	for attempt := 0; attempt <= cfg.MaxRepairs; attempt++ {
		req := s.request(messages, in, cfg)
		raw, _ := json.Marshal(req)
		reqRef, _, err := s.Artifacts.Put(ctx, scope.TenantID, "planner-request", raw)
		if err != nil {
			return Result{}, domain.Retryable(fmt.Errorf("store planner request: %w", err), 0)
		}
		scope.ManifestRef = reqRef
		resp, err := s.Gateway.Generate(ctx, scope, req, nil)
		if err != nil {
			if ge, ok := gw.AsError(err); ok && !ge.Retryable() &&
				ge.Code != gw.ErrBudgetExceeded && ge.Code != gw.ErrFenceLost && ge.Code != gw.ErrCancelled {
				validation = append(validation, err.Error())
				return s.fallback(ctx, scope, in, validation, budget, start)
			}
			s.observeResult(Result{}, time.Since(start), "error", "")
			return Result{}, mapGatewayError(err)
		}
		respRaw, _ := json.Marshal(resp)
		respRef, _, storeErr := s.Artifacts.Put(ctx, scope.TenantID, "planner-response", respRaw)
		if storeErr != nil {
			return Result{}, domain.Retryable(fmt.Errorf("store planner response: %w", storeErr), 0)
		}
		res, err := s.parseAndValidate(ctx, scope, in, resp, maxNodes, attempt, budget, cfg)
		if err == nil {
			res.ModelRequestID, res.ManifestRef, res.ResponseRef = resp.ModelRequestID, reqRef, respRef
			res.ValidationErrors = validation
			s.observeResult(res, time.Since(start), planOutcome(res), res.ReviewRisk)
			return res, nil
		}
		validation = append(validation, err.Error())
		s.ObservePlanValidation(classifyPlanError(err))
		if attempt == cfg.MaxRepairs {
			break
		}
		// Repair on a fresh request that carries the invalid plan and the
		// precise validation error; provider tool-use history is not needed.
		invalid := "no submit_flow call"
		if call := findSubmitFlow(resp); call != nil {
			invalid = string(call.Args)
		}
		messages = append(messages,
			gw.Message{Role: gw.RoleAssistant, Parts: []gw.Part{{Type: gw.PartText, Text: "[previous plan]\n" + invalid}}},
			gw.Message{Role: gw.RoleUser, Parts: []gw.Part{{Type: gw.PartText, Text: "[validation failed]\n" + err.Error() +
				"\nCall submit_flow again with a corrected plan. Keep the same goal and only use listed tools."}}})
	}
	return s.fallback(ctx, scope, in, validation, budget, start)
}

func (s *Service) request(messages []gw.Message, in Input, cfg Config) gw.Request {
	req := gw.Request{
		ModelAlias:      cfg.ModelAlias,
		System:          []gw.SystemBlock{{Text: systemPrompt, CacheBreakpoint: true}},
		Messages:        messages,
		Tools:           toolSchemas(in.Tools),
		MaxOutputTokens: cfg.MaxOutputTokens,
		Effort:          cfg.Effort,
		NeedTools:       true,
	}
	req.Tools = append(req.Tools, gw.ToolSchema{Name: SubmitFlowTool,
		Description: "Submit the orchestration plan for this goal. Call exactly once; use mode=react when a simple ReAct loop is enough.",
		InputSchema: submitFlowSchema})
	return req
}

func (s *Service) parseAndValidate(ctx context.Context, scope gw.Scope, in Input, resp gw.Response, maxNodes, attempt int, budget Budget, cfg Config) (Result, error) {
	call := findSubmitFlow(resp)
	if call == nil {
		return Result{}, fmt.Errorf("model did not call %s", SubmitFlowTool)
	}
	var p struct {
		Mode      string          `json:"mode"`
		Rationale string          `json:"rationale"`
		Flow      json.RawMessage `json:"flow"`
		Input     map[string]any  `json:"input,omitempty"`
	}
	if err := json.Unmarshal(call.Args, &p); err != nil {
		return Result{}, fmt.Errorf("%s arguments: %w", SubmitFlowTool, err)
	}
	if strings.TrimSpace(p.Rationale) == "" {
		return Result{}, fmt.Errorf("rationale is required")
	}
	switch p.Mode {
	case "react":
		a := Analysis{Nodes: 0, MaxSteps: in.MaxSteps, EstimatedModelCalls: in.MaxSteps,
			HighestRisk: domain.RiskR0, Findings: []Finding{{Code: "react_plan", Severity: "info",
				Message: "a ReAct loop reuses the profile step budget; no generated graph to review"}}}
		return Result{Mode: "react", Rationale: p.Rationale, Repairs: attempt,
			PlanHash: PlanHash("react", "", nil), Analysis: &a}, nil
	case "flow":
		if len(p.Flow) == 0 {
			return Result{}, fmt.Errorf("mode=flow requires a flow object")
		}
		opts := flow.CompileOptions{AllowTool: in.AllowTool}
		fs, canon, hash, err := flow.Compile(p.Flow, opts)
		if err != nil {
			return Result{}, err
		}
		if len(fs.Nodes) > maxNodes {
			return Result{}, fmt.Errorf("flow has %d nodes, limit is %d", len(fs.Nodes), maxNodes)
		}
		if clampLimits(&fs, budget, in.MaxSteps) {
			// The model may omit or overstate limits; clamp them to the
			// profile/plan budget instead of rejecting an otherwise valid
			// graph. The hash is recomputed so the approval binds the exact
			// graph that will run.
			if fs, canon, hash, err = recompile(fs, opts); err != nil {
				return Result{}, err
			}
		}
		analysis := Analyze(fs, AnalysisOptions{Tools: in.Tools, Budget: budget, MaxSteps: in.MaxSteps})
		if errs := analysis.Errors(); len(errs) > 0 {
			return Result{}, fmt.Errorf("flow budget: %s", strings.Join(errs, "; "))
		}
		needsReview := false
		switch strings.ToLower(cfg.ApprovalMode) {
		case "always":
			needsReview = true
		case "never":
			needsReview = false
		default:
			needsReview = analysis.RequiresReview
		}
		return Result{Mode: "flow", Flow: &fs, Canonical: canon, Hash: hash,
			PlanHash: PlanHash("flow", hash, p.Input), Rationale: p.Rationale,
			Inputs: p.Input, Analysis: &analysis, NeedsReview: needsReview,
			ReviewRisk: analysis.ReviewRisk(), Repairs: attempt}, nil
	default:
		return Result{}, fmt.Errorf("mode must be react or flow, got %q", p.Mode)
	}
}

func (s *Service) fallback(ctx context.Context, scope gw.Scope, in Input, validation []string, budget Budget, start time.Time) (Result, error) {
	cfg := s.Config
	if cfg.FallbackProfileID != "" && cfg.FallbackVersion > 0 && looksLikeRunbook(in.Goal) && s.Profiles != nil {
		raw, err := s.Profiles.ProfileSpec(ctx, scope.TenantID, cfg.FallbackProfileID, cfg.FallbackVersion)
		if err == nil {
			var spec struct {
				Flow json.RawMessage `json:"flow"`
			}
			if json.Unmarshal(raw, &spec) == nil && len(spec.Flow) > 0 {
				fs, canon, hash, err := flow.Compile(spec.Flow, flow.CompileOptions{AllowTool: in.AllowTool})
				if err == nil && (in.MaxNodes <= 0 || len(fs.Nodes) <= in.MaxNodes) &&
					(in.MaxSteps <= 0 || fs.Limits.MaxSteps <= in.MaxSteps) {
					analysis := Analyze(fs, AnalysisOptions{Tools: in.Tools, Budget: budget, MaxSteps: in.MaxSteps})
					res := Result{Mode: "flow", Flow: &fs, Canonical: canon, Hash: hash,
						PlanHash: PlanHash("flow", hash, nil), Fallback: true,
						Rationale: fmt.Sprintf("planner could not produce a valid plan; using the deterministic fallback %s@%d",
							cfg.FallbackProfileID, cfg.FallbackVersion),
						Analysis: &analysis, ValidationErrors: validation}
					s.observeResult(res, time.Since(start), "fallback_flow", analysis.ReviewRisk())
					s.ObservePlanFallback("flow")
					return res, nil
				}
			}
		}
		if s.Log != nil {
			s.Log.Warn("planner fallback flow unavailable", "profile", cfg.FallbackProfileID, "version", cfg.FallbackVersion, "err", err)
		}
	}
	if cfg.FallbackReact {
		res := Result{Mode: "react", Fallback: true, PlanHash: PlanHash("react", "", nil),
			Rationale:        "planner could not produce a valid plan; falling back to ReAct",
			ValidationErrors: validation}
		s.observeResult(res, time.Since(start), "fallback_react", domain.RiskR0)
		s.ObservePlanFallback("react")
		return res, nil
	}
	s.observeResult(Result{}, time.Since(start), "rejected", "")
	s.ObservePlanFallback("none")
	return Result{}, fmt.Errorf("%w: %s", ErrNoPlan, strings.Join(validation, "; "))
}

// PlanHash binds a plan approval to the exact mode, graph and inputs. It is
// stable across replays and independent of model request IDs.
func PlanHash(mode, flowHash string, inputs map[string]any) string {
	payload := struct {
		Mode     string         `json:"mode"`
		FlowHash string         `json:"flow_hash,omitempty"`
		Input    map[string]any `json:"input,omitempty"`
	}{Mode: mode, FlowHash: flowHash, Input: inputs}
	raw, _ := json.Marshal(payload)
	return artifact.Hash(raw)
}

// clampLimits lowers every flow limit to the plan budget and returns whether
// the spec changed. Runtime limits are therefore never larger than the
// static analysis that was approved.
func clampLimits(fs *flow.Spec, b Budget, maxSteps int) bool {
	changed := false
	set := func(dst *int, v int) {
		if v > 0 && *dst > v {
			*dst = v
			changed = true
		}
	}
	if maxSteps > 0 {
		set(&fs.Limits.MaxSteps, maxSteps)
	}
	set(&fs.Limits.MaxChildren, b.MaxChildren)
	set(&fs.Limits.MaxTotalRuns, b.MaxTotalRuns)
	set(&fs.Limits.MaxDepth, b.MaxDepth)
	set(&fs.Limits.MaxIterations, b.MaxIterations)
	if maxSteps > 0 {
		for i := range fs.Nodes {
			n := &fs.Nodes[i]
			switch n.Type {
			case flow.NodeAgent:
				// An omitted max_steps would otherwise let this node use the
				// whole profile budget, which the static model-call estimate
				// must account for. Explicit larger values are clamped too.
				if n.MaxSteps <= 0 || n.MaxSteps > maxSteps {
					n.MaxSteps = maxSteps
					changed = true
				}
			case flow.NodeSubflow:
				if n.Flow != nil && clampLimits(n.Flow, b, maxSteps) {
					changed = true
				}
			}
		}
	}
	return changed
}

func (s *Service) budget(in Input, cfg Config, maxNodes int) Budget {
	b := Budget{
		MaxNodes:           maxNodes,
		MaxChildren:        cfg.MaxChildren,
		MaxTotalRuns:       cfg.MaxTotalRuns,
		MaxDepth:           cfg.MaxDepth,
		MaxIterations:      cfg.MaxIterations,
		MaxModelCalls:      cfg.MaxModelCalls,
		MaxOutputTokens:    cfg.MaxOutputTokens,
		MaxEstimatedTokens: cfg.MaxEstimatedTokens,
	}
	if in.MaxSteps > 0 && (b.MaxModelCalls <= 0 || in.MaxSteps < b.MaxModelCalls) {
		b.MaxModelCalls = in.MaxSteps
	}
	return b.withDefaults()
}

func planOutcome(res Result) string {
	switch {
	case res.Fallback && res.Mode == "flow":
		return "fallback_flow"
	case res.Fallback:
		return "fallback_react"
	case res.Mode == "flow":
		return "generated"
	default:
		return "react"
	}
}

func (s *Service) observeResult(res Result, d time.Duration, outcome string, risk domain.RiskClass) {
	if s.Observer == nil {
		return
	}
	mode := res.Mode
	if mode == "" {
		mode = "none"
	}
	riskLabel := string(risk)
	if riskLabel == "" {
		riskLabel = string(domain.RiskR0)
	}
	s.Observer.ObservePlan(mode, outcome, riskLabel, d, res.Repairs, res.Fallback)
}

func (s *Service) ObservePlanValidation(reason string) {
	if s.Observer != nil {
		s.Observer.ObservePlanValidation(reason)
	}
}

func (s *Service) ObservePlanFallback(to string) {
	if s.Observer != nil {
		s.Observer.ObservePlanFallback(to)
	}
}

func classifyPlanError(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "did not call submit_flow"):
		return "no_submit_flow"
	case strings.Contains(msg, "arguments"):
		return "invalid_arguments"
	case strings.Contains(msg, "rationale"):
		return "missing_rationale"
	case strings.Contains(msg, "mode must be"):
		return "invalid_mode"
	case strings.Contains(msg, "nodes, limit"):
		return "node_budget"
	case strings.Contains(msg, "model calls"):
		return "model_call_budget"
	case strings.Contains(msg, "budget:"):
		return "budget"
	case strings.Contains(msg, "subflow"), strings.HasPrefix(msg, "flow:"):
		return "invalid_flow"
	default:
		return "validation"
	}
}

func recompile(fs flow.Spec, opts flow.CompileOptions) (flow.Spec, []byte, string, error) {
	b, err := json.Marshal(fs)
	if err != nil {
		return fs, nil, "", err
	}
	return flow.Compile(b, opts)
}

func findSubmitFlow(resp gw.Response) *gw.ToolCall {
	for i := range resp.ToolCalls {
		if resp.ToolCalls[i].Name == SubmitFlowTool {
			return &resp.ToolCalls[i]
		}
	}
	return nil
}

func mapGatewayError(err error) error {
	ge, ok := gw.AsError(err)
	if !ok {
		return domain.Retryable(err, 0)
	}
	switch {
	case ge.Code == gw.ErrFenceLost:
		return fmt.Errorf("%w: %v", domain.ErrLeaseLost, ge)
	case ge.Code == gw.ErrCancelled:
		return context.Canceled
	case ge.Code == gw.ErrBudgetExceeded:
		return domain.Retryable(ge, 5*time.Minute)
	case ge.Retryable():
		return domain.Retryable(ge, ge.RetryAfter)
	}
	return ge
}

func toolSchemas(ds []tools.Descriptor) []gw.ToolSchema {
	sorted := append([]tools.Descriptor(nil), ds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	out := make([]gw.ToolSchema, 0, len(sorted))
	for _, d := range sorted {
		desc := d.Description
		if d.Risk.HasSideEffects() {
			desc += fmt.Sprintf(" [risk %s: requires approval before it runs]", d.Risk)
		}
		schema := d.InputSchema
		if len(schema) == 0 || !json.Valid(schema) {
			schema = json.RawMessage(`{"type":"object"}`)
		}
		out = append(out, gw.ToolSchema{Name: d.Name, Description: desc, InputSchema: schema, Source: d.Source})
	}
	return out
}

func userPrompt(in Input, maxNodes int, budget Budget) string {
	var b strings.Builder
	b.WriteString("[goal]\n")
	b.WriteString(strings.TrimSpace(in.Goal))
	b.WriteString("\n")
	if len(in.History) > 0 {
		b.WriteString("\n[session]\n")
		for _, m := range in.History {
			b.WriteString(m.Role)
			b.WriteString(": ")
			b.WriteString(truncate(m.Text, 600))
			b.WriteString("\n")
		}
	}
	if len(in.Capabilities) > 0 {
		b.WriteString("\n[capabilities]\n- ")
		b.WriteString(strings.Join(in.Capabilities, "\n- "))
		b.WriteString("\n")
	}
	if len(in.Tools) > 0 {
		b.WriteString("\n[tools]\n")
		for _, d := range in.Tools {
			b.WriteString(fmt.Sprintf("- %s risk=%s trust=%s: %s\n", d.Name, d.Risk, d.Trust, truncate(d.Description, 240)))
		}
	}
	b.WriteString(fmt.Sprintf("\n[budgets]\nmax_nodes=%d max_children=%d max_total_runs=%d max_depth=%d max_iterations=%d max_model_calls=%d",
		maxNodes, budget.MaxChildren, budget.MaxTotalRuns, budget.MaxDepth, budget.MaxIterations, budget.MaxModelCalls))
	if in.MaxSteps > 0 {
		b.WriteString(fmt.Sprintf(" max_steps=%d", in.MaxSteps))
	}
	b.WriteString("\n\n[instruction]\nCall submit_flow exactly once.")
	b.WriteString(" Use mode=react for a question, a simple explanation, or a diagnosis that a ReAct loop handles.")
	b.WriteString(" Use mode=flow only when the work needs multiple steps, parallel evidence, an approval gate, human input, or a reusable runbook.")
	b.WriteString(" A flow must use only the exact tool names listed above; side-effecting tools must be preceded by an approval node.")
	b.WriteString(" Set max_steps on every agent node so the worst-case model-call budget is explicit.")
	b.WriteString(" Prefer read-only evidence first. Keep the graph small and deterministic.")
	return b.String()
}

func truncate(s string, n int) string {
	r := []rune(strings.Join(strings.Fields(strings.TrimSpace(s)), " "))
	if len(r) <= n {
		return string(r)
	}
	return string(r[:n]) + "…"
}

// looksLikeRunbook is the deterministic fallback router. It deliberately
// favors ReAct for question-shaped requests.
func looksLikeRunbook(text string) bool {
	t := strings.TrimSpace(strings.ToLower(text))
	for _, q := range []string{
		"why", "how", "what", "when", "where", "which", "who",
		"is ", "are ", "can ", "could ", "should ", "does ", "do ",
		"为什么", "怎么", "如何", "是否", "能否", "什么是",
	} {
		if strings.HasPrefix(t, q) {
			return false
		}
	}
	for _, v := range []string{
		"restart", "rollback", "redeploy", "deploy", "scale", "failover", "remediate", "recover",
		"drain", "rotate", "patch", "upgrade", "migrate",
		"重启", "回滚", "发布", "恢复", "修复", "扩容", "缩容", "摘流", "降级", "轮换", "升级", "迁移",
	} {
		if strings.Contains(t, v) {
			return true
		}
	}
	return false
}
