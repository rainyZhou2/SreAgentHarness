package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	"go-sre-harness/internal/planner"
)

// FlowStore is the durable flow projection the kernel needs. It is optional
// so stores used by tests can omit it.
type FlowStore interface {
	LoadFlow(ctx context.Context, tenantID, runID string) (domain.FlowSnapshot, error)
	FlowInput(ctx context.Context, tenantID, stepID string) (json.RawMessage, bool, error)
}

// executeFlow advances one coordinator step. A flow step may spawn child
// runs, wait for them, plan an approval/input step, or complete the run.
func (w *Worker) executeFlow(ctx context.Context, lease domain.Lease) (domain.StepSuccess, error) {
	rules, err := w.rules(ctx, lease)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	var g goalDoc
	if err := w.get(ctx, lease.TenantID, lease.GoalRef, &g); err != nil {
		return domain.StepSuccess{}, err
	}
	spec := rules.Flow
	inputs := flowInputs(g)
	if g.Flow != nil && g.Flow.Subflow != nil {
		spec = g.Flow.Subflow
	}
	// A generated plan is passed as the flow step's input artifact.
	generated := false
	var generatedPlan *planArtifact
	generatedPlanHash := ""
	if lease.InputRef != "" && lease.InputRef != lease.GoalRef {
		var plan planArtifact
		if err := w.get(ctx, lease.TenantID, lease.InputRef, &plan); err == nil && plan.Mode == "flow" && len(plan.Flow) > 0 {
			fs, _, hash, err := flow.Compile(plan.Flow, flow.CompileOptions{AllowTool: rules.AllowsTool})
			if err != nil {
				return domain.StepSuccess{}, fmt.Errorf("generated flow plan: %w", err)
			}
			if plan.FlowHash != "" && hash != plan.FlowHash {
				return domain.StepSuccess{}, fmt.Errorf("generated flow plan hash mismatch")
			}
			planHash := plan.PlanHash
			if planHash == "" {
				planHash = planner.PlanHash(plan.Mode, plan.FlowHash, plan.Input)
			}
			if got := planner.PlanHash(plan.Mode, plan.FlowHash, plan.Input); got != planHash {
				return domain.StepSuccess{}, fmt.Errorf("generated flow plan content does not match its plan hash")
			}
			spec = &fs
			if len(plan.Input) > 0 {
				inputs = plan.Input
			}
			generated = true
			generatedPlan, generatedPlanHash = &plan, planHash
		}
	}
	fs, ok := w.store.(FlowStore)
	if !ok {
		return domain.StepSuccess{}, fmt.Errorf("state store does not implement FlowStore")
	}

	snap, err := fs.LoadFlow(ctx, lease.TenantID, lease.RunID)
	var st flow.State
	firstExecution := false
	switch {
	case errors.Is(err, domain.ErrNotFound):
		if spec == nil {
			return domain.StepSuccess{}, fmt.Errorf("flow step without a flow spec")
		}
		if generated && generatedPlan != nil && generatedPlan.NeedsReview {
			ok, err := w.planReviewSucceeded(ctx, lease, generatedPlanHash)
			if err != nil {
				return domain.StepSuccess{}, err
			}
			if !ok {
				return domain.StepSuccess{}, fmt.Errorf("generated plan %s has no approved plan review", generatedPlanHash)
			}
		}
		ns, err := flow.NewState(*spec, inputs, lease.FlowDepth)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		st = *ns
		firstExecution = generated && generatedPlan != nil
	case err != nil:
		return domain.StepSuccess{}, err
	default:
		if len(snap.State) > 0 {
			if err := json.Unmarshal(snap.State, &st); err != nil {
				return domain.StepSuccess{}, fmt.Errorf("decode flow state: %w", err)
			}
		}
		if st.Spec != nil && spec == nil {
			spec = st.Spec
		}
		if spec == nil {
			return domain.StepSuccess{}, fmt.Errorf("flow state has no spec")
		}
		if st.Spec == nil {
			st.Spec = spec
		}
		if err := st.Spec.Validate(flow.CompileOptions{AllowTool: rules.AllowsTool}); err != nil {
			return domain.StepSuccess{}, fmt.Errorf("flow state spec: %w", err)
		}
	}
	if spec == nil {
		return domain.StepSuccess{}, fmt.Errorf("flow step without a flow spec")
	}
	// A generated plan that required review must have a succeeded
	// plan_review step whose input hash equals the plan hash. The plan step
	// only appends the flow step after that review, but the check keeps a
	// hand-crafted or forked step from bypassing the gate.
	if generated && generatedPlan != nil && generatedPlan.NeedsReview && firstExecution {
		ok, err := w.planReviewSucceeded(ctx, lease, generatedPlanHash)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		if !ok {
			return domain.StepSuccess{}, fmt.Errorf("generated plan %s has no approved plan review", generatedPlanHash)
		}
	}
	if lease.FlowDepth > spec.Limits.MaxDepth {
		return domain.StepSuccess{}, fmt.Errorf("flow depth %d exceeds max_depth=%d", lease.FlowDepth, spec.Limits.MaxDepth)
	}

	results, err := w.flowResults(ctx, lease.TenantID, snap)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	if lease.FlowNodeID != "" {
		node := specNode(spec, lease.FlowNodeID)
		switch {
		case lease.Intent != nil:
			results[lease.FlowNodeID] = flow.NodeResult{Approved: true}
		case node != nil && node.Type == flow.NodeInput:
			raw, provided, err := fs.FlowInput(ctx, lease.TenantID, lease.StepID)
			if err != nil {
				return domain.StepSuccess{}, err
			}
			if !provided {
				return domain.StepSuccess{}, fmt.Errorf("flow input node %q has no provided value", lease.FlowNodeID)
			}
			var v any
			if err := json.Unmarshal(raw, &v); err != nil {
				return domain.StepSuccess{}, fmt.Errorf("decode flow input: %w", err)
			}
			results[lease.FlowNodeID] = flow.NodeResult{InputSet: true, Input: v}
		case node != nil && node.Type == flow.NodeApproval:
			return domain.StepSuccess{}, fmt.Errorf("flow approval node %q was claimed without an approval", lease.FlowNodeID)
		}
	}

	action, err := flow.Step(*spec, &st, results)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	stateJSON, err := json.Marshal(st)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	planRef, planHash, err := w.put(ctx, lease.TenantID, "flow-plan", *spec)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	flowKey := snap.ID
	if flowKey == "" {
		flowKey = lease.RunID
	}
	commit := &domain.FlowCommit{FlowRunID: snap.ID, CompiledRef: planRef, CompiledHash: planHash,
		State: stateJSON, Status: "RUNNING", CurrentNode: st.Current, ChildRuns: st.ChildRuns}
	var planEvents []domain.EmittedEvent
	if firstExecution && generatedPlan != nil {
		payload := map[string]any{"mode": "flow", "plan_hash": generatedPlanHash, "flow_hash": generatedPlan.FlowHash,
			"fallback": generatedPlan.Fallback, "needs_review": generatedPlan.NeedsReview}
		if generatedPlan.Analysis != nil {
			payload["risk"] = generatedPlan.Analysis.HighestRisk
			payload["estimated_runs"] = generatedPlan.Analysis.EstimatedRuns
			payload["estimated_model_calls"] = generatedPlan.Analysis.EstimatedModelCalls
		}
		planEvents = []domain.EmittedEvent{{Type: "plan.executed", Payload: payload}}
	}

	switch action.Kind {
	case flow.ActionWait:
		commit.Status = "WAITING_CHILDREN"
		return domain.StepSuccess{Flow: commit, Wait: domain.WaitChildren, Events: planEvents}, nil

	case flow.ActionAgent, flow.ActionTool, flow.ActionSubflow, flow.ActionParallel:
		children, err := w.flowChildren(ctx, lease, flowKey, g.Goal, action.Children)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		if len(children) == 0 {
			return domain.StepSuccess{}, fmt.Errorf("flow action %q produced no child runs", action.Kind)
		}
		commit.Status = "WAITING_CHILDREN"
		commit.Children = children
		return domain.StepSuccess{Flow: commit, Wait: domain.WaitChildren, Events: planEvents}, nil

	case flow.ActionApproval:
		step, err := w.flowApprovalStep(ctx, lease, action.Node, stateJSON)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		commit.Status = "WAITING_APPROVAL"
		return domain.StepSuccess{Flow: commit, Plan: []domain.PlannedStep{step}, Events: planEvents}, nil

	case flow.ActionInput:
		step, err := w.flowInputStep(ctx, lease, action.Node, stateJSON)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		commit.Status = "WAITING_INPUT"
		return domain.StepSuccess{Flow: commit, Plan: []domain.PlannedStep{step}, Events: planEvents}, nil

	case flow.ActionComplete:
		result := map[string]any{"output": action.Output}
		if s, ok := action.Output.(string); ok {
			result["answer"] = s
		} else if b, err := json.Marshal(action.Output); err == nil {
			result["answer"] = string(b)
		}
		resultRef, _, err := w.put(ctx, lease.TenantID, "result", result)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		commit.Status = "COMPLETED"
		return domain.StepSuccess{Complete: true, ResultRef: resultRef, OutputRef: resultRef, Flow: commit,
			Events: planEvents}, nil

	case flow.ActionFail:
		return domain.StepSuccess{}, fmt.Errorf("flow: %s", action.Error)
	}
	return domain.StepSuccess{}, fmt.Errorf("flow: unknown action %q", action.Kind)
}

func (w *Worker) flowResults(ctx context.Context, tenantID string, snap domain.FlowSnapshot) (map[string]flow.NodeResult, error) {
	latest := map[string]domain.FlowNodeRun{}
	for _, n := range snap.Nodes {
		latest[n.NodeID] = n // ordered by node_id, attempt: the last attempt wins
	}
	out := make(map[string]flow.NodeResult, len(latest))
	for id, n := range latest {
		switch n.Status {
		case "SUCCEEDED":
			v, err := w.flowOutput(ctx, tenantID, n.OutputRef)
			if err != nil {
				return nil, err
			}
			out[id] = flow.NodeResult{Status: "SUCCEEDED", Output: v}
		case "FAILED":
			msg := n.Error
			if msg == "" {
				msg = "child run failed"
			}
			out[id] = flow.NodeResult{Status: "FAILED", Error: msg}
		case "CANCELLED":
			out[id] = flow.NodeResult{Status: "FAILED", Error: "child run cancelled"}
		}
	}
	return out, nil
}

func (w *Worker) flowOutput(ctx context.Context, tenantID, ref string) (any, error) {
	if ref == "" {
		return nil, nil
	}
	data, err := w.artifacts.Get(ctx, tenantID, ref)
	if err != nil {
		return nil, domain.Retryable(fmt.Errorf("flow: read child output %s: %w", ref, err), 0)
	}
	var r struct {
		Answer     string          `json:"answer"`
		FlowOutput json.RawMessage `json:"flow_output"`
		Output     json.RawMessage `json:"output"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("flow: decode child output %s: %w", ref, err)
	}
	if len(r.FlowOutput) > 0 && string(r.FlowOutput) != "null" {
		var v any
		if json.Unmarshal(r.FlowOutput, &v) == nil {
			return v, nil
		}
	}
	if len(r.Output) > 0 && string(r.Output) != "null" {
		var v any
		if json.Unmarshal(r.Output, &v) == nil {
			return v, nil
		}
	}
	return r.Answer, nil
}

func (w *Worker) flowChildren(ctx context.Context, lease domain.Lease, flowKey, parentGoal string, children []flow.ChildSpec) ([]domain.ChildRunSpec, error) {
	out := make([]domain.ChildRunSpec, 0, len(children))
	for _, c := range children {
		attempt := c.Attempt
		if attempt < 1 {
			attempt = 1
		}
		fg := &flowNodeGoal{ID: c.NodeID, Type: string(c.Type), Role: c.Role, Prompt: c.Prompt,
			Tools: c.Tools, ModelAlias: c.ModelAlias, MaxSteps: c.MaxSteps, Tool: c.Tool, Args: c.Args,
			Input: c.Input, Subflow: c.Flow}
		goal := goalDoc{Goal: flowNodeGoalText(parentGoal, c), Inputs: c.Input, Flow: fg}
		ref, hash, err := w.put(ctx, lease.TenantID, "flow-node", goal)
		if err != nil {
			return nil, err
		}
		spec := domain.ChildRunSpec{NodeID: c.NodeID, NodeType: string(c.Type), Attempt: attempt,
			ProfileID: lease.ProfileID, ProfileVersion: lease.ProfileVersion,
			GoalRef: ref, GoalHash: hash,
			IdempotencyKey: fmt.Sprintf("flow:%s:%s:%d", flowKey, c.NodeID, attempt)}
		if c.Type == flow.NodeSubflow {
			spec.InitialSteps = []domain.PlannedStep{{Kind: domain.StepFlow, Executor: "flow", FlowNodeID: c.NodeID,
				InputRef: ref, InputHash: hash, Status: domain.StepPending}}
		}
		out = append(out, spec)
	}
	return out, nil
}

func flowNodeGoalText(parent string, c flow.ChildSpec) string {
	switch c.Type {
	case flow.NodeTool:
		args, _ := json.Marshal(c.Args)
		return fmt.Sprintf("Flow tool node %q. Run tool %s with arguments %s. Parent goal: %s",
			c.NodeID, c.Tool, string(args), parent)
	case flow.NodeSubflow:
		return fmt.Sprintf("Flow subflow node %q. Parent goal: %s", c.NodeID, parent)
	default:
		role := c.Role
		if role == "" {
			role = "agent"
		}
		input, _ := json.Marshal(c.Input)
		return fmt.Sprintf("Flow node %q (%s). Parent goal: %s\nNode input: %s",
			c.NodeID, role, parent, string(input))
	}
}

func (w *Worker) flowApprovalStep(ctx context.Context, lease domain.Lease, node *flow.Node, stateJSON []byte) (domain.PlannedStep, error) {
	if node == nil {
		return domain.PlannedStep{}, fmt.Errorf("flow approval action without a node")
	}
	ref, hash, err := w.put(ctx, lease.TenantID, "flow-advance", map[string]any{
		"node": node.ID, "state": json.RawMessage(stateJSON)})
	if err != nil {
		return domain.PlannedStep{}, err
	}
	risk := node.Risk
	if risk == "" {
		risk = string(domain.RiskR2)
	}
	required := node.Approvers
	if required < 1 {
		required = 1
	}
	effects, _ := json.Marshal(map[string]string{"flow_node": node.ID, "state_hash": hash})
	intent := domain.ActionIntent{
		Environment: "flow", Target: node.ID, ToolName: "flow.approval", ToolVersion: "1",
		InputHash:  hash,
		ActionHash: artifact.Hash([]byte(lease.TenantID + "\x00" + lease.RunID + "\x00" + node.ID + "\x00" + hash)),
		RiskClass:  domain.RiskClass(risk), PolicyVersion: "flow", Effects: effects,
	}
	pd, _ := json.Marshal(map[string]any{"verdict": "require_approval", "risk": risk,
		"reason": "flow approval node " + node.ID})
	return domain.PlannedStep{Kind: domain.StepFlow, Executor: "flow", FlowNodeID: node.ID,
		InputRef: ref, InputHash: hash, Status: domain.StepWaitingApproval, Intent: &intent,
		RequiredApprovals: required, PolicyDecision: pd}, nil
}

func (w *Worker) flowInputStep(ctx context.Context, lease domain.Lease, node *flow.Node, stateJSON []byte) (domain.PlannedStep, error) {
	if node == nil {
		return domain.PlannedStep{}, fmt.Errorf("flow input action without a node")
	}
	ref, hash, err := w.put(ctx, lease.TenantID, "flow-advance", map[string]any{
		"node": node.ID, "schema": node.InputSchema, "state": json.RawMessage(stateJSON)})
	if err != nil {
		return domain.PlannedStep{}, err
	}
	return domain.PlannedStep{Kind: domain.StepFlow, Executor: "flow", FlowNodeID: node.ID,
		InputRef: ref, InputHash: hash, Status: domain.StepWaitingInput}, nil
}

// planReviewSucceeded reports whether this run has a committed plan review
// with the exact plan hash. It is checked once, when the flow state is first
// created.
func (w *Worker) planReviewSucceeded(ctx context.Context, lease domain.Lease, planHash string) (bool, error) {
	steps, err := w.store.History(ctx, lease.TenantID, lease.RunID, 1<<62)
	if err != nil {
		return false, err
	}
	for _, st := range steps {
		if st.Kind == domain.StepPlanReview && st.Status == domain.StepSucceeded && st.InputHash == planHash {
			return true, nil
		}
	}
	return false, nil
}

func flowToolVisible(tools []string, name string) bool {
	for _, t := range tools {
		if t == name {
			return true
		}
	}
	return false
}

func specNode(spec *flow.Spec, id string) *flow.Node {
	for i := range spec.Nodes {
		if spec.Nodes[i].ID == id {
			return &spec.Nodes[i]
		}
	}
	return nil
}

// flowInputs resolves the initial values of a flow run. A goal that is a JSON
// object is used directly; plain text is exposed as input.goal. Subflow
// children receive the parent's inputs, vars and node outputs.
func flowInputs(g goalDoc) map[string]any {
	if len(g.Inputs) > 0 {
		return g.Inputs
	}
	if g.Flow != nil && len(g.Flow.Input) > 0 {
		out := map[string]any{}
		if v, ok := g.Flow.Input["inputs"].(map[string]any); ok {
			for k, val := range v {
				out[k] = val
			}
		}
		if v, ok := g.Flow.Input["vars"]; ok {
			out["vars"] = v
		}
		if v, ok := g.Flow.Input["nodes"]; ok {
			out["nodes"] = v
		}
		return out
	}
	trimmed := strings.TrimSpace(g.Goal)
	if strings.HasPrefix(trimmed, "{") {
		var raw map[string]any
		if json.Unmarshal([]byte(trimmed), &raw) == nil {
			if v, ok := raw["inputs"].(map[string]any); ok {
				return v
			}
			return raw
		}
	}
	return map[string]any{"goal": g.Goal}
}
