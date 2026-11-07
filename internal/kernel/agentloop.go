package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/envctx"
	"go-sre-harness/internal/flow"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/artifactq"
)

// ContextGatherer collects environment facts for a run (envctx.Gatherer).
type ContextGatherer interface {
	Gather(ctx context.Context, q envctx.Query) []domain.ContextFact
}

// factsKey is the run KV key holding the pinned environment facts.
const factsKey = "context/facts"

// DefaultOffloadBytes is the size above which tool results are offloaded.
const DefaultOffloadBytes = 24 << 10

// ---------- environment facts ----------

// gatherFacts collects facts for the first reason step of a run and returns
// the KV write that pins them. renderContext sets in.Facts (possibly empty,
// never nil) once they are pinned; later steps read the pinned value.
func (w *Worker) gatherFacts(ctx context.Context, lease domain.Lease, in *cognitive.ReasoningInput) *domain.KVWrite {
	if w.cfg.Context == nil || in.Facts != nil {
		return nil
	}
	q := envctx.Query{TenantID: lease.TenantID, RunID: lease.RunID, Goal: in.Goal,
		Services: envctx.Services(in.Time, in.Goal)}
	if gc := in.Time; gc != nil {
		q.Incident, q.Labels, q.Now = gc.Incident, gc.Labels, gc.ReceivedAt
		if start, end, ok := gc.Range(time.Hour); ok {
			q.Start, q.End = start, end
		}
	}
	facts := w.cfg.Context.Gather(ctx, q)
	if facts == nil {
		facts = []domain.ContextFact{}
	}
	in.Facts = facts
	data, err := json.Marshal(facts)
	if err != nil {
		return nil
	}
	return &domain.KVWrite{Key: domain.KVKey{TenantID: lease.TenantID, Scope: "run", OwnerID: lease.RunID, Name: factsKey},
		Data: data, SchemaID: "context-facts/v1"}
}

// ---------- time arguments ----------

// fillTimeArgs adds missing time-range arguments declared by the tool from
// the run's pinned time context. The filled call is what gets approved.
func fillTimeArgs(desc *tools.Descriptor, args json.RawMessage, gc *domain.GoalContext) json.RawMessage {
	if desc == nil || desc.TimeArgs == nil || gc == nil {
		return args
	}
	ta := desc.TimeArgs
	start, end, ok := gc.Range(ta.Lookback.D())
	if !ok {
		return args
	}
	var m map[string]any
	if len(args) == 0 {
		m = map[string]any{}
	} else if err := json.Unmarshal(args, &m); err != nil || m == nil {
		return args
	}
	changed := false
	set := func(name string, t time.Time) {
		if name == "" {
			return
		}
		if v, present := m[name]; present && v != nil && v != "" {
			return
		}
		m[name] = formatTime(t, ta.Format)
		changed = true
	}
	set(ta.Start, start)
	set(ta.End, end)
	if !changed {
		return args
	}
	out, err := json.Marshal(m)
	if err != nil {
		return args
	}
	return out
}

func formatTime(t time.Time, format string) any {
	switch format {
	case "unix":
		return t.Unix()
	case "unix_ms":
		return t.UnixMilli()
	default:
		return t.UTC().Format(time.RFC3339)
	}
}

// ---------- large outputs ----------

// observe builds the observation for a tool result, offloading results
// above the size threshold to a tool-output artifact.
func (w *Worker) observe(ctx context.Context, tenantID string, desc tools.Descriptor, obs domain.Observation, out json.RawMessage) (domain.Observation, error) {
	obs.Content = out
	limit := w.cfg.OutputOffloadBytes
	if desc.MaxInlineBytes != 0 {
		limit = desc.MaxInlineBytes
	}
	if limit < 0 || len(out) <= limit || desc.Name == artifactq.Name {
		return obs, nil
	}
	ref, hash, err := w.artifacts.Put(ctx, tenantID, artifactq.Kind, out)
	if err != nil {
		return obs, domain.Retryable(fmt.Errorf("offload tool output: %w", err), 0)
	}
	ct, sum := w.cfg.Summarizers.Summarize(out)
	content, _ := json.Marshal(map[string]any{
		"offloaded": true, "handle": ref, "bytes": len(out), "content_type": ct, "summary": sum,
		"hint": "The full result is stored. Query it with " + artifactq.Name +
			` {"handle": "<handle>", "op": "grep|lines|head|tail|path|time"} instead of calling the tool again.`,
	})
	obs.Content = content
	obs.Artifact = &domain.ObservationArtifact{Handle: ref, Bytes: len(out), ContentType: ct, Hash: hash}
	return obs, nil
}

// ---------- post-action lifecycle ----------

// followupStep plans a kernel-initiated tool step through the same policy
// path as a model-proposed call. forceApproval keeps the step behind a human
// approval whatever its risk class (compensation).
func (w *Worker) followupStep(ctx context.Context, lease domain.Lease, rules profile.Rules, call domain.ToolCall,
	forceApproval bool) (domain.PlannedStep, policy.Decision, error) {
	var reg *tools.Descriptor
	if t, ok := w.tools.LookupFor(lease.TenantID, call.Name); ok {
		d := t.Descriptor()
		reg = &d
	}
	ref, hash, err := w.put(ctx, lease.TenantID, "tool-input", call)
	if err != nil {
		return domain.PlannedStep{}, policy.Decision{}, err
	}
	pd, desc := rules.DecideCall(ctx, w.policy, profile.Call{TenantID: lease.TenantID, RunID: lease.RunID,
		Name: call.Name, Args: call.Args}, reg)
	if forceApproval && pd.Verdict == policy.Allow {
		pd.Verdict, pd.RequiredApprovals = policy.RequireApproval, 1
		pd.Reason = strings.TrimSpace(pd.Reason + "; compensation always requires human approval")
	}
	step := domain.PlannedStep{Kind: domain.StepTool, Executor: "tool", InputRef: ref, InputHash: hash,
		PolicyDecision: mustJSON(pd)}
	if desc != nil && pd.Verdict != policy.Deny {
		intent := w.policy.BuildIntent(*desc, call, hash, pd)
		step.Intent = &intent
	}
	switch pd.Verdict {
	case policy.Allow:
		step.Status = domain.StepPending
	case policy.RequireApproval:
		step.Status, step.RequiredApprovals = domain.StepWaitingApproval, max(pd.RequiredApprovals, 1)
	default:
		step.Status, step.StatusReason, step.Intent = domain.StepSkipped, "denied by policy: "+pd.Reason, nil
	}
	return step, pd, nil
}

// lifecycleArgs renders follow-up arguments over {args, action}.
func lifecycleArgs(f *tools.FollowUp, ofArgs json.RawMessage, action any) (json.RawMessage, error) {
	if f.Args == nil {
		if len(ofArgs) == 0 {
			return json.RawMessage(`{}`), nil
		}
		return ofArgs, nil
	}
	v, err := flow.InterpolateValue(map[string]any(f.Args), map[string]any{"args": decodeAny(ofArgs), "action": action})
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func decodeAny(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	var v any
	if json.Unmarshal(b, &v) != nil {
		return string(b)
	}
	return v
}

// planVerify plans the verification step after a successful action. It
// returns nil when the tool declares no verification.
func (w *Worker) planVerify(ctx context.Context, lease domain.Lease, rules profile.Rules, desc tools.Descriptor,
	call domain.ToolCall, outputRef string, output json.RawMessage) (*domain.PlannedStep, string, error) {
	lc := desc.Lifecycle
	if lc == nil || lc.Verify == nil || call.Followup != nil {
		return nil, "", nil
	}
	args, err := lifecycleArgs(lc.Verify, call.Args, decodeAny(output))
	if err != nil {
		return nil, "verification not planned: " + err.Error(), nil
	}
	vcall := domain.ToolCall{Name: lc.Verify.Tool, Args: args, Followup: &domain.ToolFollowup{Role: domain.FollowupVerify,
		OfTool: call.Name, OfStep: lease.StepSeq, OfArgs: call.Args, OfOutputRef: outputRef}}
	step, pd, err := w.followupStep(ctx, lease, rules, vcall, false)
	if err != nil {
		return nil, "", err
	}
	note := "verification planned with " + lc.Verify.Tool
	if pd.Verdict == policy.Deny {
		note = "verification with " + lc.Verify.Tool + " denied by policy: " + pd.Reason
	}
	return &step, note, nil
}

// verifyOutcome is the result of polling a verify tool.
type verifyOutcome struct {
	verdict  string
	attempts int
	detail   string
	output   json.RawMessage
}

// executeVerify polls the verify tool until the expectation holds, the
// attempts are used up or the step deadline approaches. Verify tools must be
// read-only, so polling (and retrying the whole step) is safe.
func (w *Worker) executeVerify(ctx context.Context, lease domain.Lease, call domain.ToolCall, tool tools.Tool,
	desc tools.Descriptor) (verifyOutcome, *tools.Lifecycle) {
	fu := call.Followup
	action, ok := w.tools.LookupFor(lease.TenantID, fu.OfTool)
	if !ok {
		return verifyOutcome{verdict: domain.VerifyError, detail: "action tool " + fu.OfTool + " is no longer registered"}, nil
	}
	lc := action.Descriptor().Lifecycle
	if lc == nil || lc.Verify == nil || lc.Verify.Tool != call.Name {
		return verifyOutcome{verdict: domain.VerifyError, detail: "the lifecycle of " + fu.OfTool + " changed since the action"}, nil
	}
	if desc.Risk.HasSideEffects() {
		return verifyOutcome{verdict: domain.VerifyError, detail: "verify tool " + call.Name + " is " + string(desc.Risk) +
			"; verification tools must be read-only"}, lc
	}
	var actionOut any
	if fu.OfOutputRef != "" {
		var o domain.Observation
		if err := w.get(ctx, lease.TenantID, fu.OfOutputRef, &o); err == nil {
			actionOut = decodeAny(o.Content)
		}
	}
	var expect flow.Expr
	if strings.TrimSpace(lc.Expect) != "" {
		e, err := flow.CompileExpr(lc.Expect)
		if err != nil {
			return verifyOutcome{verdict: domain.VerifyError, detail: "expect: " + err.Error()}, lc
		}
		expect = e
	}
	attempts := lc.Attempts
	if attempts <= 0 {
		attempts = 3
	}
	interval := lc.Interval.D()
	if interval <= 0 {
		interval = 10 * time.Second
	}
	const margin = 5 * time.Second
	sleep := func(d time.Duration) bool {
		if d <= 0 {
			return true
		}
		if !lease.Deadline.IsZero() && time.Until(lease.Deadline)-margin < d {
			return false
		}
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		}
	}
	out := verifyOutcome{verdict: domain.VerifyError, detail: "verification did not run"}
	if !sleep(lc.Delay.D()) {
		out.detail = "no time left in the step for the verification delay"
		return out, lc
	}
	callCtx := tools.WithCallInfo(ctx, tools.CallInfo{TenantID: lease.TenantID, RunID: lease.RunID, StepID: lease.StepID,
		AttemptID: lease.AttemptID, FenceToken: lease.FenceToken, ContextSnapshotID: lease.ContextSnapshotID,
		InputRef: lease.InputRef, Deadline: lease.Deadline})
	for i := 1; i <= attempts; i++ {
		if i > 1 && !sleep(interval) {
			out.detail += "; stopped polling before the step deadline"
			break
		}
		out.attempts = i
		res, err := tool.Invoke(callCtx, lease.IdempotencyKey+":verify:"+strconv.Itoa(i), call.Args)
		if err != nil {
			out.verdict, out.detail = domain.VerifyError, "verify tool error: "+truncate(err.Error(), 300)
			continue
		}
		out.output = res
		if expect == nil {
			out.verdict, out.detail = domain.VerifyVerified, "verify tool returned without error"
			return out, lc
		}
		v, err := expect.Eval(map[string]any{"args": decodeAny(fu.OfArgs), "action": actionOut, "output": decodeAny(res)})
		if err != nil {
			out.verdict, out.detail = domain.VerifyError, "expect: "+err.Error()
			continue
		}
		if flow.Truthy(v) {
			out.verdict, out.detail = domain.VerifyVerified, "expectation holds: "+lc.Expect
			return out, lc
		}
		out.verdict, out.detail = domain.VerifyRefuted, "expectation does not hold: "+lc.Expect
	}
	return out, lc
}

// verifyStepResult turns a verification into the step result: an
// observation with the verdict, a verification thought and, when refuted, a
// compensation proposal behind approval.
func (w *Worker) verifyStepResult(ctx context.Context, lease domain.Lease, rules profile.Rules, call domain.ToolCall,
	desc tools.Descriptor, vo verifyOutcome, lc *tools.Lifecycle, tail []domain.PlannedStep) (domain.StepSuccess, error) {
	fu := call.Followup
	ver := &domain.VerificationResult{OfTool: fu.OfTool, OfStep: fu.OfStep, Verdict: vo.verdict, Attempts: vo.attempts,
		Detail: vo.detail}
	if lc != nil {
		ver.Expect = lc.Expect
	}
	trust := desc.Trust
	if trust == "" {
		trust = "T2"
	}
	obs := domain.Observation{StepSeq: lease.StepSeq, Tool: call.Name, Args: call.Args, Outcome: "ok", TrustTag: trust,
		Verification: ver}
	if vo.verdict == domain.VerifyError && len(vo.output) == 0 {
		obs.Outcome = "error"
	}
	content := vo.output
	if len(content) == 0 {
		content = mustJSON(map[string]string{"verification": vo.verdict, "detail": vo.detail})
	}
	plan := tail
	summary := map[string]any{"tool": call.Name, "verification": vo.verdict, "of_tool": fu.OfTool, "attempts": vo.attempts}
	if vo.verdict == domain.VerifyRefuted && lc != nil && lc.Compensate != nil {
		args, err := lifecycleArgs(lc.Compensate, fu.OfArgs, decodeAny(vo.output))
		if err != nil {
			ver.Detail += "; compensation not planned: " + err.Error()
		} else {
			ccall := domain.ToolCall{Name: lc.Compensate.Tool, Args: args, Followup: &domain.ToolFollowup{
				Role: domain.FollowupCompensate, OfTool: fu.OfTool, OfStep: fu.OfStep, OfArgs: fu.OfArgs}}
			step, pd, err := w.followupStep(ctx, lease, rules, ccall, true)
			if err != nil {
				return domain.StepSuccess{}, err
			}
			if pd.Verdict == policy.Deny {
				ver.Detail += "; compensation " + lc.Compensate.Tool + " denied by policy: " + pd.Reason
				plan = append([]domain.PlannedStep{step}, tail...)
			} else {
				// Waiting for approval: approval runs it, rejection resumes
				// reasoning (the store plans the next reason step).
				ver.Compensation = lc.Compensate.Tool
				plan = []domain.PlannedStep{step}
				summary["compensation"] = lc.Compensate.Tool
			}
		}
	}
	obs, err := w.observe(ctx, lease.TenantID, desc, obs, content)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	ref, h, err := w.put(ctx, lease.TenantID, "observation", obs)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	status := domain.ThoughtUnverified
	switch vo.verdict {
	case domain.VerifyVerified:
		status = domain.ThoughtVerified
	case domain.VerifyRefuted:
		status = domain.ThoughtRefuted
	}
	verifyRef := tools.CapabilityRef(desc)
	thoughts := []domain.Thought{{Seq: 1, Kind: domain.ThoughtVerification, Status: status,
		Summary: truncate(fmt.Sprintf("%s of %s (step %d): %s", vo.verdict, fu.OfTool, fu.OfStep, vo.detail), 1000),
		Refs:    []domain.ThoughtRef{{Ref: verifyRef, Relation: "uses"}}}}
	if ver.Compensation != "" {
		thoughts = append(thoughts, domain.Thought{Seq: 2, Kind: domain.ThoughtAction, Status: domain.ThoughtUnknown,
			Summary: "Propose compensation " + ver.Compensation + " (awaiting approval)"})
	}
	return domain.StepSuccess{OutputRef: ref, OutputHash: h, ArtifactKind: "observation", Plan: plan, Thoughts: thoughts,
		Capabilities: []domain.CapabilityRef{{Ref: verifyRef, Role: "used"}}, Summary: mustJSON(summary)}, nil
}
