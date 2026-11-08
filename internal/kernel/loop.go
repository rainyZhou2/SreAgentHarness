package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/artifactq"
	"go-sre-harness/internal/untrusted"
)

// Loop defaults.
const (
	DefaultMaxRepeats       = 1
	DefaultStallSteps       = 4
	DefaultMaxParallelTools = 4
	DefaultMaxOutputRepairs = 2
	// budgetSoftRatio is the share of a run budget after which the agent is
	// told to conclude.
	budgetSoftRatio = 0.8
)

// BatchToolName is the kernel-owned step that runs several read-only calls
// of one model response together.
const BatchToolName = domain.BatchToolName

// RunUsageReader reports a run's model spend (both stores implement it).
type RunUsageReader interface {
	RunUsage(ctx context.Context, tenantID, runID string) (domain.RunUsage, error)
}

// loopLimits are the effective limits of one run: the process
// configuration, tightened by the profile.
type loopLimits struct {
	maxCostMicros int64
	maxTokens     int64
	maxRepeats    int // < 0: unlimited
	stall         int // <= 0: off
	parallel      int
}

func (w *Worker) loopLimits(rules profile.Rules) loopLimits {
	l := loopLimits{maxCostMicros: int64(w.cfg.RunMaxCostUSD * 1e6), maxTokens: w.cfg.RunMaxTokens,
		maxRepeats: w.cfg.MaxRepeats, stall: w.cfg.StallSteps, parallel: w.cfg.MaxParallelTools}
	if l.maxRepeats == 0 {
		l.maxRepeats = DefaultMaxRepeats
	}
	if l.stall == 0 {
		l.stall = DefaultStallSteps
	}
	if l.parallel == 0 {
		l.parallel = DefaultMaxParallelTools
	}
	tighter64 := func(cur, p int64) int64 {
		if p > 0 && (cur <= 0 || p < cur) {
			return p
		}
		return cur
	}
	if p := rules.RunLimits; p != nil {
		l.maxCostMicros = tighter64(l.maxCostMicros, int64(p.MaxCostUSD*1e6))
		l.maxTokens = tighter64(l.maxTokens, p.MaxTokens)
		if p.MaxRepeats != nil && (l.maxRepeats < 0 || *p.MaxRepeats < l.maxRepeats) {
			l.maxRepeats = *p.MaxRepeats
		}
		if p.StallSteps > 0 && (l.stall <= 0 || p.StallSteps < l.stall) {
			l.stall = p.StallSteps
		}
		if p.MaxParallelTools > 0 && p.MaxParallelTools < l.parallel {
			l.parallel = p.MaxParallelTools
		}
	}
	if l.parallel < 1 {
		l.parallel = 1
	}
	return l
}

// ---------- budget ----------

type budgetState struct {
	usage     domain.RunUsage
	ratio     float64 // the larger of cost and token use, as a share of the limit
	exhausted bool
	reason    string
}

// budget reports the run's spend against its limits. Without a limit or a
// usage reader it reports nothing; a read failure never blocks the step.
func (w *Worker) budget(ctx context.Context, lease domain.Lease, l loopLimits) budgetState {
	var b budgetState
	r, ok := w.store.(RunUsageReader)
	if !ok || (l.maxCostMicros <= 0 && l.maxTokens <= 0) {
		return b
	}
	u, err := r.RunUsage(ctx, lease.TenantID, lease.RunID)
	if err != nil {
		w.log.Warn("run usage", "run_id", lease.RunID, "err", err)
		return b
	}
	b.usage = u
	if l.maxCostMicros > 0 {
		if q := float64(u.CostMicros) / float64(l.maxCostMicros); q > b.ratio {
			b.ratio, b.reason = q, fmt.Sprintf("model cost $%.4f of $%.4f", float64(u.CostMicros)/1e6, float64(l.maxCostMicros)/1e6)
		}
	}
	if l.maxTokens > 0 {
		if q := float64(u.InputTokens+u.OutputTokens) / float64(l.maxTokens); q > b.ratio {
			b.ratio, b.reason = q, fmt.Sprintf("%d of %d model tokens", u.InputTokens+u.OutputTokens, l.maxTokens)
		}
	}
	b.exhausted = b.ratio >= 1
	return b
}

// kernelConclusion is the final decision the kernel writes when the run must
// stop without another model call. It is never presented as verified.
func kernelConclusion(in cognitive.ReasoningInput, why string) domain.Decision {
	var b strings.Builder
	fmt.Fprintf(&b, "The investigation was stopped before a conclusion: %s.\n", why)
	if len(in.Ledger) > 0 {
		b.WriteString("\nHypotheses at that point:\n")
		for _, e := range in.Ledger {
			fmt.Fprintf(&b, "- %s [%s, %.2f]: %s\n", e.ID, e.Status, e.Confidence, truncate(e.Statement, 200))
		}
	}
	if n := len(in.Observations); n > 0 {
		b.WriteString("\nLast tool results:\n")
		for _, o := range in.Observations[max(0, n-3):] {
			fmt.Fprintf(&b, "- %s %s (%s)\n", cognitive.ObsPartRef(o.StepSeq, o.Part), o.Tool, o.Outcome)
		}
	}
	b.WriteString("\nNothing here was independently verified; continue the investigation in a new run if needed.")
	return domain.Decision{SchemaVersion: 1, Kind: domain.DecisionFinal, Intent: domain.IntentTask,
		Answer: b.String(), ReasonSummary: "kernel stop: " + why,
		Thought: &domain.ThoughtChain{Plan: "stop: " + why, Structured: true,
			Verification: &domain.Verification{Verdict: "UNVERIFIED", Observed: why}}}
}

// ---------- loop guard ----------

// callKey identifies a tool call by name and canonical arguments.
func callKey(name string, args json.RawMessage) string {
	var v any
	if json.Unmarshal(args, &v) == nil {
		if b, err := json.Marshal(v); err == nil {
			return name + "\x00" + string(b)
		}
	}
	return name + "\x00" + string(args)
}

// loopState summarizes the run's earlier calls for the guard.
type loopState struct {
	runs    map[string][]int64 // call key → steps that ran it
	blocked map[string]int64   // call key → step where it was denied, rejected or expired
	stall   int                // trailing tool results that brought nothing new
}

func newLoopState(calls []cognitive.CallRecord) loopState {
	s := loopState{runs: map[string][]int64{}, blocked: map[string]int64{}}
	seen := map[string]bool{}
	for _, c := range calls {
		switch c.Outcome {
		case "blocked", "denied", "rejected", "expired":
			s.blocked[c.Key] = c.Step
			continue
		}
		s.runs[c.Key] = append(s.runs[c.Key], c.Step)
		if c.ResultHash == "" || seen[c.ResultHash] {
			s.stall++
		} else {
			s.stall = 0
		}
		if c.ResultHash != "" {
			seen[c.ResultHash] = true
		}
	}
	return s
}

// refuse returns why a call must not run again, or "".
func (s loopState) refuse(call domain.ToolCall, maxRepeats int) string {
	k := callKey(call.Name, call.Args)
	if step, ok := s.blocked[k]; ok {
		return fmt.Sprintf("this exact call was already denied, rejected or expired at step %d; do not retry it, choose a different safe step or conclude", step)
	}
	if prev := s.runs[k]; maxRepeats >= 0 && len(prev) > maxRepeats {
		refs := make([]string, 0, len(prev))
		for _, p := range prev {
			refs = append(refs, cognitive.ObsRef(p))
		}
		return fmt.Sprintf("this exact call already ran %d time(s) (%s); reuse that result instead of calling it again, or change the arguments",
			len(prev), strings.Join(refs, ", "))
	}
	return ""
}

// refusedStep records a call the loop guard did not run, so the next
// reasoning step sees why.
func (w *Worker) refusedStep(ctx context.Context, lease domain.Lease, call domain.ToolCall, why string) (domain.PlannedStep, error) {
	ref, hash, err := w.put(ctx, lease.TenantID, "tool-input", call)
	if err != nil {
		return domain.PlannedStep{}, err
	}
	pd := policy.Decision{Verdict: policy.Deny, Reason: "loop guard: " + why}
	return domain.PlannedStep{Kind: domain.StepTool, Executor: "tool", InputRef: ref, InputHash: hash,
		PolicyDecision: mustJSON(pd), Status: domain.StepSkipped, StatusReason: "loop guard: " + why}, nil
}

// ---------- repair ----------

// repairProblem returns the correction to send back for a decision the
// kernel cannot use, or "" when the decision (or error) is not about the
// model's output.
func repairProblem(d domain.Decision, err error) string {
	if err != nil {
		var io *cognitive.InvalidOutputError
		if errors.As(err, &io) {
			return io.Error()
		}
		if ge, ok := gw.AsError(err); ok && ge.Code == gw.ErrInvalidToolCall {
			return ge.Msg
		}
		return ""
	}
	if verr := cognitive.Validate(d); verr != nil {
		return verr.Error()
	}
	return ""
}

// ---------- parallel read-only batch ----------

// batchCall is one call of a batch with the registry facts it was planned
// against.
type batchCall struct {
	Name        string           `json:"name"`
	Args        json.RawMessage  `json:"args"`
	ToolVersion string           `json:"tool_version"`
	Risk        domain.RiskClass `json:"risk"`
}

type batchArgs struct {
	Calls []batchCall `json:"calls"`
}

// batchResult is the output artifact of a batch step.
type batchResult struct {
	Observations []domain.Observation `json:"observations"`
}

// BatchTool is the registry entry of the batch step. The kernel hides it
// from the model and refuses calls it did not plan.
type BatchTool struct{}

func (BatchTool) Descriptor() tools.Descriptor {
	return tools.Descriptor{Name: BatchToolName, Version: "1", Risk: domain.RiskR0, Trust: "T2", Source: "native",
		Description: "Kernel-owned: runs several read-only calls of one model response together.",
		InputSchema: json.RawMessage(`{"type":"object"}`)}
}

func (BatchTool) Invoke(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	return nil, errors.New("the batch step is executed by the kernel")
}

// planBatch returns the read-only calls of a response that may run together
// with the first one (which the caller already accepted), and notes for the
// calls left out. A batch needs at least two calls.
func (w *Worker) planBatch(ctx context.Context, lease domain.Lease, rules profile.Rules, in cognitive.ReasoningInput,
	visible []tools.Descriptor, first domain.ToolCall, firstDesc *tools.Descriptor, deferred []domain.ToolCall, l loopLimits, ls loopState) ([]batchCall, []string) {
	if l.parallel < 2 || len(deferred) == 0 || firstDesc == nil || firstDesc.Risk.HasSideEffects() {
		return nil, deferredNotes(deferred, "runs only when the first call is a read-only call")
	}
	calls := []batchCall{{Name: first.Name, Args: first.Args, ToolVersion: firstDesc.Version, Risk: firstDesc.Risk}}
	var notes []string
	seen := map[string]bool{callKey(first.Name, first.Args): true}
	for _, c := range deferred {
		reason := ""
		var desc tools.Descriptor
		t, ok := w.tools.LookupFor(lease.TenantID, c.Name)
		switch {
		case len(calls) >= l.parallel:
			reason = fmt.Sprintf("at most %d calls run together", l.parallel)
		case !ok || c.Name == BatchToolName || tools.IsBuiltin(c.Name) && c.Name != artifactq.Name:
			reason = "not a tool that can run in a batch"
		case !visibleTool(visible, c.Name):
			reason = "not visible to this agent"
		case len(c.Args) == 0 || !json.Valid(c.Args):
			reason = "arguments are not valid JSON"
		case seen[callKey(c.Name, c.Args)]:
			reason = "duplicate of another call in the same response"
		}
		if reason == "" {
			raw := t.Descriptor()
			c.Args = fillTimeArgs(&raw, c.Args, in.Time)
			pd, eff := rules.DecideCall(ctx, w.policy, profile.Call{TenantID: lease.TenantID, RunID: lease.RunID,
				Name: c.Name, Args: c.Args}, &raw)
			switch {
			case eff != nil && eff.Risk.HasSideEffects():
				reason = "has side effects: request it on its own so it can be approved"
			case eff == nil || pd.Verdict != policy.Allow:
				reason = "not allowed without approval: request it on its own"
			default:
				desc = *eff
				reason = ls.refuse(c, l.maxRepeats)
			}
		}
		if reason != "" {
			notes = append(notes, c.Name+": "+reason)
			continue
		}
		seen[callKey(c.Name, c.Args)] = true
		calls = append(calls, batchCall{Name: c.Name, Args: c.Args, ToolVersion: desc.Version, Risk: desc.Risk})
	}
	if len(calls) < 2 {
		return nil, notes
	}
	return calls, notes
}

func deferredNotes(cs []domain.ToolCall, why string) []string {
	var out []string
	for _, c := range cs {
		out = append(out, c.Name+": "+why)
	}
	return out
}

func visibleTool(ds []tools.Descriptor, name string) bool {
	for _, d := range ds {
		if d.Name == name {
			return true
		}
	}
	return false
}

// executeBatch runs the calls of a batch step concurrently. Each call is
// re-checked against the registry; a call that changed, is no longer
// allowed or fails becomes an error observation of its own, so one bad call
// never fails the others.
func (w *Worker) executeBatch(ctx context.Context, lease domain.Lease, rules profile.Rules, call domain.ToolCall,
	plan []domain.PlannedStep) (domain.StepSuccess, error) {
	var ba batchArgs
	if err := json.Unmarshal(call.Args, &ba); err != nil || len(ba.Calls) == 0 {
		return domain.StepSuccess{}, fmt.Errorf("batch step: %w", domain.ErrNotDispatched)
	}
	var off map[string]bool
	if w.cfg.ToolToggles != nil {
		o, err := w.cfg.ToolToggles.DisabledTools(ctx, lease.TenantID)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		off = o
	}
	obs := make([]domain.Observation, len(ba.Calls))
	var wg sync.WaitGroup
	for i, bc := range ba.Calls {
		base := domain.Observation{StepSeq: lease.StepSeq, Part: i + 1, Tool: bc.Name, Args: bc.Args}
		t, ok := w.tools.LookupFor(lease.TenantID, bc.Name)
		var desc tools.Descriptor
		if ok {
			desc = rules.Effective(t.Descriptor())
		}
		problem := ""
		switch {
		case !ok:
			problem = "tool is no longer registered"
		case !rules.AllowsTool(bc.Name) || off[bc.Name]:
			problem = "tool is no longer allowed"
		case desc.Version != bc.ToolVersion || desc.Risk != bc.Risk || desc.Risk.HasSideEffects():
			problem = "tool changed since the batch was planned"
		}
		if problem != "" {
			base.Outcome, base.TrustTag = "blocked", "T2"
			base.Content = mustJSON(map[string]string{"error": problem})
			obs[i] = base
			continue
		}
		wg.Add(1)
		go func(i int, bc batchCall, t tools.Tool, desc tools.Descriptor, base domain.Observation) {
			defer wg.Done()
			obs[i] = w.invokeReadOnly(ctx, lease, t, desc, bc.Name, bc.Args, base)
		}(i, bc, t, desc, base)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return domain.StepSuccess{}, ctx.Err()
	}
	ref, hash, err := w.put(ctx, lease.TenantID, "observation", batchResult{Observations: obs})
	if err != nil {
		return domain.StepSuccess{}, err
	}
	size := 0
	for _, o := range obs {
		size += len(o.Content)
	}
	plan = w.withTokenCompaction(ctx, lease, plan, size)
	var thoughts []domain.Thought
	var caps []domain.CapabilityRef
	summary := map[string]any{"tool": BatchToolName, "calls": len(obs)}
	var failed []string
	for i, o := range obs {
		desc := tools.Descriptor{Name: o.Tool}
		if t, ok := w.tools.LookupFor(lease.TenantID, o.Tool); ok {
			desc = t.Descriptor()
		}
		th, cs := toolTrace(o.Tool, desc, o.Outcome == "ok")
		for _, x := range th {
			x.Seq = len(thoughts) + 1
			x.Summary = fmt.Sprintf("[%d] %s", i+1, x.Summary)
			thoughts = append(thoughts, x)
		}
		caps = append(caps, cs...)
		if o.Outcome != "ok" {
			failed = append(failed, o.Tool)
		}
	}
	if len(failed) > 0 {
		summary["failed"] = failed
	}
	return domain.StepSuccess{OutputRef: ref, OutputHash: hash, ArtifactKind: "observation", Plan: plan,
		Thoughts: thoughts, Capabilities: caps, Summary: mustJSON(summary)}, nil
}

// invokeReadOnly calls a read-only tool and turns the result, or a
// permanent error, into an observation.
func (w *Worker) invokeReadOnly(ctx context.Context, lease domain.Lease, t tools.Tool, desc tools.Descriptor,
	name string, args json.RawMessage, base domain.Observation) domain.Observation {
	select {
	case w.toolSem <- struct{}{}:
	case <-ctx.Done():
		base.Outcome, base.Content = "error", mustJSON(map[string]string{"error": ctx.Err().Error()})
		return base
	}
	callCtx := tools.WithCallInfo(ctx, tools.CallInfo{TenantID: lease.TenantID, RunID: lease.RunID, StepID: lease.StepID,
		AttemptID: lease.AttemptID, FenceToken: lease.FenceToken, ContextSnapshotID: lease.ContextSnapshotID,
		InputRef: lease.InputRef, Deadline: lease.Deadline})
	key := lease.IdempotencyKey
	if base.Part > 0 {
		key = fmt.Sprintf("%s#%d", key, base.Part)
	}
	out, specOK := w.takeSpeculative(ctx, lease, desc, name, args)
	var err error
	if !specOK {
		out, err = t.Invoke(callCtx, key, args)
	}
	<-w.toolSem
	trust := desc.Trust
	if err != nil {
		w.cfg.Observer.ObserveTool(desc.Source, string(desc.Risk), "error")
		if trust == "" {
			trust = "T4"
		}
		base.Outcome, base.TrustTag = "error", trust
		base.Content = mustJSON(map[string]string{"error": err.Error()})
		base.Injection = untrusted.Scan(err.Error())
		return base
	}
	w.cfg.Observer.ObserveTool(desc.Source, string(desc.Risk), "ok")
	if trust == "" {
		trust = "T2"
	}
	base.Outcome, base.TrustTag = "ok", trust
	o, err := w.observe(ctx, lease.TenantID, desc, base, out)
	if err != nil {
		base.Outcome, base.Content = "error", mustJSON(map[string]string{"error": "store result: " + err.Error()})
		return base
	}
	return o
}

// resultHash identifies an observation's content for stall detection.
func resultHash(o domain.Observation) string {
	if o.Outcome != "ok" {
		return ""
	}
	if o.Artifact != nil && o.Artifact.Hash != "" {
		return o.Artifact.Hash
	}
	if len(o.Content) == 0 {
		return ""
	}
	return artifact.Hash(o.Content)
}

// sortedNotes keeps step summaries deterministic.
func sortedNotes(xs []string) []string {
	out := append([]string(nil), xs...)
	sort.Strings(out)
	return out
}

// ---------- advisories ----------

// loopAdvisories adds the kernel notices of this step to the input. It
// returns a stop reason when the kernel must conclude without asking the
// model, and a reason the model must conclude now (a tool call is then
// replaced by a kernel conclusion).
func (w *Worker) loopAdvisories(in *cognitive.ReasoningInput, b budgetState, ls loopState, l loopLimits) (stop, mustConclude string) {
	if b.exhausted {
		return "run budget exhausted (" + b.reason + ")", ""
	}
	if b.ratio >= budgetSoftRatio {
		in.Advisories = append(in.Advisories, fmt.Sprintf("Run budget %.0f%% used (%s). Conclude now with the evidence you have; the run stops at 100%%.", b.ratio*100, b.reason))
		in.ForceConclude = true
	}
	if l.stall > 0 && ls.stall >= l.stall {
		if ls.stall >= 2*l.stall {
			mustConclude = fmt.Sprintf("no new information in the last %d tool results", ls.stall)
			in.ForceConclude = true
		}
		in.Advisories = append(in.Advisories, fmt.Sprintf("The last %d tool results brought nothing new (repeats, errors or identical output). Change approach — a different tool, scope or hypothesis — or conclude.", ls.stall))
	}
	return "", mustConclude
}

// unknownToolProblem reports a tool call naming a tool the model was not
// shown, listing the ones it may call.
func unknownToolProblem(d domain.Decision, visible []tools.Descriptor) string {
	if d.Kind != domain.DecisionTool || d.Tool == nil || visibleTool(visible, d.Tool.Name) {
		return ""
	}
	names := make([]string, 0, len(visible))
	for _, t := range visible {
		names = append(names, t.Name)
	}
	sort.Strings(names)
	if len(names) > 40 {
		names = append(names[:40], "…")
	}
	return fmt.Sprintf("tool %q does not exist or is not available to you. Available tools: %s.", d.Tool.Name, strings.Join(names, ", "))
}

// batchObservations expands a batch step of the history into the
// observations of its calls.
func (w *Worker) batchObservations(ctx context.Context, tenantID string, s domain.Step, call domain.ToolCall) ([]domain.Observation, error) {
	var ba batchArgs
	_ = json.Unmarshal(call.Args, &ba)
	if s.Status == domain.StepSucceeded && s.OutputRef != nil {
		var r batchResult
		if err := w.get(ctx, tenantID, *s.OutputRef, &r); err != nil {
			return nil, err
		}
		for i := range r.Observations {
			r.Observations[i].StepSeq = s.Seq
			r.Observations[i].Part = i + 1
		}
		return r.Observations, nil
	}
	outcome := "blocked"
	if s.Status == domain.StepFailed {
		outcome = "failed"
	}
	note := ""
	if s.StatusReason != nil {
		note = *s.StatusReason
	}
	out := make([]domain.Observation, 0, len(ba.Calls))
	for i, c := range ba.Calls {
		out = append(out, domain.Observation{StepSeq: s.Seq, Part: i + 1, Tool: c.Name, Args: c.Args,
			Outcome: outcome, Note: note, TrustTag: "T2"})
	}
	return out, nil
}
