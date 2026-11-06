// Package kernel is the Durable Step Engine worker (§4, §14, §15.2). It
// depends only on domain contracts and adapter interfaces: no SQL, no model
// SDK, no NATS (§20.1).
package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/tools"
)

// StateStore is the durable state the worker needs (implemented by store/pg).
type StateStore interface {
	ClaimStep(ctx context.Context, runID, workerID string, ttl time.Duration) (domain.Lease, error)
	RenewLease(ctx context.Context, lease domain.Lease, ttl time.Duration) (domain.DesiredState, error)
	CommitSuccess(ctx context.Context, lease domain.Lease, res domain.StepSuccess) error
	CommitFailure(ctx context.Context, lease domain.Lease, f domain.StepFailure) error
	PrepareEffect(ctx context.Context, lease domain.Lease, target string) (domain.EffectState, string, error)
	History(ctx context.Context, tenantID, runID string, beforeSeq int64) ([]domain.Step, error)
	SnapshotKVVersions(ctx context.Context, tenantID, snapshotID string) (map[string]int64, error)
	KVDataAt(ctx context.Context, key domain.KVKey, version int64) ([]byte, error)
}

// ConversationReader is implemented by stores that persist conversation
// turns; the worker renders earlier turns of the branch into the context.
type ConversationReader interface {
	RunConversation(ctx context.Context, tenantID, runID string) ([]domain.Message, error)
}

// ContextExtras lets the store contribute snapshot-pinned context: memory
// hits and the compaction summary (§11, §33.4).
type ContextExtras interface {
	SnapshotContext(ctx context.Context, tenantID, runID, snapshotID string, beforeSeq int64) (SnapshotContext, error)
}

type SnapshotContext struct {
	Memories        []cognitive.MemoryRef
	Summary         string
	CompactedBefore int64
	CompactionEpoch int64
}

type Config struct {
	ID                string
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
	StepTimeout       time.Duration
	CommitTimeout     time.Duration
	MaxLocalSteps     int // steps executed back-to-back for one wake-up before yielding
	MaxReasonInflight int // resource-class pools (§37.3)
	MaxToolInflight   int
	// CompactEvery inserts a compact step once this many tool observations
	// accumulated since the last compaction (0 = default 6, <0 = never).
	CompactEvery int
	// Compactor writes compaction summaries (default DeterministicCompactor).
	Compactor Compactor
	// Checkpoints enables per-step checkpoint manifests (§29.4).
	Checkpoints bool
	// Memory, when set, adds a retrieve_memory step before the first
	// reasoning step of a run (§11.2).
	Memory MemoryRetriever
	// Strategies, when set, selects the strategy from the run's pinned
	// profile version (canary rollouts, §32); otherwise the default is used.
	Strategies StrategyResolver
	// Observer receives low-cardinality execution metrics (optional).
	Observer Observer
	// Rules enforces the run's pinned profile (tool allowlist, risk
	// overrides, approval rules, policy pin, compaction and memory limits).
	// Nil means unrestricted (tests and runs without a profile).
	Rules RulesSource
	// Skills supplies the tenant's skills rendered into the system prompt
	// (optional).
	Skills SkillSource
	// ToolToggles hides tools the tenant switched off (optional).
	ToolToggles ToolToggles
}

// SkillSource returns a tenant's enabled skills.
type SkillSource interface {
	Skills(tenantID string) []cognitive.Skill
}

// RulesSource returns the compiled rules of an immutable profile version.
type RulesSource interface {
	Rules(ctx context.Context, tenantID, profileID string, version int64) (profile.Rules, error)
}

// Observer is implemented by the metrics backend.
type Observer interface {
	ObserveStep(kind, outcome string, d time.Duration)
	ObserveClaim(result string)
	ObserveTool(source, risk, outcome string)
}

type nopObserver struct{}

func (nopObserver) ObserveStep(string, string, time.Duration) {}
func (nopObserver) ObserveClaim(string)                       {}
func (nopObserver) ObserveTool(string, string, string)        {}

type StrategyResolver interface {
	StrategyFor(ctx context.Context, tenantID, profileID string, version int64) (cognitive.Strategy, error)
}

// MemoryRetriever executes retrieve_memory steps (§11). The returned hits are
// committed with the step and pinned for the following reason steps.
type MemoryRetriever interface {
	Retrieve(ctx context.Context, lease domain.Lease, goal string) (domain.StepSuccess, error)
}

// ToolToggles reports the tools a tenant switched off. It is applied on top
// of the profile allowlist and can only hide tools.
type ToolToggles interface {
	DisabledTools(ctx context.Context, tenantID string) (map[string]bool, error)
}

func (c *Config) defaults() {
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 45 * time.Second
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = c.LeaseTTL / 4
	}
	if c.StepTimeout == 0 {
		c.StepTimeout = 2 * time.Minute
	}
	if c.CommitTimeout == 0 {
		c.CommitTimeout = 10 * time.Second
	}
	if c.MaxLocalSteps == 0 {
		c.MaxLocalSteps = 20
	}
	if c.MaxReasonInflight == 0 {
		c.MaxReasonInflight = 50
	}
	if c.MaxToolInflight == 0 {
		c.MaxToolInflight = 20
	}
	if c.CompactEvery == 0 {
		c.CompactEvery = 6
	}
	if c.Compactor == nil {
		c.Compactor = DeterministicCompactor{}
	}
	if c.Observer == nil {
		c.Observer = nopObserver{}
	}
}

type Worker struct {
	cfg       Config
	store     StateStore
	artifacts artifact.Store
	strategy  cognitive.Strategy
	tools     *tools.Registry
	policy    policy.Gate
	log       *slog.Logger
	reasonSem chan struct{}
	toolSem   chan struct{}
}

func NewWorker(cfg Config, st StateStore, arts artifact.Store, strat cognitive.Strategy, reg *tools.Registry,
	pol policy.Gate, log *slog.Logger) *Worker {
	cfg.defaults()
	if log == nil {
		log = slog.Default()
	}
	return &Worker{cfg: cfg, store: st, artifacts: arts, strategy: strat, tools: reg, policy: policy.Guarded(pol),
		log: log.With("worker", cfg.ID), reasonSem: make(chan struct{}, cfg.MaxReasonInflight),
		toolSem: make(chan struct{}, cfg.MaxToolInflight)}
}

func (w *Worker) ID() string { return w.cfg.ID }

// Disposition tells the transport whether to ack a notification.
type Disposition struct {
	Ack   bool
	Delay time.Duration // redelivery delay when !Ack
}

// HandleNotification processes a wake-up for one run. Notifications are not
// facts: "nothing to do", "someone else holds the lease" and "lease lost" are
// all acked; only infrastructure errors are redelivered (§15.2).
func (w *Worker) HandleNotification(ctx context.Context, runID string) Disposition {
	for i := 0; i < w.cfg.MaxLocalSteps; i++ {
		err := w.ExecuteOne(ctx, runID)
		switch {
		case err == nil:
			continue // the run may have more work; claim it locally right away
		case errors.Is(err, domain.ErrNoRunnableStep), errors.Is(err, domain.ErrLeaseHeld),
			errors.Is(err, domain.ErrLeaseLost), errors.Is(err, domain.ErrNotFound):
			return Disposition{Ack: true}
		case ctx.Err() != nil:
			return Disposition{Ack: false}
		default:
			w.log.Error("execute step", "run_id", runID, "err", err)
			return Disposition{Ack: false, Delay: 2 * time.Second}
		}
	}
	return Disposition{Ack: true} // remaining work already has an outbox wake-up
}

// ExecuteOne runs exactly one step: claim (short tx) → execute outside any
// transaction under lease heartbeat → fenced commit (short tx).
func (w *Worker) ExecuteOne(ctx context.Context, runID string) (err error) {
	lease, err := w.store.ClaimStep(ctx, runID, w.cfg.ID, w.cfg.LeaseTTL)
	if err != nil {
		w.cfg.Observer.ObserveClaim(claimResult(err))
		return err
	}
	w.cfg.Observer.ObserveClaim("leased")
	started := time.Now()
	ctx, span := obsTracer().Start(ctx, "step "+string(lease.Kind), trace.WithAttributes(
		attribute.String("tenant_id", lease.TenantID), attribute.String("run_id", lease.RunID),
		attribute.String("step_id", lease.StepID), attribute.Int64("step_seq", lease.StepSeq),
		attribute.Int("attempt", lease.AttemptNo), attribute.Int64("fence_token", lease.FenceToken),
		attribute.String("profile_id", lease.ProfileID), attribute.Int64("profile_version", lease.ProfileVersion)))
	outcome := "succeeded"
	defer func() {
		if err != nil && outcome == "succeeded" {
			outcome = "error"
		}
		w.cfg.Observer.ObserveStep(string(lease.Kind), outcome, time.Since(started))
		span.SetAttributes(attribute.String("outcome", outcome))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, outcome)
		}
		span.End()
	}()
	lease.Deadline = time.Now().Add(w.cfg.StepTimeout)
	log := w.log.With("run_id", lease.RunID, "step_seq", lease.StepSeq, "attempt", lease.AttemptNo, "kind", lease.Kind)

	execCtx, cancel := context.WithDeadline(ctx, lease.Deadline)
	defer cancel()
	hb := w.startHeartbeat(lease, cancel)

	st := &execState{}
	res, execErr := w.execute(execCtx, lease, st)
	hb.stop()

	if hb.lost.Load() {
		log.Warn("lease lost during execution; dropping result")
		outcome = "lease_lost"
		return domain.ErrLeaseLost
	}
	commitCtx, commitCancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.CommitTimeout)
	defer commitCancel()

	if execErr == nil && w.cfg.Checkpoints && !res.Complete {
		w.attachCheckpoint(ctx, lease, &res)
	}
	if execErr != nil {
		deadline := errors.Is(execCtx.Err(), context.DeadlineExceeded)
		// The worker's own context ended (process shutdown), not the step.
		shutdown := ctx.Err() != nil && !deadline
		f := classify(execErr, st, hb.cancelRequested.Load(), deadline, shutdown)
		log.Info("step failed", "class", f.Class, "code", f.Code, "err", execErr)
		outcome = failureOutcome(f.Class)
		return w.store.CommitFailure(commitCtx, lease, f)
	}
	return w.store.CommitSuccess(commitCtx, lease, res)
}

type execState struct {
	dispatched bool // a side-effecting call may have reached the external system
}

func classify(err error, st *execState, cancelRequested, deadline, shutdown bool) domain.StepFailure {
	msg := err.Error()
	var re *domain.RetryableError
	switch {
	case errors.Is(err, domain.ErrUnknownEffect):
		return domain.StepFailure{Class: domain.FailUnknownEffect, Code: "EFFECT_UNKNOWN", Message: msg}
	case shutdown && st.dispatched:
		return domain.StepFailure{Class: domain.FailUnknownEffect, Code: "SHUTDOWN_AFTER_DISPATCH", Message: msg}
	case shutdown && !cancelRequested:
		// Stopping a worker is not the step's fault: another worker retries it.
		return domain.StepFailure{Class: domain.FailRetryable, Code: "WORKER_SHUTDOWN", Message: msg}
	case errors.Is(err, domain.ErrNotDispatched) && !deadline && !cancelRequested:
		return domain.StepFailure{Class: domain.FailPermanent, Code: "TOOL_REJECTED_BEFORE_DISPATCH", Message: msg}
	case cancelRequested && st.dispatched:
		return domain.StepFailure{Class: domain.FailUnknownEffect, Code: "CANCELLED_AFTER_DISPATCH", Message: msg}
	case cancelRequested:
		return domain.StepFailure{Class: domain.FailCancelled, Code: "CANCELLED", Message: msg}
	case deadline && st.dispatched:
		return domain.StepFailure{Class: domain.FailUnknownEffect, Code: "TIMEOUT_OUTCOME_UNKNOWN", Message: msg, TimedOut: true}
	case deadline:
		return domain.StepFailure{Class: domain.FailRetryable, Code: "STEP_TIMEOUT", Message: msg, TimedOut: true}
	case errors.As(err, &re):
		// The executor asserts nothing was applied (e.g. 429 before send).
		return domain.StepFailure{Class: domain.FailRetryable, Code: "RETRYABLE", Message: msg, RetryAfter: re.RetryAfter}
	case st.dispatched:
		return domain.StepFailure{Class: domain.FailUnknownEffect, Code: "TOOL_ERROR_AFTER_DISPATCH", Message: msg}
	default:
		return domain.StepFailure{Class: domain.FailPermanent, Code: "STEP_ERROR", Message: msg}
	}
}

// ---------- heartbeat ----------

type heartbeat struct {
	lost            atomic.Bool
	cancelRequested atomic.Bool
	done            chan struct{}
	wg              sync.WaitGroup
}

func (h *heartbeat) stop() {
	close(h.done)
	h.wg.Wait()
}

// startHeartbeat renews the lease; on loss it cancels execution so the
// executor stops early, and on a cancel request it cancels cooperatively.
func (w *Worker) startHeartbeat(lease domain.Lease, cancel context.CancelFunc) *heartbeat {
	h := &heartbeat{done: make(chan struct{})}
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		t := time.NewTicker(w.cfg.HeartbeatInterval)
		defer t.Stop()
		lastRenew := time.Now()
		for {
			select {
			case <-h.done:
				return
			case <-t.C:
			}
			ctx, c := context.WithTimeout(context.Background(), w.cfg.HeartbeatInterval)
			desired, err := w.store.RenewLease(ctx, lease, w.cfg.LeaseTTL)
			c()
			switch {
			case errors.Is(err, domain.ErrLeaseLost):
				h.lost.Store(true)
				cancel()
				return
			case err != nil:
				// Transient DB error: keep trying, but never outlive our own lease.
				if time.Since(lastRenew) > w.cfg.LeaseTTL-w.cfg.HeartbeatInterval {
					h.lost.Store(true)
					cancel()
					return
				}
			default:
				lastRenew = time.Now()
				if desired == domain.DesiredCancelled && !h.cancelRequested.Load() {
					h.cancelRequested.Store(true)
					cancel()
				}
			}
		}
	}()
	return h
}

// ---------- executors ----------

func (w *Worker) execute(ctx context.Context, lease domain.Lease, st *execState) (domain.StepSuccess, error) {
	switch lease.Kind {
	case domain.StepReason:
		return w.executeReason(ctx, lease)
	case domain.StepTool:
		return w.executeTool(ctx, lease, st)
	case domain.StepCompact:
		return w.executeCompact(ctx, lease)
	case domain.StepRetrieve:
		if w.cfg.Memory == nil {
			return domain.StepSuccess{}, fmt.Errorf("retrieve_memory step but no memory retriever configured")
		}
		var g goalDoc
		if err := w.get(ctx, lease.TenantID, lease.GoalRef, &g); err != nil {
			return domain.StepSuccess{}, err
		}
		rules, err := w.rules(ctx, lease)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		return w.cfg.Memory.Retrieve(domain.WithMemoryTopK(ctx, rules.MemoryTopK), lease, g.Goal)
	}
	return domain.StepSuccess{}, fmt.Errorf("unknown step kind %q", lease.Kind)
}

// rules loads the enforced part of the run's pinned profile version.
func (w *Worker) rules(ctx context.Context, lease domain.Lease) (profile.Rules, error) {
	if w.cfg.Rules == nil || lease.ProfileID == "" {
		return profile.Unrestricted(), nil
	}
	r, err := w.cfg.Rules.Rules(ctx, lease.TenantID, lease.ProfileID, lease.ProfileVersion)
	if err != nil {
		return r, domain.Retryable(fmt.Errorf("load profile rules: %w", err), 0)
	}
	return r, nil
}

type goalDoc struct {
	Goal string `json:"goal"`
}

func (w *Worker) put(ctx context.Context, tenantID, kind string, v any) (string, string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", "", err
	}
	ref, h, err := w.artifacts.Put(ctx, tenantID, kind, data)
	if err != nil {
		// Never report success without a durable artifact (§4.4).
		return "", "", domain.Retryable(fmt.Errorf("artifact upload: %w", err), 0)
	}
	return ref, h, nil
}

func (w *Worker) get(ctx context.Context, tenantID, ref string, v any) error {
	data, err := w.artifacts.Get(ctx, tenantID, ref)
	if err != nil {
		return domain.Retryable(fmt.Errorf("artifact read %s: %w", ref, err), 0)
	}
	return json.Unmarshal(data, v)
}

func (w *Worker) executeReason(ctx context.Context, lease domain.Lease) (domain.StepSuccess, error) {
	in, err := w.renderContext(ctx, lease)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	canon, _ := json.Marshal(in)
	contextHash := artifact.Hash(canon)
	in.Call = cognitive.CallScope{TenantID: lease.TenantID, RunID: lease.RunID, StepID: lease.StepID,
		AttemptID: lease.AttemptID, WorkerID: lease.WorkerID, FenceToken: lease.FenceToken}

	select {
	case w.reasonSem <- struct{}{}:
	case <-ctx.Done():
		return domain.StepSuccess{}, ctx.Err()
	}
	strat := w.strategy
	if w.cfg.Strategies != nil && lease.ProfileID != "" {
		ps, err := w.cfg.Strategies.StrategyFor(ctx, lease.TenantID, lease.ProfileID, lease.ProfileVersion)
		if err != nil {
			<-w.reasonSem
			return domain.StepSuccess{}, domain.Retryable(fmt.Errorf("resolve profile strategy: %w", err), 0)
		}
		strat = ps
	}
	decision, err := strat.Decide(ctx, in)
	<-w.reasonSem
	if err != nil {
		return domain.StepSuccess{}, err
	}
	if err := cognitive.Validate(decision); err != nil {
		// Malformed model output: retry the reasoning, never execute it (§31.3).
		return domain.StepSuccess{}, domain.Retryable(fmt.Errorf("invalid decision: %w", err), 0)
	}
	decision.SchemaVersion = 1
	decision.ContextSnapshotID = lease.ContextSnapshotID
	if decision.Intent == "" {
		decision.Intent = domain.IntentTask
	}
	if decision.RenderedRequestHash != "" {
		// Model strategies bind the exact rendered request (§33.2).
		contextHash = decision.RenderedRequestHash
	}

	outRef, outHash, err := w.put(ctx, lease.TenantID, "decision", decision)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	res := domain.StepSuccess{OutputRef: outRef, OutputHash: outHash, ArtifactKind: "decision", ContextHash: contextHash}
	summary := map[string]any{"decision_kind": decision.Kind, "intent": decision.Intent, "reason_summary": truncate(decision.ReasonSummary, 500),
		"hypotheses": len(decision.Hypotheses)}
	if decision.ModelRequestID != "" {
		summary["model_request_id"], summary["model"], summary["manifest_ref"] = decision.ModelRequestID, decision.Model, decision.ManifestRef
	}

	nextReason := domain.PlannedStep{Kind: domain.StepReason, Executor: "reasoner", InputRef: lease.InputRef,
		InputHash: lease.InputHash, Status: domain.StepPending}

	if decision.Kind == domain.DecisionFinal {
		resultRef, _, err := w.put(ctx, lease.TenantID, "result", map[string]any{
			"answer": decision.Answer, "intent": decision.Intent, "hypotheses": decision.Hypotheses, "decision_ref": outRef})
		if err != nil {
			return domain.StepSuccess{}, err
		}
		res.Complete, res.ResultRef = true, resultRef
		res.Summary = mustJSON(summary)
		return res, nil
	}

	call := *decision.Tool
	callRef, callHash, err := w.put(ctx, lease.TenantID, "tool-input", call)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	rules, err := w.rules(ctx, lease)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	var reg *tools.Descriptor
	if t, ok := w.tools.LookupFor(lease.TenantID, call.Name); ok {
		d := t.Descriptor()
		reg = &d
	}
	// The model cannot influence the risk class; the profile can only tighten.
	pd, desc := rules.Decide(w.policy, call.Name, reg, call.Args)
	pdJSON := mustJSON(pd)
	toolStep := domain.PlannedStep{Kind: domain.StepTool, Executor: "tool", InputRef: callRef, InputHash: callHash,
		PolicyDecision: pdJSON}
	if desc != nil {
		intent := w.policy.BuildIntent(*desc, call, callHash, pd)
		toolStep.Intent = &intent
	}
	switch pd.Verdict {
	case policy.Allow:
		toolStep.Status = domain.StepPending
		res.Plan = []domain.PlannedStep{toolStep}
	case policy.RequireApproval:
		toolStep.Status, toolStep.RequiredApprovals = domain.StepWaitingApproval, pd.RequiredApprovals
		res.Plan = []domain.PlannedStep{toolStep}
	default:
		toolStep.Status, toolStep.StatusReason, toolStep.Intent = domain.StepSkipped, "denied by policy: "+pd.Reason, nil
		res.Plan = []domain.PlannedStep{toolStep, nextReason}
	}
	summary["tool"], summary["policy_verdict"] = call.Name, pd.Verdict
	res.Summary = mustJSON(summary)
	return res, nil
}

// renderContext is phase ② of the snapshot lifecycle: it reads only what the
// snapshot pinned (history before this step, KV versions), so a retry sees the
// same context even if evidence was injected meanwhile.
func (w *Worker) renderContext(ctx context.Context, lease domain.Lease) (cognitive.ReasoningInput, error) {
	rules, err := w.rules(ctx, lease)
	if err != nil {
		return cognitive.ReasoningInput{}, err
	}
	visible := rules.Visible(w.tools.DescriptorsFor(lease.TenantID))
	if w.cfg.ToolToggles != nil {
		off, err := w.cfg.ToolToggles.DisabledTools(ctx, lease.TenantID)
		if err != nil {
			return cognitive.ReasoningInput{}, err
		}
		visible = withoutDisabled(visible, off)
	}
	in := cognitive.ReasoningInput{StepSeq: lease.StepSeq, ContextSnapshotID: lease.ContextSnapshotID,
		Tools: visible}
	var g goalDoc
	if err := w.get(ctx, lease.TenantID, lease.GoalRef, &g); err != nil {
		return in, err
	}
	in.Goal = g.Goal

	history, err := w.store.History(ctx, lease.TenantID, lease.RunID, lease.StepSeq)
	if err != nil {
		return in, err
	}
	for _, s := range history {
		if s.Kind != domain.StepTool {
			continue
		}
		in.HasToolSteps = true
		var call domain.ToolCall
		if err := w.get(ctx, lease.TenantID, s.InputRef, &call); err != nil {
			return in, err
		}
		obs := domain.Observation{StepSeq: s.Seq, Tool: call.Name, Args: call.Args, TrustTag: "T2"}
		switch {
		case s.Status == domain.StepSucceeded && s.OutputRef != nil:
			var o domain.Observation
			if err := w.get(ctx, lease.TenantID, *s.OutputRef, &o); err != nil {
				return in, err
			}
			obs.Outcome, obs.Content = "ok", o.Content
		case s.Status == domain.StepSucceeded:
			obs.Outcome = "ok"
		case s.Status == domain.StepFailed:
			obs.Outcome = "failed"
		default:
			obs.Outcome = "blocked"
		}
		if s.StatusReason != nil {
			obs.Note = *s.StatusReason
		}
		in.Observations = append(in.Observations, obs)
	}

	if cr, ok := w.store.(ConversationReader); ok {
		msgs, err := cr.RunConversation(ctx, lease.TenantID, lease.RunID)
		if err != nil {
			return in, err
		}
		for _, m := range msgs {
			in.Conversation = append(in.Conversation, cognitive.ConversationMessage{Seq: m.Seq, Role: m.Role,
				Text: w.messageText(ctx, lease.TenantID, m)})
		}
	}
	if w.cfg.Skills != nil {
		// Manual skills apply once the chat mentions /name (goal or an
		// earlier user message); the others always apply.
		texts := []string{in.Goal}
		for _, m := range in.Conversation {
			if m.Role == "user" {
				texts = append(texts, m.Text)
			}
		}
		in.Skills = cognitive.SelectSkills(w.cfg.Skills.Skills(lease.TenantID), texts...)
	}
	if cx, ok := w.store.(ContextExtras); ok {
		sc, err := cx.SnapshotContext(ctx, lease.TenantID, lease.RunID, lease.ContextSnapshotID, lease.StepSeq)
		if err != nil {
			return in, err
		}
		in.Memories, in.Summary, in.CompactedBefore, in.CompactionEpoch = sc.Memories, sc.Summary, sc.CompactedBefore, sc.CompactionEpoch
		if sc.CompactedBefore > 0 {
			kept := in.Observations[:0]
			for _, o := range in.Observations {
				if o.StepSeq >= sc.CompactedBefore {
					kept = append(kept, o)
				}
			}
			in.Observations = kept
		}
	}

	versions, err := w.store.SnapshotKVVersions(ctx, lease.TenantID, lease.ContextSnapshotID)
	if err != nil {
		return in, err
	}
	keys := make([]string, 0, len(versions))
	for k := range versions {
		if strings.HasPrefix(k, "evidence/") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		data, err := w.store.KVDataAt(ctx, domain.KVKey{TenantID: lease.TenantID, Scope: "run", OwnerID: lease.RunID, Name: k}, versions[k])
		if err != nil {
			return in, err
		}
		var e domain.Evidence
		if json.Unmarshal(data, &e) == nil {
			e.Key, e.Version = k, versions[k]
			in.Evidence = append(in.Evidence, e) // operator evidence: T1 data, never instructions
		}
	}
	return in, nil
}

func (w *Worker) executeTool(ctx context.Context, lease domain.Lease, st *execState) (domain.StepSuccess, error) {
	var call domain.ToolCall
	if err := w.get(ctx, lease.TenantID, lease.InputRef, &call); err != nil {
		return domain.StepSuccess{}, err
	}
	tool, ok := w.tools.LookupFor(lease.TenantID, call.Name)
	if !ok || lease.Intent == nil {
		return domain.StepSuccess{}, fmt.Errorf("tool %q is not registered", call.Name)
	}
	rules, err := w.rules(ctx, lease)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	if !rules.AllowsTool(call.Name) {
		// Defense in depth: planning already denies this; nothing was sent.
		return domain.StepSuccess{}, fmt.Errorf("tool %q is not allowed by profile %s: %w", call.Name, rules.Ref, domain.ErrNotDispatched)
	}
	if w.cfg.ToolToggles != nil {
		off, err := w.cfg.ToolToggles.DisabledTools(ctx, lease.TenantID)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		if off[call.Name] {
			return domain.StepSuccess{}, fmt.Errorf("tool %q is disabled for this tenant: %w", call.Name, domain.ErrNotDispatched)
		}
	}
	desc := rules.Effective(tool.Descriptor())
	target := tools.Target(desc, call.Args)
	// The approved intent must still describe exactly what we are about to run.
	if desc.Version != lease.Intent.ToolVersion || desc.Risk != lease.Intent.RiskClass || target != lease.Intent.Target ||
		lease.Intent.InputHash != lease.InputHash {
		return domain.StepSuccess{}, fmt.Errorf("tool %q changed since the intent was approved", call.Name)
	}

	var goal goalDoc
	goalData, err := w.artifacts.Get(ctx, lease.TenantID, lease.GoalRef)
	if err != nil {
		return domain.StepSuccess{}, domain.Retryable(err, 0)
	}
	_ = json.Unmarshal(goalData, &goal)
	next := domain.PlannedStep{Kind: domain.StepReason, Executor: "reasoner", InputRef: lease.GoalRef,
		InputHash: artifact.Hash(goalData), Status: domain.StepPending}
	plan := []domain.PlannedStep{next}
	if w.needsCompaction(ctx, lease, rules.CompactEvery) {
		plan = []domain.PlannedStep{{Kind: domain.StepCompact, Executor: "compactor", InputRef: lease.GoalRef,
			InputHash: next.InputHash, Status: domain.StepPending}, next}
	}
	sideEffect := desc.Risk.HasSideEffects()

	if sideEffect {
		state, priorRef, err := w.store.PrepareEffect(ctx, lease, target)
		if err != nil {
			return domain.StepSuccess{}, err // ErrUnknownEffect: do not call the tool again
		}
		if state == domain.EffectConfirmed {
			// Already applied by an earlier attempt whose commit was lost: reuse.
			return domain.StepSuccess{OutputRef: priorRef, OutputHash: "", ArtifactKind: "observation",
				Plan: plan, SideEffect: true, Summary: mustJSON(map[string]any{"tool": call.Name, "reused": true})}, nil
		}
		st.dispatched = true
	}

	select {
	case w.toolSem <- struct{}{}:
	case <-ctx.Done():
		st.dispatched = false // not sent yet
		return domain.StepSuccess{}, ctx.Err()
	}
	callCtx := tools.WithCallInfo(ctx, tools.CallInfo{TenantID: lease.TenantID, RunID: lease.RunID, StepID: lease.StepID,
		AttemptID: lease.AttemptID, FenceToken: lease.FenceToken, ContextSnapshotID: lease.ContextSnapshotID,
		InputRef: lease.InputRef, Deadline: lease.Deadline})
	callCtx, span := obsTracer().Start(callCtx, "tool "+call.Name, trace.WithAttributes(
		attribute.String("tool", call.Name), attribute.String("risk", string(desc.Risk)), attribute.String("source", desc.Source)))
	out, err := tool.Invoke(callCtx, lease.IdempotencyKey, call.Args)
	<-w.toolSem
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "tool error")
		span.End()
		w.cfg.Observer.ObserveTool(desc.Source, string(desc.Risk), "error")
		return domain.StepSuccess{}, err
	}
	span.End()
	w.cfg.Observer.ObserveTool(desc.Source, string(desc.Risk), "ok")
	trust := desc.Trust
	if trust == "" {
		trust = "T2"
	}
	obs := domain.Observation{StepSeq: lease.StepSeq, Tool: call.Name, Outcome: "ok", Content: out, TrustTag: trust}
	ref, h, err := w.put(ctx, lease.TenantID, "observation", obs)
	if err != nil {
		if sideEffect {
			// The effect happened but we cannot persist proof: treat as unknown.
			return domain.StepSuccess{}, fmt.Errorf("%w: %v", domain.ErrUnknownEffect, err)
		}
		return domain.StepSuccess{}, err
	}
	return domain.StepSuccess{OutputRef: ref, OutputHash: h, ArtifactKind: "observation", SideEffect: sideEffect,
		Plan: plan, Summary: mustJSON(map[string]any{"tool": call.Name, "risk": desc.Risk})}, nil
}

// messageText renders a committed conversation message for the context.
func (w *Worker) messageText(ctx context.Context, tenantID string, m domain.Message) string {
	if strings.HasPrefix(m.ContentRef, "status:") {
		return "[previous turn ended: " + strings.TrimPrefix(m.ContentRef, "status:") + "]"
	}
	data, err := w.artifacts.Get(ctx, tenantID, m.ContentRef)
	if err != nil {
		return "[message unavailable]"
	}
	var doc map[string]any
	if json.Unmarshal(data, &doc) == nil {
		for _, k := range []string{"goal", "answer", "text"} {
			if v, ok := doc[k].(string); ok {
				return v
			}
		}
	}
	return string(data)
}

// withoutDisabled removes tools the tenant switched off.
func withoutDisabled(ds []tools.Descriptor, off map[string]bool) []tools.Descriptor {
	if len(off) == 0 {
		return ds
	}
	out := ds[:0]
	for _, d := range ds {
		if !off[d.Name] {
			out = append(out, d)
		}
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
