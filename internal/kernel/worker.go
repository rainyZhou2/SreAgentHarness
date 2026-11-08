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
	"go-sre-harness/internal/flow"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/outsum"
	"go-sre-harness/internal/planner"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/reasoning"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/untrusted"
	"go-sre-harness/internal/verify"
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
	// CapabilityMap supplies the optional [E] prompt section (optional).
	CapabilityMap CapabilityMapSource
	// Planner generates the orchestration plan for an auto run (optional).
	Planner Planner
	// PlanMemory supplies plan exemplars and reasoning patterns to the
	// planner (optional).
	PlanMemory PlanMemorySource
	// Verifier independently checks final task conclusions (optional).
	Verifier *verify.Verifier
	// MaxBacktracks bounds how often a conclusion the verifier did not
	// accept is handed back to the agent (0: DefaultMaxBacktracks,
	// negative: never; the report is still attached).
	MaxBacktracks int
	// VerifyTimeout bounds one verification (default 60s).
	VerifyTimeout time.Duration
	// MinAgreement is the self-consistency agreement below which a sampled
	// conclusion is not accepted (default 0.6).
	MinAgreement float64
	// Calibration supplies the measured calibration of the model's stated
	// confidence (optional).
	Calibration CalibrationSource
	// ConclusionSamples > 1 samples final task conclusions that many times
	// and keeps the majority (self-consistency).
	ConclusionSamples int
	// Run limits (the profile's run_limits can only tighten them).
	// RunMaxCostUSD and RunMaxTokens bound one run's model spend (0: off);
	// from 80% the agent is told to conclude, at 100% the kernel concludes
	// for it without another model call.
	RunMaxCostUSD float64
	RunMaxTokens  int64
	// MaxRepeats is how often an identical call (tool and arguments) may run
	// again in a run (0: DefaultMaxRepeats, < 0: unlimited).
	MaxRepeats int
	// StallSteps is the number of consecutive tool results without new
	// content after which the agent is told it is stuck; twice that forces
	// a conclusion (0: DefaultStallSteps, < 0: off).
	StallSteps int
	// MaxParallelTools bounds the read-only calls of one response that run
	// together (0: DefaultMaxParallelTools, 1: one call per step).
	MaxParallelTools int
	// ArtifactCacheBytes bounds the worker's cache of immutable artifacts
	// (0: DefaultArtifactCacheBytes, < 0: off).
	ArtifactCacheBytes int64
	// ShortTermMessages is how many earlier conversation messages are
	// rendered verbatim; older ones are condensed into a digest
	// (0: DefaultShortTermMessages, < 0: all verbatim).
	ShortTermMessages int
	// ToolTopK is how many tool schemas the model sees when more tools are
	// visible (0: DefaultToolTopK, < 0: all); the rest are listed by name
	// and loaded with harness.load_tools.
	ToolTopK int
	// CompactTokens compacts history when the uncompacted tool results
	// exceed about this many tokens (0: DefaultCompactTokens, < 0: only
	// the step count applies).
	CompactTokens int
	// RoutineModelAlias / EscalatedModelAlias route reasoning calls: the
	// escalated alias for the first analysis, likely or forced
	// conclusions, stalls, backtracks and repairs, the routine alias
	// otherwise ("" keeps the strategy's alias).
	RoutineModelAlias   string
	EscalatedModelAlias string
	// SpeculativeTools starts read-only calls the policy allows while the
	// model is still streaming; the tool step reuses the result.
	SpeculativeTools bool
	// PlanApprovers is the default number of approvals for a generated plan;
	// R3 plans always require at least two.
	PlanApprovers int
	// PlanApprovalMode is auto (review plans whose analysis requires it),
	// always or never. It is enforced here as well as in the planner so a
	// planner bug cannot skip the gate.
	PlanApprovalMode string
	// PlanMaxReplans bounds how many times a failed generated flow is
	// replaced by a new plan within one run (0 = DefaultPlanMaxReplans,
	// <0 = never replan).
	PlanMaxReplans int
	// Context gathers environment facts on a run's first reason step; they
	// are pinned in run KV (optional).
	Context ContextGatherer
	// OutputOffloadBytes is the size above which tool results are stored as
	// artifacts and summarized (0 = DefaultOffloadBytes, <0 = never).
	OutputOffloadBytes int
	// Summarizers summarize offloaded results (default outsum.Default()).
	Summarizers outsum.Registry
}

// Planner produces a generated flow plan for an auto run.
type Planner interface {
	Plan(ctx context.Context, scope gw.Scope, in planner.Input) (planner.Result, error)
}

// CapabilityMapSource renders capability-to-service notes for the prompt.
type CapabilityMapSource interface {
	CapabilityMap(ctx context.Context, tenantID, goal string, limit int) ([]string, error)
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
	if c.PlanApprovers <= 0 {
		c.PlanApprovers = 1
	}
	if c.PlanApprovalMode == "" {
		c.PlanApprovalMode = "auto"
	}
	if c.PlanMaxReplans == 0 {
		c.PlanMaxReplans = DefaultPlanMaxReplans
	}
	if c.OutputOffloadBytes == 0 {
		c.OutputOffloadBytes = DefaultOffloadBytes
	}
	if len(c.Summarizers) == 0 {
		c.Summarizers = outsum.Default()
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
	spec      speculation
}

func NewWorker(cfg Config, st StateStore, arts artifact.Store, strat cognitive.Strategy, reg *tools.Registry,
	pol policy.Gate, log *slog.Logger) *Worker {
	cfg.defaults()
	if log == nil {
		log = slog.Default()
	}
	w := &Worker{cfg: cfg, store: st, strategy: strat, tools: reg, policy: policy.Guarded(pol),
		log: log.With("worker", cfg.ID), reasonSem: make(chan struct{}, cfg.MaxReasonInflight),
		toolSem: make(chan struct{}, cfg.MaxToolInflight)}
	w.artifacts = newCachedStore(arts, cfg.ArtifactCacheBytes, w.loopEvent)
	return w
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
	st := &execState{}
	if !lease.ReadyAt.IsZero() && started.After(lease.ReadyAt) {
		st.phases.add("queue", started.Sub(lease.ReadyAt))
	}
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
		w.observePhases(string(lease.Kind), &st.phases)
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
		defer st.phases.since("commit", time.Now())
		return w.store.CommitFailure(commitCtx, lease, f)
	}
	res.Summary = withTimings(res.Summary, st.phases.millis())
	defer st.phases.since("commit", time.Now())
	return w.store.CommitSuccess(commitCtx, lease, res)
}

// withTimings adds the step's phase times (ms) to its summary.
func withTimings(summary json.RawMessage, ms map[string]int64) json.RawMessage {
	if len(ms) == 0 {
		return summary
	}
	m := map[string]any{}
	if len(summary) > 0 && json.Unmarshal(summary, &m) != nil {
		return summary
	}
	m["timings_ms"] = ms
	return mustJSON(m)
}

type execState struct {
	dispatched bool // a side-effecting call may have reached the external system
	phases     phases
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
		return w.executeReason(ctx, lease, st)
	case domain.StepTool:
		return w.executeTool(ctx, lease, st)
	case domain.StepFlow:
		return w.executeFlow(ctx, lease)
	case domain.StepPlan:
		return w.executePlan(ctx, lease)
	case domain.StepPlanReview:
		return w.executePlanReview(ctx, lease)
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
		mctx := domain.WithMemoryExclude(domain.WithMemoryTopK(ctx, rules.MemoryTopK), rules.MemoryExcludeFor(lease.RunID))
		return w.cfg.Memory.Retrieve(mctx, lease, g.Goal)
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
	Goal   string         `json:"goal"`
	Inputs map[string]any `json:"inputs,omitempty"`
	Flow   *flowNodeGoal  `json:"flow,omitempty"`
	// Context is the run context pinned at creation (time, window,
	// services); nil for older goals.
	Context *domain.GoalContext `json:"context,omitempty"`
}

// flowNodeGoal is stored in a child run's goal artifact. It carries the DSL
// node's role, prompt, tool subset and, for subflows, the nested spec.
type flowNodeGoal struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Role       string         `json:"role,omitempty"`
	Prompt     string         `json:"prompt,omitempty"`
	Tools      []string       `json:"tools,omitempty"`
	ModelAlias string         `json:"model_alias,omitempty"`
	MaxSteps   int            `json:"max_steps,omitempty"`
	Tool       string         `json:"tool,omitempty"`
	Args       map[string]any `json:"args,omitempty"`
	Input      map[string]any `json:"input,omitempty"`
	Subflow    *flow.Spec     `json:"subflow,omitempty"`
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

func (w *Worker) executeReason(ctx context.Context, lease domain.Lease, st *execState) (domain.StepSuccess, error) {
	mark := time.Now()
	in, err := w.renderContext(ctx, lease)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	factsWrite := w.gatherFacts(ctx, lease, &in)
	cal := w.calibrator(ctx, lease)
	in.CalibrationNote = cal.Note()
	rules, err := w.rules(ctx, lease)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	limits := w.loopLimits(rules)
	loops := newLoopState(in.Calls)
	bud := w.budget(ctx, lease, limits)
	kernelStop, mustConclude := w.loopAdvisories(&in, bud, loops, limits)
	stalled := limits.stall > 0 && loops.stall >= limits.stall
	leader, converged := convergence(in.Ledger)
	if converged && !in.ForceConclude {
		// Adaptive stop: the ledger settled before the step budget did.
		in.Advisories = append(in.Advisories, fmt.Sprintf("Hypothesis %s is VERIFIED with high confidence and no competing hypothesis is open. Conclude now unless one more check could still change the answer.", leader))
	}
	// The model sees a stable subset of the tools when there are many; the
	// kernel still accepts any visible tool.
	visible := in.Tools
	if in.FlowNode == nil || len(in.FlowNode.Tools) == 0 {
		keep := map[string]bool{}
		for _, c := range in.Calls {
			name, _, _ := strings.Cut(c.Key, "\x00")
			keep[name] = true
		}
		for _, n := range in.LoadedTools {
			keep[n] = true
		}
		k := w.cfg.ToolTopK
		if k == 0 {
			k = DefaultToolTopK
		}
		in.Tools, in.MoreTools = selectTools(visible, requestText(in), keep, k)
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
	switch {
	case in.FlowNode != nil && in.FlowNode.Type == "tool":
		strat = cognitive.FlowTool{}
	case in.FlowNode != nil:
		// Flow agent/subflow child: the profile strategy is the flow
		// strategy itself, so use the deployment's default agent strategy.
	case w.cfg.Strategies != nil && lease.ProfileID != "":
		ps, err := w.cfg.Strategies.StrategyFor(ctx, lease.TenantID, lease.ProfileID, lease.ProfileVersion)
		if err != nil {
			<-w.reasonSem
			return domain.StepSuccess{}, domain.Retryable(fmt.Errorf("resolve profile strategy: %w", err), 0)
		}
		strat = ps
	}
	routed := in.FlowNode != nil && in.FlowNode.ModelAlias != ""
	if routed {
		strat = withAlias(strat, in.FlowNode.ModelAlias)
	}
	base := strat
	tier := ""
	pick := func() {
		if routed {
			return
		}
		alias, why := w.modelAlias(in, stalled, converged)
		strat, tier = withAlias(base, alias), why
	}
	in.OnToolCall = w.speculator(ctx, lease, rules, in, visible, loops, limits)
	mark = st.phases.since("context", mark)
	var decision domain.Decision
	var repairs []string
	if kernelStop != "" {
		// Hard limit: conclude without another model call.
		decision = kernelConclusion(in, kernelStop)
	} else {
		for {
			pick()
			decision, err = strat.Decide(ctx, in)
			problem := repairProblem(decision, err)
			if problem == "" && err == nil {
				problem = unknownToolProblem(decision, visible)
			}
			if problem == "" || len(repairs) >= DefaultMaxOutputRepairs || ctx.Err() != nil {
				break
			}
			// Tell the model what was wrong instead of re-asking blindly.
			repairs = append(repairs, truncate(problem, 300))
			in.Repair = problem
			w.loopEvent("repair")
		}
		if err == nil && w.cfg.ConclusionSamples > 1 && cognitive.Validate(decision) == nil {
			decision = cognitive.SelfConsistency(ctx, strat, in, decision, w.cfg.ConclusionSamples)
		}
	}
	<-w.reasonSem
	mark = st.phases.since("model", mark)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	if err := cognitive.Validate(decision); err != nil {
		// Malformed model output: retry the reasoning, never execute it (§31.3).
		return domain.StepSuccess{}, domain.Retryable(fmt.Errorf("invalid decision: %w", err), 0)
	}
	if mustConclude != "" && decision.Kind == domain.DecisionTool {
		// The agent was told to conclude and still asked for a tool.
		kernelStop = mustConclude
		decision = kernelConclusion(in, kernelStop)
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
	if factsWrite != nil {
		res.KVWrites = append(res.KVWrites, *factsWrite)
	}
	var unresolvedRefs int
	res.Thoughts, res.Capabilities, unresolvedRefs = reasonTrace(decision, in, lease.TenantID, cal)
	summary := map[string]any{"decision_kind": decision.Kind, "intent": decision.Intent, "reason_summary": truncate(decision.ReasonSummary, 500),
		"hypotheses": len(decision.Hypotheses), "thoughts": len(res.Thoughts), "capabilities": len(res.Capabilities)}
	if unresolvedRefs > 0 {
		summary["unresolved_refs"] = unresolvedRefs
	}
	if len(repairs) > 0 {
		summary["repairs"] = repairs
	}
	if len(in.Advisories) > 0 {
		summary["advisories"] = in.Advisories
	}
	if bud.usage.Requests > 0 {
		summary["run_usage"] = bud.usage
	}
	if kernelStop != "" {
		summary["kernel_stop"] = kernelStop
		w.loopEvent("kernel_stop")
	}
	if tier != "" {
		summary["model_tier"] = tier
		w.loopEvent("model_" + strings.SplitN(tier, ":", 2)[0])
	}
	if converged {
		summary["converged"] = leader
		w.loopEvent("converged")
	}
	if len(in.MoreTools) > 0 {
		summary["tools_shown"], summary["tools_more"] = len(in.Tools), len(in.MoreTools)
	}
	for _, a := range in.Advisories {
		switch {
		case strings.HasPrefix(a, "Run budget"):
			w.loopEvent("budget_notice")
		case strings.Contains(a, "brought nothing new"):
			w.loopEvent("stall_notice")
		}
	}
	if decision.ModelRequestID != "" {
		summary["model_request_id"], summary["model"], summary["manifest_ref"] = decision.ModelRequestID, decision.Model, decision.ManifestRef
	}

	nextReason := domain.PlannedStep{Kind: domain.StepReason, Executor: "reasoner", InputRef: lease.InputRef,
		InputHash: lease.InputHash, Status: domain.StepPending}

	if decision.Kind == domain.DecisionFinal {
		var review reviewOutcome
		if kernelStop == "" {
			// A kernel stop is already marked unverified; reviewing it would
			// spend what the stop protects.
			review, err = w.reviewConclusion(ctx, lease, in, decision)
			st.phases.since("review", mark)
			if err != nil {
				return domain.StepSuccess{}, err
			}
		}
		if review.thought != nil {
			th := *review.thought
			th.Seq = len(res.Thoughts) + 1
			res.Thoughts = append(res.Thoughts, th)
		}
		if review.backtrack != nil {
			summary["conclusion_review"] = review.report.Verdict
			res.Plan = []domain.PlannedStep{*review.backtrack}
			res.Summary = mustJSON(summary)
			res.Events = append(res.Events, domain.EmittedEvent{Type: "reasoning.conclusion_rejected",
				Payload: mustJSON(map[string]any{"verdict": review.report.Verdict, "issues": capList(review.report.Issues(), 10),
					"agreement": review.report.Agreement})})
			return res, nil
		}
		result := map[string]any{
			"answer": decision.Answer, "intent": decision.Intent, "hypotheses": decision.Hypotheses,
			"thought": decision.Thought, "decision_ref": outRef}
		if len(decision.Output) > 0 {
			result["flow_output"] = decision.Output
		}
		if kernelStop != "" {
			result["stopped"] = kernelStop
		}
		if rm := runMemory(in.NoteState, lease.StepSeq, decision.Notes); rm != nil {
			result["memory"] = rm
		}
		result["loop"] = map[string]any{"steps": lease.StepSeq, "model_requests": bud.usage.Requests,
			"input_tokens": bud.usage.InputTokens, "output_tokens": bud.usage.OutputTokens,
			"cost_usd": float64(bud.usage.CostMicros) / 1e6}
		if rep := review.report; rep != nil {
			result["independent_verification"] = rep
			summary["independent_verdict"] = rep.Verdict
			if rep.Verdict != verify.Supported {
				// The answer stands, but it is not presented as verified.
				result["answer"] = strings.TrimSpace(decision.Answer) + "\n\n[Independent check: " + rep.Verdict +
					" — " + truncate(strings.Join(rep.Issues(), "; "), 600) + "]"
			}
		}
		resultRef, _, err := w.put(ctx, lease.TenantID, "result", result)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		res.Complete, res.ResultRef = true, resultRef
		res.Summary = mustJSON(summary)
		return res, nil
	}

	if in.FlowNode != nil && len(in.FlowNode.Tools) > 0 && !flowToolVisible(in.FlowNode.Tools, decision.Tool.Name) {
		return domain.StepSuccess{}, fmt.Errorf("tool %q is not visible to flow node %q", decision.Tool.Name, in.FlowNode.ID)
	}
	call := *decision.Tool
	// Follow-up markers are kernel-owned; a model decision never carries one.
	call.Followup = nil
	if why := loops.refuse(call, limits.maxRepeats); why != "" {
		step, err := w.refusedStep(ctx, lease, call, why)
		if err != nil {
			return domain.StepSuccess{}, err
		}
		res.Plan = []domain.PlannedStep{step, nextReason}
		summary["tool"], summary["loop_guard"] = call.Name, why
		w.loopEvent("loop_guard")
		if n := len(decision.DeferredToolCalls); n > 0 {
			summary["deferred_not_run"] = n
		}
		res.Summary = mustJSON(summary)
		return res, nil
	}
	var reg *tools.Descriptor
	if t, ok := w.tools.LookupFor(lease.TenantID, call.Name); ok {
		d := t.Descriptor()
		reg = &d
	}
	// Missing time-range arguments come from the run's pinned time context,
	// so the approved call names a concrete range.
	call.Args = fillTimeArgs(reg, call.Args, in.Time)
	callRef, callHash, err := w.put(ctx, lease.TenantID, "tool-input", call)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	// The model cannot influence the risk class; the profile can only tighten.
	pd, desc := rules.DecideCall(ctx, w.policy, profile.Call{TenantID: lease.TenantID, RunID: lease.RunID,
		Name: call.Name, Args: call.Args}, reg)
	if call.Name == verify.ReviewToolName || call.Name == BatchToolName {
		pd, desc = policy.Decision{Verdict: policy.Deny, Reason: "kernel-owned tool"}, nil
	}
	if pd.Verdict == policy.Allow && len(decision.DeferredToolCalls) > 0 {
		// Independent read-only calls of the same response run together.
		calls, notes := w.planBatch(ctx, lease, rules, in, visible, call, desc, decision.DeferredToolCalls, limits, loops)
		if bt, ok := w.tools.LookupFor(lease.TenantID, BatchToolName); ok && len(calls) > 1 {
			bcall := domain.ToolCall{Name: BatchToolName, Args: mustJSON(batchArgs{Calls: calls}),
				Followup: &domain.ToolFollowup{Role: domain.FollowupBatch, OfTool: call.Name, OfStep: lease.StepSeq}}
			if bref, bhash, err := w.put(ctx, lease.TenantID, "tool-input", bcall); err != nil {
				return domain.StepSuccess{}, err
			} else {
				callRef, callHash = bref, bhash
			}
			bd := rules.Effective(bt.Descriptor())
			desc = &bd
			pd = policy.Decision{Verdict: policy.Allow, Reason: fmt.Sprintf("read-only batch of %d calls, each allowed", len(calls))}
			call = bcall
			summary["batch"] = len(calls)
			w.loopEvent("batch")
		} else if len(calls) > 1 {
			notes = deferredNotes(decision.DeferredToolCalls, "parallel batches are not available")
		}
		if len(notes) > 0 {
			summary["deferred_not_run"] = sortedNotes(notes)
		}
	} else if n := len(decision.DeferredToolCalls); n > 0 {
		summary["deferred_not_run"] = n
	}
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
	visible = withoutKernelTools(visible)
	if w.cfg.ToolToggles != nil {
		off, err := w.cfg.ToolToggles.DisabledTools(ctx, lease.TenantID)
		if err != nil {
			return cognitive.ReasoningInput{}, err
		}
		visible = withoutDisabled(visible, off)
	}
	in := cognitive.ReasoningInput{StepSeq: lease.StepSeq, ContextSnapshotID: lease.ContextSnapshotID,
		Tools: visible, MaxSteps: rules.MaxSteps}
	var g goalDoc
	if err := w.get(ctx, lease.TenantID, lease.GoalRef, &g); err != nil {
		return in, err
	}
	in.Goal = g.Goal
	in.Time = g.Context
	if g.Flow != nil {
		in.FlowNode = &cognitive.FlowNode{
			ID: g.Flow.ID, Type: g.Flow.Type, Role: g.Flow.Role, Prompt: g.Flow.Prompt,
			Tools: g.Flow.Tools, ModelAlias: g.Flow.ModelAlias, MaxSteps: g.Flow.MaxSteps,
			Tool: g.Flow.Tool, Args: g.Flow.Args, Input: g.Flow.Input,
		}
		if g.Flow.Role != "" || g.Flow.Prompt != "" {
			instr := cognitive.DefaultInstructions
			if role := strings.TrimSpace(g.Flow.Role); role != "" {
				instr += "\n\nYou are the \"" + role + "\" agent in a flow. Stay within this role."
			}
			if prompt := strings.TrimSpace(g.Flow.Prompt); prompt != "" {
				instr += "\n\n" + prompt
			}
			in.Instructions = instr
		}
		if len(g.Flow.Tools) > 0 {
			allowed := make(map[string]bool, len(g.Flow.Tools))
			for _, t := range g.Flow.Tools {
				allowed[t] = true
			}
			filtered := make([]tools.Descriptor, 0, len(visible))
			for _, t := range visible {
				if allowed[t.Name] {
					filtered = append(filtered, t)
				}
			}
			visible = filtered
			in.Tools = visible
		}
		if g.Flow.MaxSteps > 0 && (in.MaxSteps == 0 || g.Flow.MaxSteps < in.MaxSteps) {
			in.MaxSteps = g.Flow.MaxSteps
		}
	}
	if rules.CapabilityMap && w.cfg.CapabilityMap != nil && g.Flow == nil {
		lines, mapErr := w.cfg.CapabilityMap.CapabilityMap(ctx, lease.TenantID, in.Goal, 5)
		if mapErr != nil {
			w.log.Warn("capability map", "err", mapErr)
		} else {
			in.CapabilityMap = lines
		}
	}

	history, err := w.store.History(ctx, lease.TenantID, lease.RunID, lease.StepSeq)
	if err != nil {
		return in, err
	}
	ledger := newLedger()
	var noteWrites []stepNotes
	for _, s := range history {
		if s.Kind == domain.StepReason && s.Status == domain.StepSucceeded && s.OutputRef != nil {
			var d domain.Decision
			if err := w.get(ctx, lease.TenantID, *s.OutputRef, &d); err == nil {
				ledger.add(s.Seq, d.Hypotheses)
				if len(d.Notes) > 0 {
					noteWrites = append(noteWrites, stepNotes{seq: s.Seq, ops: d.Notes})
				}
			}
			continue
		}
		if s.Kind != domain.StepTool {
			continue
		}
		in.HasToolSteps = true
		var call domain.ToolCall
		if err := w.get(ctx, lease.TenantID, s.InputRef, &call); err != nil {
			return in, err
		}
		if fu := call.Followup; fu != nil && fu.Role == domain.FollowupReview {
			ledger.challenge(fu.OfStep)
		}
		modelCall := call.Followup == nil
		if call.Name == BatchToolName && call.Followup != nil && call.Followup.Role == domain.FollowupBatch {
			parts, err := w.batchObservations(ctx, lease.TenantID, s, call)
			if err != nil {
				return in, err
			}
			for _, o := range parts {
				in.Observations = append(in.Observations, o)
				in.Calls = append(in.Calls, cognitive.CallRecord{Step: s.Seq, Key: callKey(o.Tool, o.Args),
					Outcome: o.Outcome, ResultHash: resultHash(o)})
			}
			continue
		}
		obs := domain.Observation{StepSeq: s.Seq, Tool: call.Name, Args: call.Args, TrustTag: "T2"}
		switch {
		case s.Status == domain.StepSucceeded && s.OutputRef != nil:
			var o domain.Observation
			if err := w.get(ctx, lease.TenantID, *s.OutputRef, &o); err != nil {
				return in, err
			}
			if call.Name == LoadToolsName && o.Outcome != "error" {
				in.LoadedTools = append(in.LoadedTools, loadedTools(o.Content)...)
			}
			if call.Name == domain.MemorySearchToolName && o.Outcome != "error" {
				in.RecalledMemories = append(in.RecalledMemories, recalledMemories(o.Content)...)
			}
			obs.Outcome, obs.Content = "ok", o.Content
			if o.Outcome != "" {
				obs.Outcome = o.Outcome
			}
			obs.Artifact = o.Artifact
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
		if modelCall {
			in.Calls = append(in.Calls, cognitive.CallRecord{Step: s.Seq, Key: callKey(call.Name, call.Args),
				Outcome: obs.Outcome, ResultHash: resultHash(obs)})
		}
	}

	in.Ledger = ledger.entries()

	notes := &domain.NoteSet{}
	if cr, ok := w.store.(ConversationReader); ok {
		msgs, err := cr.RunConversation(ctx, lease.TenantID, lease.RunID)
		if err != nil {
			return in, err
		}
		var all []cognitive.ConversationMessage
		for _, m := range msgs {
			all = append(all, cognitive.ConversationMessage{Seq: m.Seq, Role: m.Role,
				Text: w.messageText(ctx, lease.TenantID, m)})
		}
		in.Conversation, in.ConversationDigest = conversationWindow(all, w.shortTermMessages())
		notes.Seed(w.priorSessionNotes(ctx, lease.TenantID, msgs))
	}
	for _, sn := range noteWrites {
		notes.Apply(sn.seq, capNoteOps(sn.ops))
	}
	in.Notes, in.NoteState = notes.Live(), notes
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
	if v, ok := versions[factsKey]; ok {
		data, err := w.store.KVDataAt(ctx, domain.KVKey{TenantID: lease.TenantID, Scope: "run", OwnerID: lease.RunID, Name: factsKey}, v)
		if err != nil {
			return in, err
		}
		facts := []domain.ContextFact{}
		if json.Unmarshal(data, &facts) == nil {
			in.Facts = facts // pinned: never nil from here on
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
	if call.Name == BatchToolName && (call.Followup == nil || call.Followup.Role != domain.FollowupBatch) {
		return domain.StepSuccess{}, fmt.Errorf("tool %q is kernel-owned: %w", call.Name, domain.ErrNotDispatched)
	}
	if call.Name == verify.ReviewToolName && (call.Followup == nil || call.Followup.Role != domain.FollowupReview) {
		// Kernel-owned: a model cannot forge a review result.
		return domain.StepSuccess{}, fmt.Errorf("tool %q is kernel-owned: %w", call.Name, domain.ErrNotDispatched)
	}
	rules, err := w.rules(ctx, lease)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	// Memory a profile retired stays out of in-run memory searches too.
	ctx = domain.WithMemoryExclude(ctx, rules.MemoryExcludeFor(lease.RunID))
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

	if call.Name == BatchToolName {
		toolStart := time.Now()
		res, err := w.executeBatch(ctx, lease, rules, call, plan)
		st.phases.since("tool", toolStart)
		return res, err
	}

	if call.Followup != nil && call.Followup.Role == domain.FollowupVerify {
		select {
		case w.toolSem <- struct{}{}:
		case <-ctx.Done():
			return domain.StepSuccess{}, ctx.Err()
		}
		vo, lc := w.executeVerify(ctx, lease, call, tool, desc)
		<-w.toolSem
		if ctx.Err() != nil {
			// Deadline or cancellation: let the step machinery classify it.
			return domain.StepSuccess{}, ctx.Err()
		}
		w.cfg.Observer.ObserveTool(desc.Source, string(desc.Risk), "verify_"+strings.ToLower(vo.verdict))
		return w.verifyStepResult(ctx, lease, rules, call, desc, vo, lc, plan)
	}

	if sideEffect {
		state, priorRef, err := w.store.PrepareEffect(ctx, lease, target)
		if err != nil {
			return domain.StepSuccess{}, err // ErrUnknownEffect: do not call the tool again
		}
		if state == domain.EffectConfirmed {
			// Already applied by an earlier attempt whose commit was lost: reuse.
			thoughts, caps := toolTrace(call.Name, desc, true)
			summary := map[string]any{"tool": call.Name, "reused": true}
			var prior domain.Observation
			if priorRef != "" && w.get(ctx, lease.TenantID, priorRef, &prior) == nil {
				vstep, note, err := w.planVerify(ctx, lease, rules, desc, call, priorRef, prior.Content)
				if err != nil {
					return domain.StepSuccess{}, err
				}
				plan = withVerify(plan, vstep)
				if note != "" {
					summary["lifecycle"] = note
				}
			}
			return domain.StepSuccess{OutputRef: priorRef, OutputHash: "", ArtifactKind: "observation",
				Plan: plan, SideEffect: true, Thoughts: thoughts, Capabilities: caps,
				Summary: mustJSON(summary)}, nil
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
	toolStart := time.Now()
	var out json.RawMessage
	specOK := false
	if call.Followup == nil {
		// Started while the model was still streaming.
		out, specOK = w.takeSpeculative(ctx, lease, desc, call.Name, call.Args)
	}
	if !specOK {
		out, err = tool.Invoke(callCtx, lease.IdempotencyKey, call.Args)
	}
	<-w.toolSem
	st.phases.since("tool", toolStart)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "tool error")
		span.End()
		w.cfg.Observer.ObserveTool(desc.Source, string(desc.Risk), "error")
		var retry *domain.RetryableError
		if !sideEffect && ctx.Err() == nil && !errors.As(err, &retry) {
			// A read-only call that failed is evidence too (wrong arguments,
			// missing resource, permission): the agent sees it and adapts.
			// Transient failures still retry the step.
			trust := desc.Trust
			if trust == "" {
				trust = "T4"
			}
			obs := domain.Observation{StepSeq: lease.StepSeq, Tool: call.Name, Outcome: "error",
				Content: mustJSON(map[string]string{"error": err.Error()}), TrustTag: trust,
				Injection: untrusted.Scan(err.Error())}
			ref, h, putErr := w.put(ctx, lease.TenantID, "observation", obs)
			if putErr != nil {
				return domain.StepSuccess{}, putErr
			}
			thoughts, caps := toolTrace(call.Name, desc, false)
			return domain.StepSuccess{OutputRef: ref, OutputHash: h, ArtifactKind: "observation", Plan: plan,
				Thoughts: thoughts, Capabilities: caps, Summary: mustJSON(map[string]any{"tool": call.Name, "error": true})}, nil
		}
		return domain.StepSuccess{}, err
	}
	span.End()
	w.cfg.Observer.ObserveTool(desc.Source, string(desc.Risk), "ok")
	trust := desc.Trust
	if trust == "" {
		trust = "T2"
	}
	obs, err := w.observe(ctx, lease.TenantID, desc,
		domain.Observation{StepSeq: lease.StepSeq, Tool: call.Name, Outcome: "ok", TrustTag: trust}, out)
	if err != nil {
		if sideEffect {
			// The effect happened but we cannot persist proof: treat as unknown.
			return domain.StepSuccess{}, fmt.Errorf("%w: %v", domain.ErrUnknownEffect, err)
		}
		return domain.StepSuccess{}, err
	}
	ref, h, err := w.put(ctx, lease.TenantID, "observation", obs)
	if err != nil {
		if sideEffect {
			// The effect happened but we cannot persist proof: treat as unknown.
			return domain.StepSuccess{}, fmt.Errorf("%w: %v", domain.ErrUnknownEffect, err)
		}
		return domain.StepSuccess{}, err
	}
	summary := map[string]any{"tool": call.Name, "risk": desc.Risk}
	if obs.Artifact != nil {
		summary["offloaded_bytes"] = obs.Artifact.Bytes
	}
	if sideEffect {
		// The effect is applied; planning the verification must not turn a
		// committed effect into an unknown one, so failures only skip it.
		vstep, note, verr := w.planVerify(ctx, lease, rules, desc, call, ref, out)
		switch {
		case verr != nil:
			summary["lifecycle"] = "verification not planned: " + verr.Error()
		case note != "":
			summary["lifecycle"] = note
		}
		plan = withVerify(plan, vstep)
	}
	if !sideEffect {
		plan = w.withTokenCompaction(ctx, lease, plan, len(obs.Content))
	}
	thoughts, caps := toolTrace(call.Name, desc, true)
	return domain.StepSuccess{OutputRef: ref, OutputHash: h, ArtifactKind: "observation", SideEffect: sideEffect,
		Plan: plan, Thoughts: thoughts, Capabilities: caps, Summary: mustJSON(summary)}, nil
}

// withVerify puts a planned verification first. A verification waiting for
// approval stands alone: approving it runs it (it plans the next reason
// step), rejecting it resumes reasoning. A denied one is recorded as skipped
// ahead of the normal continuation.
func withVerify(plan []domain.PlannedStep, v *domain.PlannedStep) []domain.PlannedStep {
	if v == nil {
		return plan
	}
	switch v.Status {
	case domain.StepPending, domain.StepWaitingApproval:
		return []domain.PlannedStep{*v}
	default:
		return append([]domain.PlannedStep{*v}, plan...)
	}
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

// reasonTrace renders a decision as stored thoughts. Citations are resolved
// against what the request showed the model; unresolved ones are kept,
// marked, and counted in the returned total.
func reasonTrace(d domain.Decision, in cognitive.ReasoningInput, tenantID string, cal *reasoning.Calibrator) ([]domain.Thought, []domain.CapabilityRef, int) {
	var out []domain.Thought
	var caps []domain.CapabilityRef
	seq := 0
	refSet := cognitive.NewRefSet(in)
	unresolved := 0
	resolve := func(raws []string, relation string) []domain.ThoughtRef {
		refs, n := refSet.ResolveRefs(raws, relation)
		unresolved += n
		return refs
	}
	next := func(kind domain.ThoughtKind, summary string, status domain.ThoughtStatus, confidence float64, refs []domain.ThoughtRef) {
		if strings.TrimSpace(summary) == "" {
			return
		}
		seq++
		out = append(out, domain.Thought{Seq: seq, Kind: kind, Summary: truncate(summary, 1000), Status: status,
			Confidence: confidence, Structured: d.Thought != nil && d.Thought.Structured, Refs: refs})
	}
	var memoryRefs []domain.ThoughtRef
	for _, m := range in.Memories {
		memoryRefs = append(memoryRefs, domain.ThoughtRef{Ref: fmt.Sprintf("memory:%s@%d", m.ID, m.Version), Relation: "based_on"})
	}
	evidenceRefs := resolve(d.EvidenceRefs, "supports")
	if d.Thought != nil {
		next(domain.ThoughtPlan, d.Thought.Plan, domain.ThoughtUnknown, d.Thought.Confidence, memoryRefs)
	}
	next(domain.ThoughtDecision, d.ReasonSummary, domain.ThoughtUnknown, 0, evidenceRefs)
	for _, h := range d.Hypotheses {
		status := domain.ThoughtUnverified
		switch strings.ToUpper(h.Status) {
		case string(domain.ThoughtVerified):
			status = domain.ThoughtVerified
		case string(domain.ThoughtRefuted):
			status = domain.ThoughtRefuted
		}
		refs := append(resolve(h.Supports, "supports"), resolve(h.Refutes, "refutes")...)
		n := len(out)
		next(domain.ThoughtHypothesis, h.Statement, status, h.ConfidenceEstimate, refs)
		if len(out) > n {
			attrs := map[string]any{}
			if id := strings.TrimSpace(h.ID); id != "" {
				attrs[reasoning.HypothesisIDAttr] = truncate(id, 64)
			}
			if d.Kind == domain.DecisionTool {
				// Listed while investigating: tracked, not a claim.
				attrs[reasoning.TrackingAttr] = true
			}
			if cal != nil && h.ConfidenceEstimate > 0 && h.ConfidenceEstimate <= 1 {
				attrs[reasoning.CalibratedAttr] = cal.Apply(h.ConfidenceEstimate)
			}
			if len(attrs) > 0 {
				out[n].Attrs = attrs
			}
		}
	}
	if d.Kind == domain.DecisionTool && d.Tool != nil {
		var refs []domain.ThoughtRef
		for _, desc := range in.Tools {
			if desc.Name != d.Tool.Name {
				continue
			}
			ref := tools.CapabilityRef(desc)
			caps = append(caps, domain.CapabilityRef{Ref: ref, Role: "considered"})
			refs = append(refs, domain.ThoughtRef{Ref: ref, Relation: "uses"})
			break
		}
		next(domain.ThoughtAction, "Call "+d.Tool.Name, domain.ThoughtUnknown, 0, refs)
	}
	if d.Kind == domain.DecisionFinal && d.Thought != nil {
		if v := d.Thought.Verification; v != nil && (v.Verdict != "" || v.Expected != "" || v.Observed != "") {
			summary := strings.TrimSpace(strings.Join([]string{v.Verdict, v.Expected, v.Observed}, " · "))
			var status domain.ThoughtStatus
			switch strings.ToUpper(v.Verdict) {
			case string(domain.ThoughtVerified):
				status = domain.ThoughtVerified
			case string(domain.ThoughtRefuted):
				status = domain.ThoughtRefuted
			default:
				status = domain.ThoughtUnverified
			}
			next(domain.ThoughtVerification, summary, status, 0, evidenceRefs)
		}
		next(domain.ThoughtReflection, d.Thought.Reflection, domain.ThoughtUnknown, 0, nil)
		next(domain.ThoughtConclusion, d.Answer, domain.ThoughtUnknown, 0, evidenceRefs)
	}
	for _, s := range in.Skills {
		caps = append(caps, domain.CapabilityRef{Ref: cognitive.SkillRef(tenantID, s), Role: "considered"})
	}
	return out, caps, unresolved
}

func toolTrace(name string, d tools.Descriptor, ok bool) ([]domain.Thought, []domain.CapabilityRef) {
	ref := tools.CapabilityRef(d)
	status := domain.ThoughtOK
	word := "ok"
	if !ok {
		status, word = domain.ThoughtFailed, "failed"
	}
	return []domain.Thought{{Seq: 1, Kind: domain.ThoughtObservation, Summary: name + " → " + word, Status: status,
			Refs: []domain.ThoughtRef{{Ref: ref, Relation: "uses"}}}},
		[]domain.CapabilityRef{{Ref: ref, Role: "used"}}
}

// withoutKernelTools hides kernel-owned tools from the model.
func withoutKernelTools(ds []tools.Descriptor) []tools.Descriptor {
	out := ds[:0:0]
	for _, d := range ds {
		if d.Name != verify.ReviewToolName && d.Name != BatchToolName {
			out = append(out, d)
		}
	}
	return out
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
	if n <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "…"
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
