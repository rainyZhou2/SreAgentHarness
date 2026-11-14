package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/dataguard"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/quota"
	"go-sre-harness/internal/runbook"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/untrusted"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// Orchestrator advances Works. Several replicas may run against one
// database: a Work is processed only under its lease, and every write is
// fenced by the lease and the Work version.
type Orchestrator struct {
	Store     *cpsql.Store
	Runs      Runs
	Actions   *action.Runtime // nil: action stages block (HARNESS_ACTION_V2_ENABLED off)
	Evaluator *outcome.Evaluator
	Clock     cpkit.Clock
	Log       *slog.Logger
	Metrics   *Metrics
	// OnClose is called after a Work reached a terminal status (best effort,
	// outside the commit: e.g. playbook outcome feedback).
	OnClose func(ctx context.Context, w work.Work)

	// Owner identifies this replica in leases.
	Owner string
	// Lease bounds one processing pass (default 2m). External calls made in
	// a pass (run start, action execution) are bounded well below it.
	Lease time.Duration
	// Poll is the claim interval (default 1s); RunPoll how often a running
	// stage re-reads its runs (default 5s).
	Poll    time.Duration
	RunPoll time.Duration
	// Queues bounds each scheduling queue (default DefaultQueueLimits):
	// Run gives every queue its own claim loop and concurrency, so a flood
	// of routine or background Works never delays a mitigation (§20.2).
	Queues map[work.Queue]QueueLimit
	// Pools are the shared resource pools (model runs, production actions,
	// world snapshots); nil means unlimited.
	Pools *Pools
	// MaxStepsPerClaim bounds immediate transitions in one pass (default 8).
	MaxStepsPerClaim int
	// MaxDataClass is the most sensitive data class the model provider may
	// receive (default confidential): Works above it never start model
	// runs, everything below is redacted (§19.3).
	MaxDataClass dataguard.Class
	maxClassSet  bool
	// World, when set, pins a persisted snapshot of the Work's subjects for
	// every run a stage starts (label world_snapshot), so all agents of a
	// stage reason over the same facts and the run can be replayed.
	World *world.Service
	// Catalog resolves the templates of child Works (children stages).
	Catalog *Catalog
	// Checks enables evidence planning (RFC §12.4): before a run or agents
	// stage starts, the read-only checks that best separate the Work's
	// hypotheses are ranked within the run's tool and policy constraints.
	Checks CheckSource
}

// SetMaxDataClass sets the model data-class limit.
func (o *Orchestrator) SetMaxDataClass(c dataguard.Class) { o.MaxDataClass, o.maxClassSet = c, true }

// pinSnapshot persists the World view of the Work's subjects for the runs
// about to start ("" when no World is configured or it fails: environment
// facts are hints, not prerequisites).
func (o *Orchestrator) pinSnapshot(ctx context.Context, w *work.Work) string {
	if o.World == nil || len(w.Goal.SubjectRefs) == 0 {
		return ""
	}
	snap, err := o.World.Snapshot(ctx, world.SnapshotRequest{TenantID: w.TenantID, Subjects: w.Goal.SubjectRefs,
		Depth: 1, MaxNodes: 100, Persist: true})
	if err != nil {
		o.log().Warn("pin world snapshot", "tenant", w.TenantID, "work_id", w.ID, "err", err)
		return ""
	}
	return snap.ID
}

// modelGoal renders a run goal for the model: untrusted values are fenced
// as data, credentials and personal data are redacted, and Works whose
// data class exceeds the provider limit are refused.
func (o *Orchestrator) modelGoal(tpl string, w *work.Work, summaries map[string]string, extra string) (string, error) {
	limit := dataguard.Confidential
	if o.maxClassSet {
		limit = o.MaxDataClass
	}
	class, err := dataguard.Parse(w.Goal.Constraints.DataClass)
	if err != nil {
		return "", err
	}
	goal := work.RenderWith(tpl, w, summaries, func(name, v string) string {
		if v == "" || !work.UntrustedField(name) {
			return v
		}
		return untrusted.Wrap("source=work."+name+" trust=T4", v).Text
	}) + extra
	out, rep, err := dataguard.ForModel(goal, class, limit)
	if err != nil {
		o.Metrics.GuardDenied("data_class", class.String())
		return "", err
	}
	if rep.Total() > 0 {
		o.log().Info("redacted run goal", "tenant", w.TenantID, "work_id", w.ID, "secrets", rep.Secrets, "pii", rep.PII)
	}
	return out, nil
}

func (o *Orchestrator) now() time.Time { return o.Clock.Now() }

func (o *Orchestrator) log() *slog.Logger {
	if o.Log != nil {
		return o.Log
	}
	return slog.Default()
}

func (o *Orchestrator) defaults() {
	if o.Owner == "" {
		o.Owner = "orchestrator-" + cpkit.NewID()[24:]
	}
	if o.Lease <= 0 {
		o.Lease = 2 * time.Minute
	}
	if o.Poll <= 0 {
		o.Poll = time.Second
	}
	if o.RunPoll <= 0 {
		o.RunPoll = 5 * time.Second
	}
	if o.Queues == nil {
		o.Queues = DefaultQueueLimits()
	}
	if o.MaxStepsPerClaim <= 0 {
		o.MaxStepsPerClaim = 8
	}
}

// limit is the effective limit of a queue.
func (o *Orchestrator) limit(q work.Queue) QueueLimit {
	l := o.Queues[q]
	if l.Concurrency <= 0 {
		l.Concurrency = 1
	}
	if l.Batch <= 0 {
		l.Batch = 4
	}
	return l
}

// Run claims and processes due Works until ctx ends: one loop per queue,
// each with its own concurrency, so queues never wait on each other.
func (o *Orchestrator) Run(ctx context.Context) error {
	o.defaults()
	var wg sync.WaitGroup
	for _, q := range work.Queues {
		wg.Add(1)
		go func(q work.Queue) {
			defer wg.Done()
			o.runQueue(ctx, q)
		}(q)
	}
	wg.Wait()
	return nil
}

// runQueue keeps up to the queue's concurrency Works in flight; a slow Work
// holds one slot of its own queue only.
func (o *Orchestrator) runQueue(ctx context.Context, q work.Queue) {
	lim := o.limit(q)
	slots := make(chan struct{}, lim.Concurrency)
	var inflight sync.WaitGroup
	defer inflight.Wait()
	t := time.NewTicker(o.Poll)
	defer t.Stop()
	for {
		if free := cap(slots) - len(slots); free > 0 {
			works, err := o.Store.ClaimQueue(ctx, o.Owner, q, o.now(), o.Lease, min(free, lim.Batch))
			if err != nil && ctx.Err() == nil {
				o.log().Warn("work orchestrator claim failed", "queue", q, "err", err)
			}
			for _, w := range works {
				slots <- struct{}{}
				inflight.Add(1)
				o.Metrics.claimed(string(q))
				go func(w work.Work) {
					defer inflight.Done()
					defer func() { <-slots }()
					o.process(ctx, w)
				}(w)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick claims due Works of every queue once (most urgent first) and
// processes them, each queue within its own concurrency; it returns how
// many were claimed. Run is the production loop; Tick serves tests, the
// Work bench and single-shot callers.
func (o *Orchestrator) Tick(ctx context.Context) (int, error) {
	o.defaults()
	var wg sync.WaitGroup
	total := 0
	for _, q := range work.Queues {
		lim := o.limit(q)
		works, err := o.Store.ClaimQueue(ctx, o.Owner, q, o.now(), o.Lease, lim.Batch)
		if err != nil {
			wg.Wait()
			return total, err
		}
		total += len(works)
		sem := make(chan struct{}, lim.Concurrency)
		for _, w := range works {
			o.Metrics.claimed(string(q))
			wg.Add(1)
			go func(w work.Work) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				o.process(ctx, w)
			}(w)
		}
	}
	wg.Wait()
	return total, nil
}

func (o *Orchestrator) process(ctx context.Context, w work.Work) {
	if w.WakeAt != nil {
		lag := o.now().Sub(*w.WakeAt)
		o.Metrics.woke(lag)
		if dl := o.limit(w.Queue).AdmissionDeadline.D(); dl > 0 && lag > dl {
			// Late, not lost: the Work is processed now and the delay is
			// visible (alert on the counter; the Work is never dropped).
			o.Metrics.queuedTooLong(string(w.Queue))
			o.log().Warn("work waited past its admission deadline", "tenant", w.TenantID, "work_id", w.ID,
				"queue", w.Queue, "lag", lag.Round(time.Millisecond), "deadline", dl)
		}
	}
	ctx, sp := obs.Tracer().Start(ctx, "work.resume", trace.WithAttributes(attribute.String("work_id", w.ID),
		attribute.String("work.kind", string(w.Kind)), attribute.String("work.status", string(w.Status)),
		attribute.Int("work.stage", w.Stage), attribute.Int("work.generation", w.Generation)))
	defer sp.End()
	d := &driver{o: o, w: w}
	defer func() {
		if p := recover(); p != nil {
			o.log().Error("work processing panicked", "work_id", w.ID, "panic", p, "stack", string(debug.Stack()))
			o.Metrics.tick("panic")
			_ = o.Store.ReleaseWork(context.WithoutCancel(ctx), w.TenantID, w.ID, o.Owner, o.now().Add(time.Minute))
		}
	}()
	err := d.run(ctx)
	var def *errDeferred
	switch {
	case err == nil:
		o.Metrics.tick("ok")
	case errors.As(err, &def):
		// A resource pool is full: retry later without noise.
		o.Metrics.tick("deferred")
		o.Metrics.poolDeferred(def.pool)
		_ = o.Store.ReleaseWork(context.WithoutCancel(ctx), w.TenantID, w.ID, o.Owner, o.now().Add(def.after))
	case errors.Is(err, work.ErrLeaseLost), errors.Is(err, work.ErrVersionConflict):
		// Someone else changed the Work (a command, another replica). Its
		// own wake decides what happens next; drop our lease if we still
		// hold it.
		o.Metrics.tick("conflict")
		_ = o.Store.ReleaseWork(context.WithoutCancel(ctx), w.TenantID, w.ID, o.Owner, o.now())
	default:
		o.Metrics.tick("error")
		o.log().Warn("work step failed; retrying", "tenant", w.TenantID, "work_id", w.ID, "stage", d.w.Stage, "err", err)
		backoff := 10 * time.Second
		var re *domain.RetryableError
		if errors.As(err, &re) && re.RetryAfter > 0 {
			backoff = re.RetryAfter
		}
		_ = o.Store.ReleaseWork(context.WithoutCancel(ctx), w.TenantID, w.ID, o.Owner, o.now().Add(backoff))
	}
}

// stageState is the scratch of the current stage (Work.StageState).
type stageState struct {
	StageID    string     `json:"stage_id"`
	Generation int        `json:"generation"`
	StartedAt  time.Time  `json:"started_at"`
	Runs       []stageRun `json:"runs,omitempty"`
	WatchID    string     `json:"watch_id,omitempty"`
	ActionID   string     `json:"action_id,omitempty"`
	Verdict    string     `json:"verdict,omitempty"`
	// Holds are the model-run reservations of a running stage.
	Holds []quotaHold `json:"holds,omitempty"`
	// Children are the child Works a children stage spawned.
	Children []stageChild `json:"children,omitempty"`
}

type stageRun struct {
	RunID    string                   `json:"run_id"`
	Role     string                   `json:"role"`
	Required bool                     `json:"required"`
	Status   string                   `json:"status,omitempty"`
	Done     bool                     `json:"done,omitempty"`
	Result   *evidence.ResultEnvelope `json:"result,omitempty"`
	Snapshot string                   `json:"world_snapshot,omitempty"`
	// Deadline is when the assignment's time budget ends (zero: none).
	Deadline time.Time `json:"deadline,omitempty"`
}

// step is one decision: a mutation, and whether to continue immediately.
type step struct {
	m     work.Mutation
	wake  *time.Time // nil with again=false: no wake (waits on a watch/command)
	again bool
}

type driver struct {
	o *Orchestrator
	w work.Work
}

func (d *driver) run(ctx context.Context) error {
	for i := 0; i < d.o.MaxStepsPerClaim; i++ {
		s, err := d.step(ctx)
		if err != nil {
			return err
		}
		last := !s.again || i == d.o.MaxStepsPerClaim-1
		m := s.m
		if s.again {
			now := d.o.now()
			m.WakeAt = &now
		} else if s.wake != nil {
			m.WakeAt = s.wake
		} else {
			m.ClearWake = true
		}
		if last {
			m.ReleaseLease = true
		}
		if err := d.apply(ctx, m); err != nil {
			if name := work.LimitName(err); name != "" {
				// A bound refused the step: stop here for a human instead of
				// retrying the same step forever.
				d.o.Metrics.limit(name)
				b := d.block(err.Error())
				b.m.ClearWake, b.m.ReleaseLease = true, true
				return d.apply(ctx, b.m)
			}
			return err
		}
		if last {
			return nil
		}
	}
	return nil
}

func (d *driver) apply(ctx context.Context, m work.Mutation) error {
	m.TenantID, m.WorkID, m.ExpectedVersion = d.w.TenantID, d.w.ID, d.w.Version
	m.LeaseOwner, m.ActorType, m.ActorID, m.Now = d.o.Owner, "system", "orchestrator", d.o.now()
	from := d.w.Status
	cctx, sp := obs.Tracer().Start(ctx, "work.commit", trace.WithAttributes(attribute.String("work_id", d.w.ID),
		attribute.Int("work.events", len(m.Events)), attribute.Int("work.links", len(m.Links))))
	w, err := d.o.Store.ApplyWork(cctx, m)
	if err != nil {
		sp.SetStatus(codes.Error, err.Error())
		sp.End()
		return err
	}
	sp.SetAttributes(attribute.String("work.status", string(w.Status)), attribute.Int64("work.version", w.Version))
	sp.End()
	for _, f := range m.Findings {
		d.o.Metrics.finding(f.Verdict)
		for range f.Rejected {
			d.o.Metrics.finding("rejected")
		}
	}
	if w.Status.Terminal() || w.Stage != d.w.Stage || w.Generation != d.w.Generation {
		// Left the stage (terminated, replanned, advanced): whatever the
		// stage still reserved is returned. Runs of a stage that was left
		// early are cancelled or ignored, never counted against the budget.
		var st stageState
		if len(d.w.StageState) > 0 && json.Unmarshal(d.w.StageState, &st) == nil && len(st.Holds) > 0 {
			d.o.releaseHolds(ctx, w.TenantID, st.Holds)
		}
	}
	if w.Stage > d.w.Stage && w.Generation == d.w.Generation {
		// A completed stage is a signal other Works may wait for.
		if prev, ok := d.w.CurrentStage(); ok {
			if _, err := d.o.Store.SignalDependents(context.WithoutCancel(ctx), w.TenantID, w.ID, "stage:"+prev.ID); err != nil {
				d.o.log().Warn("signal dependent works", "work_id", w.ID, "stage", prev.ID, "err", err)
			}
		}
	}
	if w.Status.Terminal() && w.Status != from {
		d.recordRunbook(ctx, w)
		// Settle the graph: release or close what waited for this Work and
		// wake its parent. The reconciler repeats it if this is lost.
		if err := d.o.Store.CompleteChild(context.WithoutCancel(ctx), w.TenantID, w.ID, 0); err != nil {
			d.o.log().Warn("settle work graph", "work_id", w.ID, "err", err)
		}
	}
	if w.Status != from {
		d.o.Metrics.transition(string(w.Kind), string(w.Status))
		d.o.Metrics.closed(w, m.Now)
		if w.Status.Terminal() && d.o.OnClose != nil {
			d.o.OnClose(ctx, w)
		}
		d.o.log().Info("work status changed", "tenant", w.TenantID, "work_id", w.ID, "from", from, "to", w.Status,
			"stage", w.Stage, "generation", w.Generation, "reason", w.Reason)
	}
	d.w = w
	return nil
}

func (d *driver) state(stage work.Stage) stageState {
	var st stageState
	if len(d.w.StageState) > 0 {
		_ = json.Unmarshal(d.w.StageState, &st)
	}
	if st.StageID != stage.ID || st.Generation != d.w.Generation {
		st = stageState{StageID: stage.ID, Generation: d.w.Generation, StartedAt: d.o.now()}
	}
	return st
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func statusPtr(s work.Status) *work.Status { return &s }
func strPtr(s string) *string              { return &s }
func intPtr(i int) *int                    { return &i }

func (d *driver) step(ctx context.Context) (step, error) {
	w := &d.w
	now := d.o.now()
	switch {
	case w.Status.Terminal(), w.Status == work.StatusSuspended, w.Status == work.StatusBlocked:
		// Commands move these on; nothing to do until then.
		return step{}, nil
	}
	if lim := w.Template.Limits.MaxDuration.D(); lim > 0 && now.Sub(w.CreatedAt) > lim {
		return d.terminate(work.StatusFailed, "max_duration_exceeded"), nil
	}
	if w.Status == work.StatusNew {
		// A Work held by a dependency does not start, whoever woke it: the
		// graph releases it when its upstream Works are done.
		unmet, err := d.o.Store.UnmetDependencies(ctx, w.TenantID, w.ID)
		if err != nil {
			return step{}, err
		}
		if len(unmet) > 0 {
			reason := "held: waiting for work " + strings.Join(unmet, ", ")
			if w.Reason == reason {
				return step{}, nil
			}
			return step{m: work.Mutation{Reason: strPtr(reason)}}, nil
		}
		return step{m: work.Mutation{Status: statusPtr(work.StatusActive), Stage: intPtr(0), ClearState: true,
			Events: []work.NewEvent{{Type: "work.started", Payload: map[string]any{"template": w.Template.ID,
				"template_version": w.Template.Version}}}}, again: true}, nil
	}
	stage, ok := w.CurrentStage()
	if !ok {
		return d.finish(ctx)
	}
	st := d.state(stage)
	switch stage.Type {
	case work.StageRun, work.StageAgents:
		return d.runStage(ctx, stage, st)
	case work.StageWait:
		return d.waitStage(ctx, stage, st)
	case work.StageAction:
		return d.actionStage(ctx, stage, st)
	case work.StageVerify:
		return d.verifyStage(ctx, stage, st)
	case work.StageConfirm:
		return d.confirmStage(ctx, stage, st)
	case work.StageChildren:
		return d.childrenStage(ctx, stage, st)
	}
	return d.block(fmt.Sprintf("unknown stage type %q", stage.Type)), nil
}

// ---- outcomes of a stage ----

func (d *driver) terminate(s work.Status, reason string) step {
	return step{m: work.Mutation{Status: statusPtr(s), Reason: strPtr(reason), CancelAllWatches: true}}
}

func (d *driver) block(reason string) step {
	return step{m: work.Mutation{Status: statusPtr(work.StatusBlocked), Reason: strPtr(reason),
		Events: []work.NewEvent{{Type: "work.blocked", Payload: map[string]any{"reason": reason, "stage": d.w.Stage}}}}}
}

// advance completes the current stage and moves to the next one.
func (d *driver) advance(stage work.Stage, payload map[string]any) step {
	next := d.w.Stage + 1
	if payload == nil {
		payload = map[string]any{}
	}
	payload["stage_id"], payload["type"], payload["generation"] = stage.ID, stage.Type, d.w.Generation
	d.o.Metrics.stage(string(stage.Type), "completed")
	m := work.Mutation{Stage: &next, ClearState: true, CancelAllWatches: true,
		Events: []work.NewEvent{{Type: "work.stage_completed", Payload: payload}}}
	if d.w.Reason != "" {
		m.Reason = strPtr("")
	}
	if d.w.Status != work.StatusActive {
		m.Status = statusPtr(work.StatusActive)
	}
	return step{m: m, again: true}
}

// fail applies the stage's on_failure policy.
func (d *driver) fail(stage work.Stage, policy, reason string) step {
	d.o.Metrics.stage(string(stage.Type), "failed")
	ev := work.NewEvent{Type: "work.stage_failed", Payload: map[string]any{"stage_id": stage.ID, "reason": reason,
		"policy": policy, "generation": d.w.Generation}}
	switch policy {
	case "", work.OnFailBlock:
		s := d.block(fmt.Sprintf("stage %s: %s", stage.ID, reason))
		s.m.Events = append([]work.NewEvent{ev}, s.m.Events...)
		return s
	case work.OnFailFail:
		s := d.terminate(work.StatusFailed, fmt.Sprintf("stage %s: %s", stage.ID, reason))
		s.m.Events = []work.NewEvent{ev}
		return s
	case work.OnFailSkip, "continue":
		s := d.advance(stage, map[string]any{"skipped": true, "reason": reason})
		s.m.Events = append([]work.NewEvent{ev}, s.m.Events...)
		return s
	}
	idx, ok := d.w.Template.Index(policy)
	if !ok {
		return d.block(fmt.Sprintf("stage %s: %s (unknown on_failure target %q)", stage.ID, reason, policy))
	}
	return d.replan(idx, fmt.Sprintf("stage %s: %s", stage.ID, reason), ev)
}

func (d *driver) timeout(stage work.Stage, reason string) step {
	p := stage.OnTimeout
	if p == "" {
		p = work.OnFailBlock
	}
	return d.fail(stage, p, reason)
}

// replan restarts from stage idx in a new generation.
func (d *driver) replan(idx int, reason string, evs ...work.NewEvent) step {
	if d.w.Replans >= d.w.Template.Limits.MaxReplans {
		s := d.terminate(work.StatusFailed, "max_replans_exceeded: "+reason)
		s.m.Events = evs
		return s
	}
	gen := d.w.Generation + 1
	d.o.Metrics.replan("stage_failure")
	evs = append(evs, work.NewEvent{Type: "work.replanned", Payload: map[string]any{"to_stage": idx, "reason": reason,
		"generation": gen}})
	m := work.Mutation{Stage: &idx, ClearState: true, Generation: &gen, AddReplans: 1, CancelAllWatches: true,
		Reason: strPtr(reason), Events: evs}
	if d.w.Status != work.StatusActive {
		m.Status = statusPtr(work.StatusActive)
	}
	return step{m: m, again: true}
}

// waitFor parks the Work in WAITING until a watch, command or wake time.
func (d *driver) waitFor(st stageState, reason string, wake *time.Time) step {
	m := work.Mutation{StageState: mustJSON(st)}
	if d.w.Status != work.StatusWaiting {
		m.Status = statusPtr(work.StatusWaiting)
	}
	if d.w.Reason != reason {
		m.Reason = strPtr(reason)
	}
	return step{m: m, wake: wake}
}

// queued postpones the current step because a resource pool is full. The
// reason is recorded once (visible on the Work); later attempts are quiet
// deferrals without writes.
func (d *driver) queued(pool, reason string, after time.Duration) (step, error) {
	if d.w.Reason != reason {
		wake := d.o.now().Add(after)
		return step{m: work.Mutation{Reason: strPtr(reason)}, wake: &wake}, nil
	}
	return step{}, deferred(pool, after)
}

// quotaHold is a model-run reservation a stage holds while its runs are in
// flight (kept in the stage state so any replica can renew or release it).
type quotaHold struct {
	ID        string    `json:"id"`
	Token     int64     `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// reservationTTL is the lease of a stage's model-run reservation.
func (o *Orchestrator) reservationTTL() time.Duration {
	if o.Pools != nil && o.Pools.Limits.ReservationTTL > 0 {
		return o.Pools.Limits.ReservationTTL.D()
	}
	return 10 * time.Minute
}

// stageKey identifies a stage of one Work generation (reservation dedup).
func stageKey(w *work.Work, stage work.Stage) string {
	return fmt.Sprintf("stage:%s:g%d:%s", w.ID, w.Generation, stage.ID)
}

// admit reserves the model-run budget for the n runs a stage is about to
// start: the queue's own budget, then the tenant total shared by every
// queue except critical-mitigation (which is bounded only by its own queue
// and for which CriticalReservedModelRuns is held back from the total).
//
// The reservation is taken in the store under the budget's row lock, so
// replicas cannot admit more than the capacity together. It is idempotent
// per stage and generation: a retried step re-reads the reservation it
// already holds. A stage wider than a whole budget is admitted when
// nothing else holds units, so it cannot starve forever. reason is the
// human-readable cause when the budget is full.
func (o *Orchestrator) admit(ctx context.Context, w *work.Work, stage work.Stage, n int) (holds []quotaHold, reason string, err error) {
	qcap := o.limit(w.Queue).ModelRunsPerTenant
	total := 0
	if o.Pools != nil && w.Queue != work.QueueCritical {
		if total = o.Pools.Limits.ModelRunsPerTenant; total > 0 {
			total = max(total-o.Pools.Limits.CriticalReservedModelRuns, 1)
		}
	}
	if qcap <= 0 && total <= 0 {
		return nil, "", nil
	}
	base := quota.LeaseRequest{TenantID: w.TenantID, WorkID: w.ID, Resource: quota.ResourceModelRun, Units: int64(n),
		TTL: o.reservationTTL(), Owner: o.Owner, Oversize: true, Now: o.now()}
	var reqs []quota.LeaseRequest
	if qcap > 0 {
		r := base
		r.Queue, r.Capacity, r.DedupKey = string(w.Queue), int64(qcap), stageKey(w, stage)+":queue"
		reqs = append(reqs, r)
	}
	if total > 0 {
		r := base
		r.Queue, r.Capacity, r.DedupKey = quota.AnyQueue, int64(total), stageKey(w, stage)+":tenant"
		reqs = append(reqs, r)
	}
	held, err := quota.Group(ctx, o.Store, reqs...)
	var ex *quota.ExhaustedError
	switch {
	case errors.As(err, &ex):
		scope := string(w.Queue)
		if ex.Budget.Queue == quota.AnyQueue {
			scope = "tenant"
		}
		return nil, fmt.Sprintf("queued: %s model run budget (%d of %d reserved)", scope, ex.Budget.Reserved, ex.Budget.Capacity), nil
	case err != nil:
		return nil, "", err
	}
	for _, h := range held {
		holds = append(holds, quotaHold{ID: h.ID, Token: h.Token, ExpiresAt: h.ExpiresAt})
	}
	return holds, "", nil
}

// renewHolds extends a running stage's reservations once half their lease
// has passed. A reservation the reaper already freed is not an error: the
// reaper renews reservations whose runs are in flight, so a lost hold
// means this stage was considered dead and its budget is counted again
// when the next stage reserves.
func (o *Orchestrator) renewHolds(ctx context.Context, tenantID string, holds []quotaHold) bool {
	ttl := o.reservationTTL()
	now := o.now()
	changed := false
	for i := range holds {
		if holds[i].ExpiresAt.Sub(now) > ttl/2 {
			continue
		}
		if err := o.Store.Renew(ctx, tenantID, holds[i].ID, holds[i].Token, ttl); err != nil {
			if !errors.Is(err, quota.ErrFenced) {
				o.log().Warn("renew model run reservation", "tenant", tenantID, "reservation_id", holds[i].ID, "err", err)
			}
			continue
		}
		holds[i].ExpiresAt, changed = now.Add(ttl), true
	}
	return changed
}

// releaseHolds returns a stage's reservations (fenced; double release is a
// no-op).
func (o *Orchestrator) releaseHolds(ctx context.Context, tenantID string, holds []quotaHold) {
	freed := 0
	for _, h := range holds {
		err := o.Store.Release(context.WithoutCancel(ctx), tenantID, h.ID, h.Token)
		switch {
		case err == nil:
			freed++
		case !errors.Is(err, quota.ErrFenced):
			o.log().Warn("release model run reservation", "tenant", tenantID, "reservation_id", h.ID, "err", err)
		}
	}
	if freed > 0 {
		// Freed budget is offered to the Works waiting for it right away.
		if _, err := o.Store.WakeDeferred(context.WithoutCancel(ctx), tenantID, o.now(), 4*freed); err != nil {
			o.log().Warn("wake deferred works", "tenant", tenantID, "err", err)
		}
	}
}

// ---- run / agents ----

func (d *driver) runStage(ctx context.Context, stage work.Stage, st stageState) (step, error) {
	w := &d.w
	now := d.o.now()
	if len(st.Runs) == 0 {
		specs := stage.Agents
		if stage.Type == work.StageRun {
			role := stage.Phase
			if role == "" {
				role = "owner"
			}
			specs = []work.AgentSpec{{Role: role, RunSpec: *stage.Run, Required: true}}
		}
		if w.RunsStarted+len(specs) > w.Template.Limits.MaxRuns {
			return d.terminate(work.StatusFailed, "max_runs_exceeded"), nil
		}
		if lim := w.Template.Limits.MaxCostUSD; lim > 0 && float64(w.CostMicros) >= lim*1e6 {
			d.o.Metrics.limit("max_cost_usd")
			return d.terminate(work.StatusFailed, fmt.Sprintf("max_cost_exceeded: spent $%.2f of $%.2f",
				float64(w.CostMicros)/1e6, lim)), nil
		}
		holds, reason, err := d.o.admit(ctx, w, stage, len(specs))
		if err != nil {
			return step{}, err
		}
		if reason != "" {
			return d.queued("model_runs", reason, 10*time.Second)
		}
		st.Holds = holds
		summaries, err := d.summaries(ctx)
		if err != nil {
			return step{}, err
		}
		var links []work.RunLink
		var assignments []Assignment
		snapID := d.o.pinSnapshot(ctx, w)
		keys := make([]string, len(specs))
		for i, a := range specs {
			keys[i] = fmt.Sprintf("work:%s:g%d:%s:%s", w.ID, w.Generation, stage.ID, a.Role)
		}
		plan := d.planEvidence(ctx, stage, specs, keys)
		for i, a := range specs {
			goal, err := d.o.modelGoal(a.Goal, w, summaries, plan.text[a.Role])
			if err != nil {
				d.o.releaseHolds(ctx, w.TenantID, holds)
				return d.fail(stage, work.OnFailBlock, err.Error()), nil
			}
			key := keys[i]
			labels := map[string]string{"work_id": w.ID, "work_stage": stage.ID}
			if snapID != "" {
				labels[SnapshotLabel] = snapID
			}
			runID, err := d.o.Runs.ReserveRun(ctx, RunRequest{TenantID: w.TenantID, WorkID: w.ID, Generation: w.Generation, StageID: stage.ID, ActorID: "work:" + w.ID, Goal: goal,
				ProfileID: a.ProfileID, ProfileVersion: a.ProfileVersion, Mode: a.Mode, Services: w.Goal.SubjectRefs,
				Labels: labels, Source: "work:" + string(w.Kind), IdempotencyKey: key})
			if err != nil {
				var rej *domain.RejectedError
				if errors.As(err, &rej) {
					d.o.releaseHolds(ctx, w.TenantID, holds)
					return d.fail(stage, stage.OnFailure, "run rejected: "+rej.Reason), nil
				}
				return step{}, err
			}
			sr := stageRun{RunID: runID, Role: a.Role, Required: a.Required, Snapshot: snapID}
			as := Assignment{WorkID: w.ID, StageID: stage.ID, Role: a.Role, Goal: cpkit.Truncate(a.Goal, 300), RunID: runID,
				SnapshotID: snapID, EvidenceScope: w.Goal.SubjectRefs, Required: a.Required}
			if a.Budget != nil {
				as.Budget = *a.Budget
				if dl := a.Budget.MaxDuration.D(); dl > 0 {
					sr.Deadline = now.Add(dl)
					as.Deadline = sr.Deadline
				}
			}
			assignments = append(assignments, as)
			st.Runs = append(st.Runs, sr)
			links = append(links, work.RunLink{RunID: runID, Role: work.RoleOwner, Phase: stage.Phase, StageID: stage.ID,
				AgentRole: a.Role, Generation: w.Generation, Required: a.Required})
		}
		d.o.Metrics.stage(string(stage.Type), "started")
		m := work.Mutation{StageState: mustJSON(st), Links: links, AddRuns: len(links),
			Events: []work.NewEvent{{Type: "work.runs_started", Payload: map[string]any{"stage_id": stage.ID,
				"runs": st.Runs, "assignments": assignments, "pending_activation": true}}}}
		if plan.event != nil {
			m.Events = append(m.Events, *plan.event)
		}
		if w.Status != work.StatusActive {
			m.Status = statusPtr(work.StatusActive)
		}
		return step{m: m, again: true}, nil
	}

	changed, needsHuman, allDone := false, false, true
	linkStatus := map[string]string{}
	linkResults := map[string]work.LinkResult{}
	var cost int64
	for i := range st.Runs {
		r := &st.Runs[i]
		if r.Done {
			continue
		}
		rs, err := d.o.Runs.RunState(ctx, w.TenantID, r.RunID)
		if err != nil {
			return step{}, fmt.Errorf("read run %s: %w", r.RunID, err)
		}
		if rs.Status != r.Status {
			r.Status, changed = rs.Status, true
			linkStatus[r.RunID] = rs.Status
		}
		if !rs.Terminal && !r.Deadline.IsZero() && !now.Before(r.Deadline) {
			// The assignment's time budget is spent: stop the run and record
			// a timeout. The stage goes on with what the other agents found.
			if err := d.o.Runs.CancelRun(ctx, w.TenantID, r.RunID, "work:"+w.ID, "assignment time budget exceeded",
				"budget:"+r.RunID); err != nil {
				return step{}, fmt.Errorf("cancel run %s: %w", r.RunID, err)
			}
			rs = RunState{Status: "TIMEOUT", Terminal: true, FailureReason: "assignment time budget exceeded"}
			r.Status, changed = rs.Status, true
			linkStatus[r.RunID] = "CANCELLED"
		}
		if rs.Status == "PENDING_ACTIVATION" {
			if err := d.o.Runs.ActivateRun(ctx, work.RunActivation{TenantID: w.TenantID, WorkID: w.ID,
				RunID: r.RunID, Generation: w.Generation, StageID: stage.ID, ExpectedVersion: w.Version, LeaseOwner: d.o.Owner}); err != nil {
				return step{}, fmt.Errorf("activate linked run %s: %w", r.RunID, err)
			}
		}
		if !rs.Terminal {
			allDone = false
			needsHuman = needsHuman || rs.NeedsHuman
			continue
		}
		env := evidence.ParseResult(r.RunID, r.Role, rs.Status, rs.Result)
		env.Required = r.Required
		if env.FailureReason == "" {
			env.FailureReason = rs.FailureReason
		}
		r.Result, r.Done, changed = &env, true, true
		runCost := int64(env.CostUSD * 1e6)
		cost += runCost
		res := work.LinkResult{Outcome: assignmentOutcome(env), CostMicros: runCost, FinishedAt: now}
		if env.Summary != "" || len(env.Hypotheses) > 0 {
			rec, _, err := d.o.Store.AddEvidence(ctx, evidence.Record{TenantID: w.TenantID, WorkID: w.ID,
				SourceType: evidence.SourceRun, SourceRef: "run://" + r.RunID, Summary: env.Summary, RunID: r.RunID,
				AgentRole: r.Role, TrustTier: "T4"})
			switch name := work.LimitName(err); {
			case name != "":
				// The evidence budget is spent: the run result stays on the
				// run link; only the ledger copy is dropped.
				d.o.Metrics.limit(name)
				d.o.log().Warn("run evidence dropped", "work_id", w.ID, "run_id", r.RunID, "err", err)
			case err != nil:
				return step{}, err
			default:
				res.EvidenceRefs = append(res.EvidenceRefs, "evidence:"+rec.ID)
			}
		}
		linkResults[r.RunID] = res
		d.o.Metrics.assignment(string(stage.Type), res.Outcome)
	}
	if !allDone {
		m := work.Mutation{LinkStatus: linkStatus, LinkResults: linkResults, AddCost: cost}
		if d.o.renewHolds(ctx, w.TenantID, st.Holds) {
			changed = true
		}
		if changed {
			m.StageState = mustJSON(st)
		}
		poll := d.o.RunPoll
		reason := ""
		if needsHuman {
			poll = 3 * d.o.RunPoll
			reason = "a run of stage " + stage.ID + " waits for a human"
		}
		want := work.StatusActive
		if needsHuman {
			want = work.StatusWaiting
		}
		if w.Status != want {
			m.Status = statusPtr(want)
		}
		if w.Reason != reason {
			m.Reason = strPtr(reason)
		}
		wake := now.Add(poll)
		return step{m: m, wake: &wake}, nil
	}

	// Every run of the stage ended: its model-run budget is free again. A
	// retry of this step after a failed commit finds nothing to release.
	d.o.releaseHolds(ctx, w.TenantID, st.Holds)
	var envs []evidence.ResultEnvelope
	var proposed []json.RawMessage
	for _, r := range st.Runs {
		if r.Result != nil {
			envs = append(envs, *r.Result)
			if len(r.Result.Proposed) > 0 {
				proposed = append(proposed, r.Result.Proposed)
			}
		}
	}
	rep := evidence.Merge(envs)
	payload := map[string]any{"summary": summarize(rep), "verdict": rep.Verdict, "conflicts": rep.Conflicts,
		"gaps": rep.Gaps, "cost_usd": rep.CostUSD, "proposed": proposed}
	if len(rep.NextChecks) > 0 {
		payload["next_checks"] = rep.NextChecks
	}
	syn, hypotheses, err := d.synthesize(ctx, envs)
	if errors.Is(err, evidence.ErrDecisionLimit) {
		s := d.block(err.Error())
		s.m.LinkStatus, s.m.LinkResults, s.m.AddCost = linkStatus, linkResults, cost
		d.o.Metrics.limit("adjudication")
		return s, nil
	}
	if err != nil {
		return step{}, err
	}
	if len(syn.Findings) > 0 || len(syn.Unavailable) > 0 {
		payload["findings"] = map[string]any{"count": len(syn.Findings), "contested": syn.Contested,
			"rejected_refs": syn.Rejected, "unavailable": syn.Unavailable, "distinguishing_checks": syn.NextChecks}
	}
	if max := w.Goal.Constraints.MaxCostUSD; max > 0 && float64(w.CostMicros+cost)/1e6 > max {
		s := d.block(fmt.Sprintf("cost budget exceeded (%.2f USD > %.2f USD)", float64(w.CostMicros+cost)/1e6, max))
		s.m.LinkStatus, s.m.LinkResults, s.m.AddCost = linkStatus, linkResults, cost
		s.m.Findings, s.m.Hypotheses = syn.Findings, hypotheses
		return s, nil
	}
	if rep.Verdict == "FAILED" {
		reason := "runs failed"
		if len(rep.RequiredFailed) > 0 {
			reason = "required agents failed: " + strings.Join(rep.RequiredFailed, ", ")
		}
		s := d.fail(stage, stage.OnFailure, reason)
		s.m.LinkStatus, s.m.LinkResults, s.m.AddCost = linkStatus, linkResults, cost
		s.m.Findings, s.m.Hypotheses = syn.Findings, hypotheses
		return s, nil
	}
	s := d.advance(stage, payload)
	s.m.LinkStatus, s.m.LinkResults, s.m.AddCost = linkStatus, linkResults, cost
	s.m.Findings, s.m.Hypotheses = syn.Findings, hypotheses
	return s, nil
}

// synthesize prepares the batch; ApplyWork commits it with the stage transition.
func (d *driver) synthesize(ctx context.Context, envs []evidence.ResultEnvelope) (Synthesis, []evidence.Hypothesis, error) {
	has := false
	for _, e := range envs {
		has = has || len(e.Hypotheses) > 0 || e.Status != "COMPLETED"
	}
	if !has {
		return Synthesis{}, nil, nil
	}
	ledger, err := d.o.Store.ListEvidence(ctx, d.w.TenantID, d.w.ID, 1000)
	if err != nil {
		return Synthesis{}, nil, err
	}
	if len(ledger) >= 1000 {
		return Synthesis{}, nil, fmt.Errorf("%w: evidence ledger reaches 1000 records", evidence.ErrDecisionLimit)
	}
	existing, err := d.o.Store.ListHypotheses(ctx, d.w.TenantID, d.w.ID)
	if err != nil {
		return Synthesis{}, nil, err
	}
	byKey := map[string]evidence.Hypothesis{}
	scope := evidence.DecisionScope{TenantID: d.w.TenantID, WorkID: d.w.ID, Generation: d.w.Generation}
	inputs := append([]evidence.ResultEnvelope(nil), envs...)
	for _, h := range existing {
		byKey[h.Key] = h
		if h.Decision != nil && !h.CurrentDecisionFor(scope) {
			continue
		}
		if len(envs) == 0 {
			continue
		}
		claim := ""
		if h.Decision != nil && h.Decision.ClaimedSupport {
			claim = "SUPPORTED"
		}
		inputs = append(inputs, evidence.ResultEnvelope{RunID: envs[0].RunID, AgentRole: "prior_findings", Status: "COMPLETED", Hypotheses: []evidence.ResultHypothesis{{Statement: h.Statement, Category: h.Category, Status: claim, Supports: h.Supporting, Refutes: h.Refuting}}})
	}
	syn, hypotheses, err := evidence.Adjudicate(scope, inputs, ledger, d.o.now())
	if err != nil {
		return syn, nil, err
	}
	for i := range hypotheses {
		h := &hypotheses[i]
		if old, ok := byKey[h.Key]; ok {
			h.ID, h.Version = old.ID, old.Version
		} else {
			h.ID = cpkit.NewID()
		}
	}
	return syn, hypotheses, nil
}

// assignmentOutcome classifies how an agent assignment ended.
func assignmentOutcome(env evidence.ResultEnvelope) string {
	switch {
	case env.Status != "COMPLETED":
		return strings.ToLower(env.Status)
	case env.Verification != "":
		return "completed:" + strings.ToLower(cpkit.Truncate(env.Verification, 40))
	}
	return "completed"
}

func summarize(rep evidence.MergeReport) string {
	if len(rep.Summaries) == 1 {
		for _, s := range rep.Summaries {
			return s
		}
	}
	var b strings.Builder
	keys := make([]string, 0, len(rep.Summaries))
	for k := range rep.Summaries {
		keys = append(keys, k)
	}
	sortStrings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "[%s] %s\n", k, rep.Summaries[k])
	}
	if len(rep.Conflicts) > 0 {
		fmt.Fprintf(&b, "Conflicting conclusions on %d hypotheses.\n", len(rep.Conflicts))
	}
	if len(rep.Gaps) > 0 {
		fmt.Fprintf(&b, "Missing investigations: %s.\n", strings.Join(rep.Gaps, ", "))
	}
	return cpkit.Truncate(strings.TrimSpace(b.String()), evidence.MaxSummary)
}

// summaries returns the latest completion summary per stage, for
// ${stage.<id>.summary}.
func (d *driver) summaries(ctx context.Context) (map[string]string, error) {
	out := map[string]string{}
	err := d.stageEvents(ctx, func(stageID string, p map[string]any) {
		if s, ok := p["summary"].(string); ok {
			out[stageID] = s
		}
	})
	return out, err
}

// stageEvents calls fn for each work.stage_completed event (oldest first)
// in the recent timeline.
func (d *driver) stageEvents(ctx context.Context, fn func(stageID string, payload map[string]any)) error {
	after := d.w.LastSeq - 1000
	if after < 0 {
		after = 0
	}
	evs, err := d.o.Store.ListWorkEvents(ctx, d.w.TenantID, d.w.ID, after, 1000)
	if err != nil {
		return err
	}
	for _, e := range evs {
		if e.Type != "work.stage_completed" {
			continue
		}
		var p map[string]any
		if json.Unmarshal(e.Payload, &p) != nil {
			continue
		}
		id, _ := p["stage_id"].(string)
		fn(id, p)
	}
	return nil
}

// ---- wait ----

func (d *driver) waitStage(ctx context.Context, stage work.Stage, st stageState) (step, error) {
	w := &d.w
	if st.WatchID == "" {
		wt, err := continuous.Build(*stage.Wait, w.TenantID, w.ID, w.Generation, w.Goal.SubjectRefs, d.o.now())
		if err != nil {
			return d.block(fmt.Sprintf("stage %s: %v", stage.ID, err)), nil
		}
		st.WatchID = wt.ID
		s := d.waitFor(st, "waiting: "+cpkit.Truncate(firstNonEmpty(stage.Wait.Purpose, string(stage.Wait.Mode)), 200), nil)
		s.m.Watches = []continuous.Watch{wt}
		d.o.Metrics.stage(string(stage.Type), "started")
		return s, nil
	}
	wt, err := d.o.Store.GetWatch(ctx, w.TenantID, st.WatchID)
	if err != nil {
		return step{}, err
	}
	switch wt.Status {
	case continuous.Fired:
		return d.advance(stage, map[string]any{"watch_id": wt.ID, "fired_by": wt.FiredBy}), nil
	case continuous.Expired:
		return d.timeout(stage, "wait timed out"), nil
	case continuous.Cancelled:
		return d.block(fmt.Sprintf("stage %s: watch was cancelled", stage.ID)), nil
	}
	if wt.Mode == continuous.Periodic && wt.Checks > 0 {
		s := d.advance(stage, map[string]any{"watch_id": wt.ID, "checks": wt.Checks})
		return s, nil
	}
	return d.waitFor(st, d.w.Reason, nil), nil
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

// ---- action ----

func (d *driver) actionStage(ctx context.Context, stage work.Stage, st stageState) (step, error) {
	w := &d.w
	rt := d.o.Actions
	if rt == nil {
		return d.block("action stages need the action runtime (HARNESS_ACTION_V2_ENABLED)"), nil
	}
	spec := stage.Action
	now := d.o.now()
	if st.ActionID == "" {
		existing, err := d.o.Store.ListActions(ctx, w.TenantID, w.ID)
		if err != nil {
			return step{}, err
		}
		if len(existing) >= w.Template.Limits.MaxActions {
			return d.terminate(work.StatusFailed, "max_actions_exceeded"), nil
		}
		name, target, args := spec.Name, spec.Target, spec.Args
		runID := ""
		if spec.FromProposal != "" {
			p, ok, err := d.proposal(ctx, spec.FromProposal, spec.Allowed)
			if err != nil {
				return step{}, err
			}
			if !ok {
				return d.fail(stage, stage.OnFailure, "no acceptable action was proposed by stage "+spec.FromProposal), nil
			}
			name, runID = p.Name, p.RunID
			if target == "" {
				target = p.Target
			}
			if len(args) == 0 {
				args = p.Args
			}
		}
		target = work.Render(target, w, nil)
		if len(args) > 0 {
			rendered, err := work.RenderJSON(args, w)
			if err != nil {
				return d.fail(stage, stage.OnFailure, "action args: "+err.Error()), nil
			}
			args = rendered
		}
		// A template that is a runbook may only carry a production action
		// while the runbook is active, in date, rehearsed and written for
		// this action. Otherwise a human decides what to do.
		if probs, err := d.runbookProblems(ctx, name); err != nil {
			return step{}, err
		} else if len(probs) > 0 {
			return d.fail(stage, work.OnFailBlock, strings.Join(probs, "; ")), nil
		}
		owner := w.OwnerActor
		if owner == "" {
			owner = "work:" + w.ID
		}
		rec, err := rt.Propose(ctx, action.ProposeRequest{TenantID: w.TenantID, WorkID: w.ID, RunID: runID,
			StageID: stage.ID, Generation: w.Generation, Name: name, Target: target, Args: args, Effect: spec.Effect,
			ProposedBy: owner, WritesMode: w.Goal.Constraints.ProductionWrites, AllowedTargets: w.Goal.SubjectRefs})
		if err != nil {
			if errors.Is(err, action.ErrNotApplied) || errors.Is(err, outcome.ErrInvalidSpec) {
				return d.fail(stage, stage.OnFailure, "action: "+err.Error()), nil
			}
			return step{}, err
		}
		d.o.Metrics.action(string(rec.State))
		st.ActionID = rec.ID
		d.o.Metrics.stage(string(stage.Type), "started")
		return step{m: work.Mutation{StageState: mustJSON(st), Events: []work.NewEvent{{Type: "work.action_proposed",
			Payload: map[string]any{"action_id": rec.ID, "action": rec.ActionName, "target": rec.TargetRef,
				"state": rec.State, "risk": rec.Risk, "intent_hash": rec.IntentHash, "reason": rec.Reason}}}}, again: true}, nil
	}

	rec, err := d.o.Store.GetAction(ctx, w.TenantID, st.ActionID)
	if err != nil {
		return step{}, err
	}
	ap := map[string]any{"action_id": rec.ID, "state": rec.State}
	switch rec.State {
	case action.StateRejected:
		return d.fail(stage, stage.OnFailure, "action rejected: "+rec.Reason), nil
	case action.StateWaitingApproval:
		deadline := rec.ExpiresAt
		if t := spec.ApprovalTimeout.D(); t > 0 && st.StartedAt.Add(t).Before(deadline) {
			deadline = st.StartedAt.Add(t)
		}
		if !now.Before(deadline) {
			if _, err := rt.Expire(ctx, w.TenantID, rec.ID); err != nil && !errors.Is(err, action.ErrInvalidState) {
				return step{}, err
			}
			return d.timeout(stage, "approval timed out"), nil
		}
		return d.waitFor(st, fmt.Sprintf("awaiting approval of %s on %s", rec.ActionName, rec.TargetRef), &deadline), nil
	case action.StateReady:
		if w.Status != work.StatusActive {
			return step{again: true, m: work.Mutation{Status: statusPtr(work.StatusActive)}}, nil
		}
		release, ok := d.o.Pools.actionSlot(w.TenantID)
		if !ok {
			return d.queued("actions", "queued: production action pool is full", 5*time.Second)
		}
		res, err := rt.Execute(ctx, w.TenantID, rec.ID)
		release()
		switch {
		case errors.Is(err, action.ErrTargetBusy):
			wake := now.Add(30 * time.Second)
			return d.waitFor(st, "target "+rec.TargetRef+" is busy with another action", &wake), nil
		case errors.Is(err, action.ErrPendingEffect):
			// An agent's tool call on the same target has no known result:
			// wait for the kernel to settle it (or a human to reconcile).
			wake := now.Add(time.Minute)
			return d.waitFor(st, cpkit.Truncate(err.Error(), 300), &wake), nil
		case errors.Is(err, action.ErrPrecondition), errors.Is(err, action.ErrInvalidState):
			// The runtime recorded the new state (rejected / expired);
			// re-read it.
			return step{again: true, m: work.Mutation{}}, nil
		case err != nil && res.State == "":
			return step{}, err
		}
		d.o.Metrics.action(string(res.State))
		return step{again: true, m: work.Mutation{Events: []work.NewEvent{{Type: "work.action_executed",
			Payload: map[string]any{"action_id": res.ID, "state": res.State, "error": res.LastError}}}}}, nil
	case action.StateDispatched:
		if rec.DispatchedAt != nil && now.Sub(*rec.DispatchedAt) > 2*time.Minute {
			release, ok := d.o.Pools.actionSlot(w.TenantID)
			if !ok {
				return d.queued("actions", "queued: production action pool is full", 5*time.Second)
			}
			_, err := rt.Reconcile(ctx, w.TenantID, rec.ID)
			release()
			if err != nil {
				return step{}, err
			}
			return step{again: true}, nil
		}
		wake := now.Add(15 * time.Second)
		return step{m: work.Mutation{StageState: mustJSON(st)}, wake: &wake}, nil
	case action.StateApplied, action.StateVerifying:
		timeout := spec.EffectTimeout.D()
		if timeout <= 0 {
			timeout = 10 * time.Minute
		}
		start := rec.UpdatedAt
		if rec.DispatchedAt != nil {
			start = *rec.DispatchedAt
		}
		res, err := rt.VerifyEffect(ctx, w.TenantID, rec.ID, start.Add(timeout))
		if err != nil {
			return step{}, err
		}
		if res.State == action.StateVerifying {
			wake := now.Add(15 * time.Second)
			if d.w.Status != work.StatusActive {
				return step{m: work.Mutation{Status: statusPtr(work.StatusActive)}, wake: &wake}, nil
			}
			return step{wake: &wake}, nil
		}
		d.o.Metrics.action(string(res.State))
		return step{again: true}, nil
	case action.StateVerified:
		return d.advance(stage, ap), nil
	case action.StateFailed:
		return d.fail(stage, stage.OnFailure, "action failed: "+rec.LastError), nil
	case action.StateDegraded:
		return d.fail(stage, stage.OnFailure, "action effect not achieved: "+rec.Reason), nil
	case action.StateEffectUnknown, action.StateManualRecovery:
		// Never replayed automatically: reconcile or resolve by a human.
		return d.block(fmt.Sprintf("action %s effect is unknown; reconcile or resolve it", rec.ID)), nil
	default:
		return d.block(fmt.Sprintf("action %s is %s", rec.ID, rec.State)), nil
	}
}

// runbookProblems checks the runbook behind the Work's template.
func (d *driver) runbookProblems(ctx context.Context, actionName string) ([]string, error) {
	ref := d.w.Template.Runbook
	if ref == nil {
		return nil, nil
	}
	rb, err := d.o.Store.GetRunbook(ctx, d.w.TenantID, ref.ID, ref.Version)
	switch {
	case errors.Is(err, runbook.ErrNotFound):
		return []string{fmt.Sprintf("runbook %s is not registered", ref.ID)}, nil
	case err != nil:
		return nil, err
	}
	return rb.Problems(actionName, d.o.now()), nil
}

// recordRunbook adds a closed Work to its runbook's track record.
func (d *driver) recordRunbook(ctx context.Context, w work.Work) {
	ref := w.Template.Runbook
	if ref == nil {
		return
	}
	cctx := context.WithoutCancel(ctx)
	rb, err := d.o.Store.GetRunbook(cctx, w.TenantID, ref.ID, ref.Version)
	if err != nil {
		return
	}
	res := runbook.Result{Succeeded: w.Status == work.StatusSucceeded}
	if acts, err := d.o.Store.ListActions(cctx, w.TenantID, w.ID); err == nil {
		for _, a := range acts {
			res.UnknownEffect = res.UnknownEffect || a.State == action.StateEffectUnknown || a.State == action.StateManualRecovery
		}
	}
	if err := d.o.Store.RecordExecution(cctx, w.TenantID, rb.ID, rb.Version, res); err != nil {
		d.o.log().Warn("record runbook execution", "work_id", w.ID, "runbook", rb.ID, "err", err)
	}
}

type proposal struct {
	Name   string          `json:"action"`
	Target string          `json:"target"`
	Args   json.RawMessage `json:"args"`
	RunID  string          `json:"-"`
}

// proposal picks the first proposal of a stage whose action is allowed.
// Proposals are untrusted model output: only the action name (from the
// allow-list), target and args are taken, and the action runtime applies
// policy, preflight and approval as for any other action.
func (d *driver) proposal(ctx context.Context, stageID string, allowed []string) (proposal, bool, error) {
	var out proposal
	found := false
	err := d.stageEvents(ctx, func(id string, p map[string]any) {
		if id != stageID {
			return
		}
		list, _ := p["proposed"].([]any)
		found = false
		for _, raw := range list {
			b, _ := json.Marshal(raw)
			var x struct {
				proposal
				Name2 string `json:"name"`
				Tool  string `json:"tool"`
			}
			if json.Unmarshal(b, &x) != nil {
				continue
			}
			pr := x.proposal
			pr.Name = firstNonEmpty(pr.Name, x.Name2, x.Tool)
			for _, a := range allowed {
				if a == pr.Name {
					out, found = pr, true
					return
				}
			}
		}
	})
	return out, found, err
}

// ---- verify / confirm ----

func (d *driver) latestOutcome(ctx context.Context, match func(outcome.Evaluation) bool) (*outcome.Evaluation, error) {
	evs, err := d.o.Store.ListOutcomes(ctx, d.w.TenantID, d.w.ID, 50)
	if err != nil {
		return nil, err
	}
	for i := range evs {
		if evs[i].Generation == d.w.Generation && match(evs[i]) {
			return &evs[i], nil
		}
	}
	return nil, nil
}

func (d *driver) verifyStage(ctx context.Context, stage work.Stage, st stageState) (step, error) {
	w := &d.w
	var spec *outcome.Spec
	if stage.Verify != nil && stage.Verify.Spec != nil {
		spec = stage.Verify.Spec
	} else {
		spec = w.Goal.Success
	}
	if spec == nil {
		// No machine-checkable success: a human confirms (RFC §11).
		return d.confirmStage(ctx, stage, st)
	}
	if d.o.Evaluator == nil {
		return d.block("outcome verification is not configured"), nil
	}
	if w.Status != work.StatusVerifying {
		d.o.Metrics.stage(string(stage.Type), "started")
		return step{m: work.Mutation{Status: statusPtr(work.StatusVerifying), StageState: mustJSON(st), Reason: strPtr("")}, again: true}, nil
	}
	hash := spec.Hash()
	prev, err := d.latestOutcome(ctx, func(e outcome.Evaluation) bool { return e.SpecHash == hash })
	if err != nil {
		return step{}, err
	}
	now := d.o.now()
	// Source errors become INCONCLUSIVE objectives inside the evaluation.
	ectx, sp := obs.Tracer().Start(ctx, "outcome.evaluate", trace.WithAttributes(attribute.String("work_id", w.ID),
		attribute.String("work.stage_id", stage.ID)))
	ev := d.o.Evaluator.Evaluate(ectx, *spec, outcome.Input{TenantID: w.TenantID, WorkID: w.ID, Now: now,
		DefaultTarget: w.Goal.PrimarySubject(), Previous: prev})
	sp.SetAttributes(attribute.String("outcome.verdict", string(ev.Verdict)))
	sp.End()
	ev.ID, ev.Stage, ev.Generation = cpkit.NewID(), stage.ID, w.Generation
	d.o.Metrics.outcome(string(ev.Verdict))
	recheck, timeout := time.Minute, 30*time.Minute
	if stage.Verify != nil {
		if v := stage.Verify.RecheckEvery.D(); v > 0 {
			recheck = v
		}
		if v := stage.Verify.Timeout.D(); v > 0 {
			timeout = v
		}
	}
	st.Verdict = string(ev.Verdict)
	if ev.Verdict == outcome.Satisfied {
		if w.Stage+1 >= len(w.Template.Stages) {
			return d.succeed(&ev), nil
		}
		s := d.advance(stage, map[string]any{"verdict": ev.Verdict, "evaluation_id": ev.ID})
		s.m.Outcome = &ev
		return s, nil
	}
	if now.Sub(st.StartedAt) >= timeout {
		s := d.timeout(stage, "verification timed out ("+string(ev.Verdict)+")")
		if ev.Verdict == outcome.NotSatisfied {
			s = d.fail(stage, stage.OnFailure, "success criteria not met")
		}
		s.m.Outcome = &ev
		return s, nil
	}
	// Record only verdict changes; the stabilization clock lives in the
	// latest evaluation, so keep it current when it started.
	var rec *outcome.Evaluation
	if prev == nil || prev.Verdict != ev.Verdict || prev.RawVerdict != ev.RawVerdict ||
		!sameTime(prev.SatisfiedSince, ev.SatisfiedSince) {
		rec = &ev
	}
	wake := now.Add(recheck)
	return step{m: work.Mutation{StageState: mustJSON(st), Outcome: rec}, wake: &wake}, nil
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// confirmStage waits for a human decision recorded by the confirm command
// as an outcome of this stage.
func (d *driver) confirmStage(ctx context.Context, stage work.Stage, st stageState) (step, error) {
	ev, err := d.latestOutcome(ctx, func(e outcome.Evaluation) bool {
		return e.Stage == stage.ID && !e.EvaluatedAt.Before(st.StartedAt)
	})
	if err != nil {
		return step{}, err
	}
	if ev != nil {
		switch ev.Verdict {
		case outcome.Satisfied:
			if d.w.Stage+1 >= len(d.w.Template.Stages) {
				return d.succeedFrom(ev), nil
			}
			return d.advance(stage, map[string]any{"verdict": ev.Verdict, "evaluation_id": ev.ID, "confirmed": true}), nil
		case outcome.NotSatisfied:
			return d.fail(stage, stage.OnFailure, "a human reported the goal is not met"), nil
		}
	}
	var deadline *time.Time
	if stage.Confirm != nil && stage.Confirm.Timeout > 0 {
		t := st.StartedAt.Add(stage.Confirm.Timeout.D())
		if !d.o.now().Before(t) {
			return d.timeout(stage, "confirmation timed out"), nil
		}
		deadline = &t
	}
	if len(d.w.StageState) == 0 || d.w.Status != work.StatusWaiting {
		d.o.Metrics.stage(string(stage.Type), "started")
	}
	prompt := "awaiting human confirmation"
	if stage.Confirm != nil && stage.Confirm.Prompt != "" {
		prompt = "awaiting human confirmation: " + cpkit.Truncate(stage.Confirm.Prompt, 300)
	}
	return d.waitFor(st, prompt, deadline), nil
}

// succeed closes the Work with a satisfied outcome. SUCCEEDED is reachable
// only from VERIFYING, and the store requires the outcome in the same
// mutation.
func (d *driver) succeed(ev *outcome.Evaluation) step {
	if d.w.Status != work.StatusVerifying {
		return step{m: work.Mutation{Status: statusPtr(work.StatusVerifying)}, again: true}
	}
	return step{m: work.Mutation{Status: statusPtr(work.StatusSucceeded), Reason: strPtr("goal satisfied"), Outcome: ev,
		Stage: intPtr(len(d.w.Template.Stages)), ClearState: true, CancelAllWatches: true}}
}

// succeedFrom closes with a copy of an already recorded evaluation.
func (d *driver) succeedFrom(prev *outcome.Evaluation) step {
	ev := *prev
	ev.ID, ev.Stage, ev.EvaluatedAt = cpkit.NewID(), "final", d.o.now()
	ev.Reasons = append(append([]string(nil), prev.Reasons...), "confirmed by evaluation "+prev.ID)
	return d.succeed(&ev)
}

// finish runs after the last stage: success needs a satisfied outcome of
// this generation; without one, a human confirms.
func (d *driver) finish(ctx context.Context) (step, error) {
	ev, err := d.latestOutcome(ctx, func(outcome.Evaluation) bool { return true })
	if err != nil {
		return step{}, err
	}
	if ev != nil && ev.Verdict == outcome.Satisfied {
		return d.succeedFrom(ev), nil
	}
	final := work.Stage{ID: "final", Type: work.StageConfirm, OnFailure: work.OnFailBlock}
	var st stageState
	if len(d.w.StageState) > 0 {
		_ = json.Unmarshal(d.w.StageState, &st)
	}
	if st.StageID != "final" || st.Generation != d.w.Generation {
		st = stageState{StageID: "final", Generation: d.w.Generation, StartedAt: d.o.now()}
	}
	return d.confirmStage(ctx, final, st)
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
