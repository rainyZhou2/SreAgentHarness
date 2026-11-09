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
	// Batch is how many Works one tick claims (default 16); Concurrency how
	// many are processed in parallel (default 4).
	Batch       int
	Concurrency int
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
func (o *Orchestrator) modelGoal(tpl string, w *work.Work, summaries map[string]string) (string, error) {
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
	})
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
	if o.Batch <= 0 {
		o.Batch = 16
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 4
	}
	if o.MaxStepsPerClaim <= 0 {
		o.MaxStepsPerClaim = 8
	}
}

// Run claims and processes due Works until ctx ends.
func (o *Orchestrator) Run(ctx context.Context) error {
	o.defaults()
	t := time.NewTicker(o.Poll)
	defer t.Stop()
	for {
		if _, err := o.Tick(ctx); err != nil && ctx.Err() == nil {
			o.log().Warn("work orchestrator tick failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Tick claims due Works once and processes them; it returns how many were
// claimed.
func (o *Orchestrator) Tick(ctx context.Context) (int, error) {
	o.defaults()
	works, err := o.Store.ClaimDueWorks(ctx, o.Owner, o.now(), o.Lease, o.Batch)
	if err != nil {
		return 0, err
	}
	sem := make(chan struct{}, o.Concurrency)
	var wg sync.WaitGroup
	for _, w := range works {
		wg.Add(1)
		sem <- struct{}{}
		go func(w work.Work) {
			defer wg.Done()
			defer func() { <-sem }()
			o.process(ctx, w)
		}(w)
	}
	wg.Wait()
	return len(works), nil
}

func (o *Orchestrator) process(ctx context.Context, w work.Work) {
	if w.WakeAt != nil {
		o.Metrics.woke(o.now().Sub(*w.WakeAt))
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
	switch {
	case err == nil:
		o.Metrics.tick("ok")
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
}

type stageRun struct {
	RunID    string                   `json:"run_id"`
	Role     string                   `json:"role"`
	Required bool                     `json:"required"`
	Status   string                   `json:"status,omitempty"`
	Done     bool                     `json:"done,omitempty"`
	Result   *evidence.ResultEnvelope `json:"result,omitempty"`
	Snapshot string                   `json:"world_snapshot,omitempty"`
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
func timePtr(t time.Time) *time.Time       { return &t }

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
		summaries, err := d.summaries(ctx)
		if err != nil {
			return step{}, err
		}
		var links []work.RunLink
		snapID := d.o.pinSnapshot(ctx, w)
		for _, a := range specs {
			goal, err := d.o.modelGoal(a.Goal, w, summaries)
			if err != nil {
				return d.fail(stage, work.OnFailBlock, err.Error()), nil
			}
			key := fmt.Sprintf("work:%s:g%d:%s:%s", w.ID, w.Generation, stage.ID, a.Role)
			labels := map[string]string{"work_id": w.ID, "work_stage": stage.ID}
			if snapID != "" {
				labels[SnapshotLabel] = snapID
			}
			runID, err := d.o.Runs.StartRun(ctx, RunRequest{TenantID: w.TenantID, ActorID: "work:" + w.ID, Goal: goal,
				ProfileID: a.ProfileID, ProfileVersion: a.ProfileVersion, Mode: a.Mode, Services: w.Goal.SubjectRefs,
				Labels: labels, Source: "work:" + string(w.Kind), IdempotencyKey: key})
			if err != nil {
				var rej *domain.RejectedError
				if errors.As(err, &rej) {
					return d.fail(stage, stage.OnFailure, "run rejected: "+rej.Reason), nil
				}
				return step{}, err
			}
			st.Runs = append(st.Runs, stageRun{RunID: runID, Role: a.Role, Required: a.Required, Snapshot: snapID})
			links = append(links, work.RunLink{RunID: runID, Role: work.RoleOwner, Phase: stage.Phase, StageID: stage.ID,
				AgentRole: a.Role, Generation: w.Generation, Required: a.Required})
		}
		d.o.Metrics.stage(string(stage.Type), "started")
		m := work.Mutation{StageState: mustJSON(st), Links: links, AddRuns: len(links),
			Events: []work.NewEvent{{Type: "work.runs_started", Payload: map[string]any{"stage_id": stage.ID,
				"runs": st.Runs}}}}
		if w.Status != work.StatusActive {
			m.Status = statusPtr(work.StatusActive)
		}
		wake := now.Add(d.o.RunPoll)
		return step{m: m, wake: &wake}, nil
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
			if err != nil {
				return step{}, err
			}
			res.EvidenceRefs = append(res.EvidenceRefs, "evidence:"+rec.ID)
		}
		linkResults[r.RunID] = res
		d.o.Metrics.assignment(string(stage.Type), res.Outcome)
	}
	if !allDone {
		m := work.Mutation{LinkStatus: linkStatus, LinkResults: linkResults, AddCost: cost}
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
	if err := d.recordHypotheses(ctx, rep); err != nil {
		return step{}, err
	}
	payload := map[string]any{"summary": summarize(rep), "verdict": rep.Verdict, "conflicts": rep.Conflicts,
		"gaps": rep.Gaps, "cost_usd": rep.CostUSD, "proposed": proposed}
	if max := w.Goal.Constraints.MaxCostUSD; max > 0 && float64(w.CostMicros+cost)/1e6 > max {
		s := d.block(fmt.Sprintf("cost budget exceeded (%.2f USD > %.2f USD)", float64(w.CostMicros+cost)/1e6, max))
		s.m.LinkStatus, s.m.LinkResults, s.m.AddCost = linkStatus, linkResults, cost
		return s, nil
	}
	if rep.Verdict == "FAILED" {
		reason := "runs failed"
		if len(rep.RequiredFailed) > 0 {
			reason = "required agents failed: " + strings.Join(rep.RequiredFailed, ", ")
		}
		s := d.fail(stage, stage.OnFailure, reason)
		s.m.LinkStatus, s.m.LinkResults, s.m.AddCost = linkStatus, linkResults, cost
		return s, nil
	}
	s := d.advance(stage, payload)
	s.m.LinkStatus, s.m.LinkResults, s.m.AddCost = linkStatus, linkResults, cost
	return s, nil
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

func (d *driver) recordHypotheses(ctx context.Context, rep evidence.MergeReport) error {
	if len(rep.Hypotheses) == 0 {
		return nil
	}
	existing, err := d.o.Store.ListHypotheses(ctx, d.w.TenantID, d.w.ID)
	if err != nil {
		return err
	}
	byKey := map[string]evidence.Hypothesis{}
	for _, h := range existing {
		byKey[h.Key] = h
	}
	for i, m := range rep.Hypotheses {
		if i >= evidence.MaxHypotheses {
			break
		}
		h, ok := byKey[m.Key]
		if !ok {
			h = evidence.Hypothesis{ID: cpkit.NewID(), TenantID: d.w.TenantID, WorkID: d.w.ID, Key: m.Key,
				Statement: cpkit.Truncate(m.Statement, evidence.MaxStatement), Category: m.Category}
		}
		for _, e := range m.Supporting {
			h.Supporting = appendUnique(h.Supporting, e)
		}
		for _, e := range m.Refuting {
			h.Refuting = appendUnique(h.Refuting, e)
		}
		for _, p := range m.Proponents {
			h.Proponents = appendUnique(h.Proponents, p)
		}
		h.UpdatedAt = d.o.now()
		if _, err := d.o.Store.UpsertHypothesis(ctx, h, h.Version); err != nil {
			return err
		}
	}
	return nil
}

func appendUnique(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
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
		res, err := rt.Execute(ctx, w.TenantID, rec.ID)
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
			if _, err := rt.Reconcile(ctx, w.TenantID, rec.ID); err != nil {
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
