package eval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/tools"
)

type Dataset struct {
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Version      int64      `json:"version"`
	Status       string     `json:"status"`
	ManifestHash *string    `json:"manifest_hash,omitempty"`
	CreatedBy    string     `json:"created_by"`
	CreatedAt    time.Time  `json:"created_at"`
	FrozenAt     *time.Time `json:"frozen_at,omitempty"`
}

type EvalRun struct {
	ID             string          `json:"id"`
	DatasetID      string          `json:"dataset_id"`
	ProfileID      string          `json:"profile_id"`
	ProfileVersion int64           `json:"profile_version"`
	Split          string          `json:"split"`
	Status         string          `json:"status"`
	Metrics        json.RawMessage `json:"metrics,omitempty"`
	ReportRef      *string         `json:"report_ref,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

type Candidate struct {
	ID               string          `json:"id"`
	ProfileID        string          `json:"profile_id"`
	BaseVersion      int64           `json:"base_version"`
	CandidateVersion *int64          `json:"candidate_version,omitempty"`
	ChangeSummary    json.RawMessage `json:"change_summary"`
	BaseEvaluationID *string         `json:"base_evaluation_id,omitempty"`
	EvaluationID     *string         `json:"evaluation_id,omitempty"`
	GateResult       json.RawMessage `json:"gate_result,omitempty"`
	Status           string          `json:"status"`
	ProposedBy       string          `json:"proposed_by"`
	ApprovedBy       *string         `json:"approved_by,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
}

type Guardrails struct {
	MaxFailureRate     float64 `json:"max_failure_rate"`      // canary runs FAILED / finished
	MaxFailureRateDiff float64 `json:"max_failure_rate_diff"` // vs control
	MaxCostPerRunUSD   float64 `json:"max_cost_per_run_usd"`  // 0 = no limit
	MinRuns            int     `json:"min_runs"`              // before guardrails are judged
}

type Rollout struct {
	ID             string          `json:"id"`
	ProfileID      string          `json:"profile_id"`
	FromVersion    int64           `json:"from_version"`
	ToVersion      int64           `json:"to_version"`
	CandidateID    *string         `json:"candidate_id,omitempty"`
	Status         string          `json:"status"`
	TrafficPercent float64         `json:"traffic_percent"`
	Guardrails     Guardrails      `json:"guardrails"`
	Observations   json.RawMessage `json:"observations,omitempty"`
	ApprovedBy     *string         `json:"approved_by,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	EndedAt        *time.Time      `json:"ended_at,omitempty"`
}

type RolloutStats struct {
	Finished   int     `json:"finished"`
	Failed     int     `json:"failed"`
	CostMicros int64   `json:"cost_micros"`
	FailRate   float64 `json:"fail_rate"`
	CostPerRun float64 `json:"cost_per_run_usd"`
}

type Store interface {
	GetRun(ctx context.Context, tenantID, runID string) (domain.Run, error)
	ListSteps(ctx context.Context, tenantID, runID string) ([]domain.Step, error)
	ListFeedback(ctx context.Context, tenantID, runID string) ([]memory.Feedback, error)

	CreateDataset(ctx context.Context, tenantID, name, actor string) (Dataset, error)
	GetDataset(ctx context.Context, tenantID, id string) (Dataset, error)
	AddCase(ctx context.Context, tenantID, datasetID string, c Case) (Case, bool, error)
	FreezeDataset(ctx context.Context, tenantID, datasetID string) (Dataset, error)
	ListCases(ctx context.Context, tenantID, datasetID, split string) ([]Case, error)

	ProfileSpec(ctx context.Context, tenantID, profileID string, version int64) (json.RawMessage, error)
	CreateProfileVersion(ctx context.Context, tenantID, profileID string, spec json.RawMessage, status string) (int64, error)
	SetProfileStatus(ctx context.Context, tenantID, profileID string, version int64, status string) error
	SetActiveVersion(ctx context.Context, tenantID, profileID string, version int64) error
	ActiveVersion(ctx context.Context, tenantID, profileID string) (int64, error)

	StartEvalRun(ctx context.Context, tenantID, datasetID, profileID string, version int64, split, actor string) (string, error)
	FinishEvalRun(ctx context.Context, tenantID, id, status string, metrics any, reportRef string) error
	GetEvalRun(ctx context.Context, tenantID, id string) (EvalRun, error)

	CreateCandidate(ctx context.Context, tenantID string, c Candidate, specRef string) (Candidate, error)
	GetCandidate(ctx context.Context, tenantID, id string) (Candidate, error)
	UpdateCandidate(ctx context.Context, tenantID, id, fromStatus string, set map[string]any) error

	CreateRollout(ctx context.Context, tenantID string, r Rollout) (Rollout, error)
	GetRollout(ctx context.Context, tenantID, id string) (Rollout, error)
	ActiveRollout(ctx context.Context, tenantID, profileID string) (*Rollout, error)
	ListCanaryRollouts(ctx context.Context) ([]struct{ TenantID, ID string }, error)
	EndRollout(ctx context.Context, tenantID, id, status, actor string, observations any) error
	RolloutStats(ctx context.Context, tenantID, rolloutID string, control bool, profileID string, since time.Time) (RolloutStats, error)
}

var ErrInvalid = errors.New("invalid request")

type Service struct {
	Store     Store
	Artifacts artifact.Store
	Registry  *tools.Registry
	Factory   profile.Factory
	Gate      GateConfig
	Log       *slog.Logger
	// Grader scores answers during replay (nil = SubstringGrader).
	Grader Grader
}

// ---------- datasets ----------

// RecordFromRun turns a finished run into an evaluation case: recorded tool
// outputs, the goal, and ground truth from labels or verified feedback.
func (s *Service) RecordFromRun(ctx context.Context, tenantID, datasetID, runID string, labels, mustNotCall []string, split string) (Case, bool, error) {
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
	rec := Recording{Goal: goal.Goal}
	for _, st := range steps {
		if st.Kind != domain.StepTool {
			continue
		}
		var call domain.ToolCall
		if data, err := s.Artifacts.Get(ctx, tenantID, st.InputRef); err == nil {
			_ = json.Unmarshal(data, &call)
		}
		rc := RecordedCall{Tool: call.Name, Args: call.Args, Outcome: "blocked"}
		if st.Status == domain.StepSucceeded && st.OutputRef != nil {
			var o domain.Observation
			if data, err := s.Artifacts.Get(ctx, tenantID, *st.OutputRef); err == nil && json.Unmarshal(data, &o) == nil {
				rc.Outcome, rc.Output = "ok", o.Content
			}
		}
		rec.Calls = append(rec.Calls, rc)
	}
	if len(labels) == 0 {
		fb, _ := s.Store.ListFeedback(ctx, tenantID, runID)
		for _, f := range fb {
			if f.RootCauseCorrect != nil && *f.RootCauseCorrect && f.RootCauseLabel != nil {
				labels = append(labels, *f.RootCauseLabel)
			}
		}
	}
	if len(labels) == 0 {
		return Case{}, false, fmt.Errorf("%w: ground truth needs root_cause_labels or verified feedback with a label", ErrInvalid)
	}
	data, _ := json.Marshal(rec)
	redacted, _ := memory.Redact(string(data))
	ref, _, err := s.Artifacts.Put(ctx, tenantID, "eval-recording", []byte(redacted))
	if err != nil {
		return Case{}, false, err
	}
	var allowed []string
	for _, d := range s.Registry.Descriptors() {
		allowed = append(allowed, d.Name)
	}
	key := "run:" + runID
	if split == "" {
		split = SplitFor(key)
	}
	c := Case{Key: key, Split: split, ServiceID: orUnknown(memory.ServiceFromGoal(goal.Goal)), Environment: "production",
		Goal: goal.Goal, RecordingRef: ref, GroundTruth: GroundTruth{RootCauseLabels: labels, MustNotCall: mustNotCall},
		AllowedTools: allowed, OccurredAt: run.CreatedAt, SourceRunID: &run.ID}
	return s.Store.AddCase(ctx, tenantID, datasetID, c)
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

// ---------- evaluation ----------

// Evaluate replays a frozen dataset split with a profile version.
func (s *Service) Evaluate(ctx context.Context, tenantID, datasetID, profileID string, version int64, split, actor string) (EvalRun, Metrics, error) {
	ds, err := s.Store.GetDataset(ctx, tenantID, datasetID)
	if err != nil {
		return EvalRun{}, Metrics{}, err
	}
	if ds.Status != "FROZEN" {
		return EvalRun{}, Metrics{}, fmt.Errorf("%w: dataset must be frozen before evaluation", ErrInvalid)
	}
	raw, err := s.Store.ProfileSpec(ctx, tenantID, profileID, version)
	if err != nil {
		return EvalRun{}, Metrics{}, err
	}
	spec, err := profile.Parse(raw)
	if err != nil {
		return EvalRun{}, Metrics{}, err
	}
	strat, err := s.Factory(spec)
	if err != nil {
		return EvalRun{}, Metrics{}, err
	}
	id, err := s.Store.StartEvalRun(ctx, tenantID, datasetID, profileID, version, split, actor)
	if err != nil {
		return EvalRun{}, Metrics{}, err
	}
	cases, err := s.Store.ListCases(ctx, tenantID, datasetID, split)
	if err != nil {
		_ = s.Store.FinishEvalRun(ctx, tenantID, id, "FAILED", nil, "")
		return EvalRun{}, Metrics{}, err
	}
	rp := Replayer{Registry: s.Registry, MaxSteps: spec.MaxSteps, Grader: s.Grader}
	var results []CaseResult
	for _, c := range cases {
		data, err := s.Artifacts.Get(ctx, tenantID, c.RecordingRef)
		if err != nil {
			_ = s.Store.FinishEvalRun(ctx, tenantID, id, "FAILED", nil, "")
			return EvalRun{}, Metrics{}, err
		}
		_ = json.Unmarshal(data, &c.Recording)
		results = append(results, rp.Replay(ctx, strat, c))
	}
	m := Aggregate(results)
	report, _ := json.Marshal(map[string]any{"dataset_id": datasetID, "profile": fmt.Sprintf("%s@%d", profileID, version),
		"split": split, "metrics": m, "cases": results})
	ref, _, err := s.Artifacts.Put(ctx, tenantID, "eval-report", report)
	if err != nil {
		_ = s.Store.FinishEvalRun(ctx, tenantID, id, "FAILED", nil, "")
		return EvalRun{}, Metrics{}, err
	}
	if err := s.Store.FinishEvalRun(ctx, tenantID, id, "SUCCEEDED", m, ref); err != nil {
		return EvalRun{}, Metrics{}, err
	}
	run, err := s.Store.GetEvalRun(ctx, tenantID, id)
	return run, m, err
}

// ---------- evolution (§12.4) ----------

// Propose creates a candidate profile version from a patch of evolvable fields.
func (s *Service) Propose(ctx context.Context, tenantID, profileID string, baseVersion int64, patch json.RawMessage,
	rationale, actor string) (Candidate, error) {
	raw, err := s.Store.ProfileSpec(ctx, tenantID, profileID, baseVersion)
	if err != nil {
		return Candidate{}, err
	}
	base, err := profile.Parse(raw)
	if err != nil {
		return Candidate{}, err
	}
	next, changed, err := profile.ApplyPatch(base, patch)
	if err != nil {
		return Candidate{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if len(changed) == 0 {
		return Candidate{}, fmt.Errorf("%w: empty patch", ErrInvalid)
	}
	spec, _ := json.Marshal(next)
	v, err := s.Store.CreateProfileVersion(ctx, tenantID, profileID, spec, "CANDIDATE")
	if err != nil {
		return Candidate{}, err
	}
	ref, _, err := s.Artifacts.Put(ctx, tenantID, "profile-spec", spec)
	if err != nil {
		return Candidate{}, err
	}
	summary, _ := json.Marshal(map[string]any{"changed_fields": changed, "patch": patch, "rationale": rationale})
	return s.Store.CreateCandidate(ctx, tenantID, Candidate{ProfileID: profileID, BaseVersion: baseVersion,
		CandidateVersion: &v, ChangeSummary: summary, Status: "PROPOSED", ProposedBy: actor}, ref)
}

// EvaluateCandidate replays the holdout split for base and candidate and
// applies the release gate.
func (s *Service) EvaluateCandidate(ctx context.Context, tenantID, candidateID, datasetID, actor string) (Candidate, GateResult, error) {
	c, err := s.Store.GetCandidate(ctx, tenantID, candidateID)
	if err != nil {
		return c, GateResult{}, err
	}
	if c.Status != "PROPOSED" && c.Status != "GATE_FAILED" {
		return c, GateResult{}, fmt.Errorf("%w: candidate is %s", ErrInvalid, c.Status)
	}
	if err := s.Store.UpdateCandidate(ctx, tenantID, c.ID, c.Status, map[string]any{"status": "EVALUATING"}); err != nil {
		return c, GateResult{}, err
	}
	baseRun, baseM, err := s.Evaluate(ctx, tenantID, datasetID, c.ProfileID, c.BaseVersion, "holdout", actor)
	if err != nil {
		_ = s.Store.UpdateCandidate(ctx, tenantID, c.ID, "EVALUATING", map[string]any{"status": "PROPOSED"})
		return c, GateResult{}, err
	}
	candRun, candM, err := s.Evaluate(ctx, tenantID, datasetID, c.ProfileID, *c.CandidateVersion, "holdout", actor)
	if err != nil {
		_ = s.Store.UpdateCandidate(ctx, tenantID, c.ID, "EVALUATING", map[string]any{"status": "PROPOSED"})
		return c, GateResult{}, err
	}
	cfg := s.Gate
	if cfg == (GateConfig{}) {
		cfg = DefaultGate()
	}
	g := Gate(baseM, candM, cfg)
	status := "PENDING_APPROVAL"
	if !g.Pass {
		status = "GATE_FAILED"
	}
	gj, _ := json.Marshal(g)
	if err := s.Store.UpdateCandidate(ctx, tenantID, c.ID, "EVALUATING", map[string]any{"status": status,
		"base_evaluation_id": baseRun.ID, "evaluation_id": candRun.ID, "gate_result": gj}); err != nil {
		return c, g, err
	}
	c, err = s.Store.GetCandidate(ctx, tenantID, c.ID)
	return c, g, err
}

// Approve records a human approval; the proposer cannot approve their own candidate.
func (s *Service) Approve(ctx context.Context, tenantID, candidateID, actor string) (Candidate, error) {
	c, err := s.Store.GetCandidate(ctx, tenantID, candidateID)
	if err != nil {
		return c, err
	}
	if c.Status != "PENDING_APPROVAL" {
		return c, fmt.Errorf("%w: candidate is %s (gate must pass first)", ErrInvalid, c.Status)
	}
	if c.ProposedBy == actor {
		return c, domain.ErrApproverConflict
	}
	if err := s.Store.UpdateCandidate(ctx, tenantID, c.ID, "PENDING_APPROVAL", map[string]any{"status": "APPROVED",
		"approved_by": actor}); err != nil {
		return c, err
	}
	return s.Store.GetCandidate(ctx, tenantID, c.ID)
}

func (s *Service) StartCanary(ctx context.Context, tenantID, candidateID string, percent float64, g Guardrails, actor string) (Rollout, error) {
	c, err := s.Store.GetCandidate(ctx, tenantID, candidateID)
	if err != nil {
		return Rollout{}, err
	}
	if c.Status != "APPROVED" {
		return Rollout{}, fmt.Errorf("%w: candidate must be APPROVED", ErrInvalid)
	}
	if percent <= 0 || percent > 50 {
		return Rollout{}, fmt.Errorf("%w: canary traffic must be in (0, 50]", ErrInvalid)
	}
	if g.MinRuns == 0 {
		g.MinRuns = 5
	}
	if g.MaxFailureRate == 0 {
		g.MaxFailureRate = 0.3
	}
	if g.MaxFailureRateDiff == 0 {
		g.MaxFailureRateDiff = 0.15
	}
	from, err := s.Store.ActiveVersion(ctx, tenantID, c.ProfileID)
	if err != nil {
		from = c.BaseVersion
	}
	r, err := s.Store.CreateRollout(ctx, tenantID, Rollout{ProfileID: c.ProfileID, FromVersion: from,
		ToVersion: *c.CandidateVersion, CandidateID: &c.ID, Status: "CANARY", TrafficPercent: percent, Guardrails: g,
		ApprovedBy: &actor})
	if err != nil {
		return r, err
	}
	_ = s.Store.UpdateCandidate(ctx, tenantID, c.ID, "APPROVED", map[string]any{"status": "ROLLED_OUT"})
	return r, nil
}

func (s *Service) Promote(ctx context.Context, tenantID, rolloutID, actor string) (Rollout, error) {
	r, err := s.Store.GetRollout(ctx, tenantID, rolloutID)
	if err != nil {
		return r, err
	}
	if r.Status != "CANARY" {
		return r, fmt.Errorf("%w: rollout is %s", ErrInvalid, r.Status)
	}
	if err := s.Store.SetActiveVersion(ctx, tenantID, r.ProfileID, r.ToVersion); err != nil {
		return r, err
	}
	_ = s.Store.SetProfileStatus(ctx, tenantID, r.ProfileID, r.ToVersion, "ACTIVE")
	if err := s.Store.EndRollout(ctx, tenantID, r.ID, "PROMOTED", actor, nil); err != nil {
		return r, err
	}
	return s.Store.GetRollout(ctx, tenantID, r.ID)
}

func (s *Service) Rollback(ctx context.Context, tenantID, rolloutID, actor, reason string) (Rollout, error) {
	r, err := s.Store.GetRollout(ctx, tenantID, rolloutID)
	if err != nil {
		return r, err
	}
	if r.Status != "CANARY" && r.Status != "PROMOTED" {
		return r, fmt.Errorf("%w: rollout is %s", ErrInvalid, r.Status)
	}
	if r.Status == "PROMOTED" {
		if err := s.Store.SetActiveVersion(ctx, tenantID, r.ProfileID, r.FromVersion); err != nil {
			return r, err
		}
	}
	_ = s.Store.SetProfileStatus(ctx, tenantID, r.ProfileID, r.ToVersion, "QUARANTINED")
	if err := s.Store.EndRollout(ctx, tenantID, r.ID, "ROLLED_BACK", actor, map[string]string{"reason": reason}); err != nil {
		return r, err
	}
	return s.Store.GetRollout(ctx, tenantID, r.ID)
}

// ResolveProfile picks the version for a new run: the canary version for a
// stable hash bucket of the routing key, otherwise the active channel version.
func (s *Service) ResolveProfile(ctx context.Context, tenantID, profileID, routingKey string) (int64, *string) {
	if r, err := s.Store.ActiveRollout(ctx, tenantID, profileID); err == nil && r != nil && r.Status == "CANARY" {
		h := fnv.New32a()
		_, _ = h.Write([]byte(r.ID + "|" + routingKey))
		if float64(h.Sum32()%10000) < r.TrafficPercent*100 {
			id := r.ID
			return r.ToVersion, &id
		}
	}
	if v, err := s.Store.ActiveVersion(ctx, tenantID, profileID); err == nil && v > 0 {
		return v, nil
	}
	return 0, nil
}

// MonitorTick judges every canary against its guardrails and rolls back
// automatically on a breach (§32.3). Promotion stays a human decision.
func (s *Service) MonitorTick(ctx context.Context) (int, error) {
	list, err := s.Store.ListCanaryRollouts(ctx)
	if err != nil {
		return 0, err
	}
	rolled := 0
	for _, item := range list {
		r, err := s.Store.GetRollout(ctx, item.TenantID, item.ID)
		if err != nil || r.Status != "CANARY" || r.StartedAt == nil {
			continue
		}
		canary, err := s.Store.RolloutStats(ctx, item.TenantID, r.ID, false, r.ProfileID, *r.StartedAt)
		if err != nil {
			return rolled, err
		}
		control, err := s.Store.RolloutStats(ctx, item.TenantID, r.ID, true, r.ProfileID, *r.StartedAt)
		if err != nil {
			return rolled, err
		}
		if canary.Finished < r.Guardrails.MinRuns {
			continue
		}
		var breach []string
		if canary.FailRate > r.Guardrails.MaxFailureRate {
			breach = append(breach, fmt.Sprintf("failure rate %.2f > %.2f", canary.FailRate, r.Guardrails.MaxFailureRate))
		}
		if control.Finished >= r.Guardrails.MinRuns && canary.FailRate-control.FailRate > r.Guardrails.MaxFailureRateDiff {
			breach = append(breach, fmt.Sprintf("failure rate %.2f vs control %.2f", canary.FailRate, control.FailRate))
		}
		if r.Guardrails.MaxCostPerRunUSD > 0 && canary.CostPerRun > r.Guardrails.MaxCostPerRunUSD {
			breach = append(breach, fmt.Sprintf("cost/run $%.4f > $%.4f", canary.CostPerRun, r.Guardrails.MaxCostPerRunUSD))
		}
		if len(breach) == 0 {
			continue
		}
		_ = s.Store.SetProfileStatus(ctx, item.TenantID, r.ProfileID, r.ToVersion, "QUARANTINED")
		if err := s.Store.EndRollout(ctx, item.TenantID, r.ID, "ROLLED_BACK", "guardrail-monitor",
			map[string]any{"breaches": breach, "canary": canary, "control": control}); err != nil {
			return rolled, err
		}
		if s.Log != nil {
			s.Log.Warn("canary rolled back", "rollout", r.ID, "breaches", breach)
		}
		rolled++
	}
	return rolled, nil
}

// Run executes MonitorTick periodically.
func (s *Service) Run(ctx context.Context, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := s.MonitorTick(ctx); err != nil && s.Log != nil && ctx.Err() == nil {
			s.Log.Warn("rollout monitor", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
