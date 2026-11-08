// Package improve is the bounded self-improvement loop. It reads measured
// reasoning quality, diagnoses weak dimensions and underperforming
// reasoning patterns, and proposes a profile candidate that changes only the
// reasoning layer (playbook, excluded patterns). The candidate then goes
// through the existing evolution pipeline: offline gate on the holdout
// split, approval by a human other than the proposer, canary with automatic
// rollback. This package never approves, starts a canary or promotes.
package improve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/reasoning"
)

// Actor is the proposer identity of every candidate this loop creates. The
// evolution service refuses approvals by the proposer, so a human must
// approve.
const Actor = "system:reasoning-improver"

// Config bounds one loop.
type Config struct {
	Tenants   []string      `json:"tenants"`
	ProfileID string        `json:"profile_id"`
	DatasetID string        `json:"dataset_id,omitempty"` // holdout gate; empty = propose without evaluating
	Window    time.Duration `json:"-"`
	MinRuns   int           `json:"min_runs,omitempty"`
	MinUses   int           `json:"min_uses,omitempty"`
	// RetireZ and MinLift retire an ACTIVE pattern whose exposed runs
	// succeed significantly (z <= -RetireZ) and materially (lift <=
	// -MinLift) less often than their services' baseline (defaults 1.96
	// and 0.10).
	RetireZ float64 `json:"retire_z,omitempty"`
	MinLift float64 `json:"min_lift,omitempty"`
	// ReinstateZ reinstates an excluded pattern whose probed runs reach this
	// z against the baseline (default 0: at least as good as baseline).
	// Retire and reinstate thresholds differ, so a pattern does not flap.
	ReinstateZ float64 `json:"reinstate_z,omitempty"`
	// ProbeRate is proposed as reasoning.pattern_probe_rate when patterns
	// are excluded and the profile has none (default 0.05).
	ProbeRate    float64    `json:"probe_rate,omitempty"`
	DisableProbe bool       `json:"disable_probe,omitempty"`
	Thresholds   Thresholds `json:"thresholds,omitempty"`
}

// Thresholds mark a dimension as weak.
type Thresholds struct {
	MaxBrier          float64 `json:"max_brier,omitempty"`
	MinGrounding      float64 `json:"min_grounding,omitempty"`
	MaxRedundancy     float64 `json:"max_redundancy,omitempty"`
	MaxUnverifiedRate float64 `json:"max_unverified_rate,omitempty"`
	MaxNoVerdictRate  float64 `json:"max_no_verdict_rate,omitempty"`
	// MinStructured and MinCoverage flag traces that are not structured
	// enough to measure (structured thought share, confidence coverage of
	// settled hypotheses).
	MinStructured float64 `json:"min_structured,omitempty"`
	MinCoverage   float64 `json:"min_coverage,omitempty"`
	// MinCitationValidity flags citations that name nothing the model was
	// shown; MaxUnsupportedRate verdicts or conclusions without one.
	MinCitationValidity float64 `json:"min_citation_validity,omitempty"`
	MaxUnsupportedRate  float64 `json:"max_unsupported_rate,omitempty"`
}

// Defaults fills unset fields.
func (c Config) Defaults() Config {
	if c.Window <= 0 {
		c.Window = 7 * 24 * time.Hour
	}
	if c.MinRuns <= 0 {
		c.MinRuns = 20
	}
	if c.MinUses <= 0 {
		c.MinUses = 5
	}
	if c.RetireZ <= 0 {
		c.RetireZ = 1.96
	}
	if c.MinLift <= 0 {
		c.MinLift = 0.10
	}
	if c.ProbeRate <= 0 {
		c.ProbeRate = 0.05
	}
	t := &c.Thresholds
	if t.MaxBrier <= 0 {
		t.MaxBrier = 0.25
	}
	if t.MinGrounding <= 0 {
		t.MinGrounding = 0.6
	}
	if t.MaxRedundancy <= 0 {
		t.MaxRedundancy = 0.15
	}
	if t.MaxUnverifiedRate <= 0 {
		t.MaxUnverifiedRate = 0.4
	}
	if t.MaxNoVerdictRate <= 0 {
		t.MaxNoVerdictRate = 0.5
	}
	if t.MinStructured <= 0 {
		t.MinStructured = 0.8
	}
	if t.MinCoverage <= 0 {
		t.MinCoverage = 0.6
	}
	if t.MinCitationValidity <= 0 {
		t.MinCitationValidity = 0.9
	}
	if t.MaxUnsupportedRate <= 0 {
		t.MaxUnsupportedRate = 0.3
	}
	return c
}

// Validate checks the configuration.
func (c Config) Validate() error {
	if c.ProfileID == "" {
		return errors.New("reasoning improver: profile_id is required")
	}
	if len(c.Tenants) == 0 {
		return errors.New("reasoning improver: tenants are required")
	}
	if c.RetireZ < 0 || c.MinLift < 0 || c.MinLift >= 1 {
		return errors.New("reasoning improver: retire_z must be >= 0 and min_lift in [0,1)")
	}
	if c.ReinstateZ <= -c.Defaults().RetireZ {
		return errors.New("reasoning improver: reinstate_z must be above -retire_z, or patterns would flap")
	}
	if c.ProbeRate < 0 || c.ProbeRate > profile.MaxPatternProbeRate {
		return fmt.Errorf("reasoning improver: probe_rate must be in [0, %.1f]", profile.MaxPatternProbeRate)
	}
	return nil
}

// Store is what the loop reads.
type Store interface {
	ReasoningScores(ctx context.Context, tenantID string, f reasoning.ScoreFilter) ([]reasoning.RunScore, error)
	GetMemory(ctx context.Context, tenantID, id string) (memory.Memory, error)
	ListCandidates(ctx context.Context, tenantID, profileID string) ([]eval.Candidate, error)
	ActiveVersion(ctx context.Context, tenantID, profileID string) (int64, error)
	ProfileSpec(ctx context.Context, tenantID, profileID string, version int64) (json.RawMessage, error)
	ListCases(ctx context.Context, tenantID, datasetID, split string) ([]eval.Case, error)
}

// Evolution is the gated pipeline (eval.Service).
type Evolution interface {
	Propose(ctx context.Context, tenantID, profileID string, baseVersion int64, patch json.RawMessage, rationale, actor string) (eval.Candidate, error)
	EvaluateCandidate(ctx context.Context, tenantID, candidateID, datasetID, actor string) (eval.Candidate, eval.GateResult, error)
}

// Diagnosis is what the loop found.
type Diagnosis struct {
	Summary      reasoning.Summary `json:"summary"`
	Weak         []string          `json:"weak_dimensions,omitempty"`
	Patterns     ExclusionPlan     `json:"patterns"`
	FailureCases []string          `json:"failure_digests,omitempty"`
	Excluded     int               `json:"excluded_holdout_runs"`
	// PlaybookSource is template or induced; Induced the accepted induced
	// lines; InducedRejected the rejected ones with reasons.
	PlaybookSource  string   `json:"playbook_source,omitempty"`
	Induced         []string `json:"induced,omitempty"`
	InducedRejected []string `json:"induced_rejected,omitempty"`
	InduceError     string   `json:"induce_error,omitempty"`
}

// Outcome reports one tick for one tenant.
type Outcome struct {
	TenantID    string           `json:"tenant_id"`
	Status      string           `json:"status"` // skipped | proposed | evaluated | error
	Reason      string           `json:"reason,omitempty"`
	BaseVersion int64            `json:"base_version,omitempty"`
	Diagnosis   *Diagnosis       `json:"diagnosis,omitempty"`
	Candidate   *eval.Candidate  `json:"candidate,omitempty"`
	Gate        *eval.GateResult `json:"gate,omitempty"`
}

// Improver runs the loop.
type Improver struct {
	Config    Config
	Store     Store
	Evolution Evolution
	// Inducer, when set, writes playbook guidance from failure cases; the
	// template guidance is the fallback.
	Inducer Inducer
	Log     *slog.Logger
	Now     func() time.Time
}

// Run ticks every interval until ctx is cancelled.
func (im *Improver) Run(ctx context.Context, every time.Duration) error {
	if every <= 0 {
		every = 6 * time.Hour
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			for _, o := range im.Tick(ctx) {
				im.log().Info("reasoning improver", "tenant", o.TenantID, "status", o.Status, "reason", o.Reason)
			}
		}
	}
}

// Tick runs once for every configured tenant.
func (im *Improver) Tick(ctx context.Context) []Outcome {
	var out []Outcome
	for _, tenant := range im.Config.Tenants {
		out = append(out, im.TickTenant(ctx, tenant))
	}
	return out
}

// open candidate states: the loop proposes one change at a time.
var inFlight = map[string]bool{"PROPOSED": true, "EVALUATING": true, "PENDING_APPROVAL": true, "APPROVED": true}

// TickTenant runs the loop for one tenant.
func (im *Improver) TickTenant(ctx context.Context, tenant string) Outcome {
	cfg := im.Config.Defaults()
	o := Outcome{TenantID: tenant}
	skip := func(reason string) Outcome { o.Status, o.Reason = "skipped", reason; return o }
	fail := func(err error) Outcome { o.Status, o.Reason = "error", err.Error(); return o }

	cands, err := im.Store.ListCandidates(ctx, tenant, cfg.ProfileID)
	if err != nil {
		return fail(err)
	}
	for _, c := range cands {
		if c.ProposedBy == Actor && inFlight[c.Status] {
			return skip("candidate " + c.ID + " is " + c.Status)
		}
	}
	base, err := im.Store.ActiveVersion(ctx, tenant, cfg.ProfileID)
	if err != nil || base <= 0 {
		return skip("profile has no active version")
	}
	o.BaseVersion = base
	raw, err := im.Store.ProfileSpec(ctx, tenant, cfg.ProfileID, base)
	if err != nil {
		return fail(err)
	}
	spec, err := profile.Parse(raw)
	if err != nil {
		return fail(err)
	}
	scores, err := im.Store.ReasoningScores(ctx, tenant, reasoning.ScoreFilter{ProfileID: cfg.ProfileID, Version: base,
		Since: im.now().Add(-cfg.Window), Limit: 5000})
	if err != nil {
		return fail(err)
	}
	holdout, err := im.holdoutRuns(ctx, tenant, cfg.DatasetID)
	if err != nil {
		return fail(err)
	}
	d := Diagnosis{}
	kept := scores[:0:0]
	for _, s := range scores {
		if holdout[s.RunID] {
			d.Excluded++
			continue
		}
		kept = append(kept, s)
	}
	if len(kept) < cfg.MinRuns {
		return skip(fmt.Sprintf("%d scored runs of version %d in the window (need %d)", len(kept), base, cfg.MinRuns))
	}
	d.Summary = reasoning.Summarize(reasoning.MetricsOf(kept))
	d.Weak = weakDimensions(d.Summary, cfg.Thresholds)
	current := profile.ReasoningSpec{}
	if spec.Reasoning != nil {
		current = *spec.Reasoning
	}
	d.Patterns, err = PlanExclusions(ctx, im.Store, tenant, current.PatternsExcluded, reasoning.PatternStats(kept), cfg)
	if err != nil {
		return fail(err)
	}
	d.FailureCases = failureDigests(kept, 8)
	o.Diagnosis = &d
	if len(d.Weak) == 0 && !d.Patterns.Changes() {
		return skip("no weak dimension and no pattern to retire or reinstate")
	}

	next := current
	next.PatternsExcluded = d.Patterns.Next
	if len(next.PatternsExcluded) > 0 && next.PatternProbeRate == 0 && !cfg.DisableProbe {
		next.PatternProbeRate = cfg.ProbeRate
	}
	if len(d.Weak) > 0 {
		d.PlaybookSource = "template"
		if im.Inducer != nil {
			cases := failureCases(kept, 8)
			lines, err := im.Inducer.Induce(ctx, tenant, d.Weak, cases, next.Playbook)
			if err != nil {
				d.InduceError = err.Error()
			}
			accepted, rejected := InduceLines(lines, next.Playbook, cases)
			d.InducedRejected = rejected
			if len(accepted) > 0 {
				next.Playbook = MergePlaybook(next.Playbook, accepted, profile.MaxPlaybookRunes)
				d.PlaybookSource, d.Induced = "induced", accepted
			}
		}
		if d.PlaybookSource == "template" {
			next.Playbook = StatsPlaybook(d.Weak, next.Playbook)
		}
	}
	if err := next.Validate(); err != nil {
		return fail(fmt.Errorf("proposed reasoning layer is invalid: %w", err))
	}
	if spec.Reasoning != nil && equalReasoning(*spec.Reasoning, next) {
		return skip("proposal equals the current reasoning layer")
	}
	patch, _ := json.Marshal(map[string]any{"reasoning": next})
	rationale, _ := json.Marshal(map[string]any{"weak_dimensions": d.Weak, "summary": d.Summary,
		"patterns": d.Patterns, "excluded_holdout_runs": d.Excluded, "playbook_source": d.PlaybookSource,
		"induced": d.Induced, "induced_rejected": d.InducedRejected})
	c, err := im.Evolution.Propose(ctx, tenant, cfg.ProfileID, base, patch, string(rationale), Actor)
	if err != nil {
		return fail(err)
	}
	o.Status, o.Candidate = "proposed", &c
	if cfg.DatasetID == "" {
		o.Reason = "no dataset configured: evaluate the candidate before approval"
		return o
	}
	c2, g, err := im.Evolution.EvaluateCandidate(ctx, tenant, c.ID, cfg.DatasetID, Actor)
	if err != nil {
		o.Reason = "evaluation failed: " + err.Error()
		return o
	}
	o.Status, o.Candidate, o.Gate = "evaluated", &c2, &g
	if g.Pass {
		o.Reason = "gate passed: waiting for human approval"
	} else {
		o.Reason = "gate failed: " + strings.Join(g.Failures, "; ")
	}
	return o
}

func (im *Improver) holdoutRuns(ctx context.Context, tenant, datasetID string) (map[string]bool, error) {
	out := map[string]bool{}
	if datasetID == "" {
		return out, nil
	}
	cases, err := im.Store.ListCases(ctx, tenant, datasetID, "holdout")
	if err != nil {
		return nil, err
	}
	for _, c := range cases {
		if c.SourceRunID != nil {
			out[*c.SourceRunID] = true
		}
	}
	return out, nil
}

func (im *Improver) now() time.Time {
	if im.Now != nil {
		return im.Now()
	}
	return time.Now()
}

func (im *Improver) log() *slog.Logger {
	if im.Log != nil {
		return im.Log
	}
	return slog.Default()
}

// Dimensions the loop can diagnose.
const (
	DimCalibration    = "calibration"
	DimGrounding      = "grounding"
	DimRedundancy     = "redundancy"
	DimVerification   = "verification"
	DimDiscrimination = "discrimination"
	DimStructure      = "structure"
	DimCitation       = "citation"
)

func weakDimensions(s reasoning.Summary, t Thresholds) []string {
	var out []string
	if s.MeanBrier != nil && *s.MeanBrier > t.MaxBrier {
		out = append(out, DimCalibration)
	}
	if s.Grounding < t.MinGrounding {
		out = append(out, DimGrounding)
	}
	if s.Redundancy > t.MaxRedundancy {
		out = append(out, DimRedundancy)
	}
	if s.UnverifiedRate > t.MaxUnverifiedRate {
		out = append(out, DimVerification)
	}
	if s.NoVerdictRate > t.MaxNoVerdictRate {
		out = append(out, DimDiscrimination)
	}
	if s.Structured < t.MinStructured || (s.Settled > 0 && s.CalibrationCoverage < t.MinCoverage) {
		out = append(out, DimStructure)
	}
	if (s.Citations > 0 && s.CitationValidity < t.MinCitationValidity) ||
		s.UnsupportedVerdictRate > t.MaxUnsupportedRate || s.UnsupportedConclusionRate > t.MaxUnsupportedRate {
		out = append(out, DimCitation)
	}
	return out
}

// guidance is domain-free advice per dimension.
var guidance = map[string]string{
	DimCalibration:    "- Calibrate confidence: state a confidence only as high as the evidence you have observed supports; lower it when a check could still refute the hypothesis.",
	DimGrounding:      "- Ground every hypothesis and decision: cite the evidence or observation that supports or refutes it (evidence_refs, supports, refutes).",
	DimRedundancy:     "- Do not repeat a tool call with the same arguments; reuse the earlier observation, or query an offloaded result instead of re-running the tool.",
	DimVerification:   "- Before concluding, run one check that could prove the conclusion wrong, and report VERIFIED only when that check passed.",
	DimDiscrimination: "- Prefer the check that best separates your top hypotheses, and mark each hypothesis VERIFIED or REFUTED as soon as evidence settles it.",
	DimCitation:       "- Cite only refs printed in observation, evidence and memory headers (obs:step<N>, kv:<key>@<v>, memory:<id>@<v>), and give every VERIFIED or REFUTED hypothesis and every verification at least one such ref.",
	DimStructure:      "- Fill the structured fields: give every hypothesis a short stable id reused across steps, a status and a confidence_estimate between 0 and 1, and state the verification verdict when concluding.",
}

// StatsPlaybook merges guidance for the weak dimensions into the current
// playbook, keeping existing lines and avoiding duplicates.
func StatsPlaybook(weak []string, current string) string {
	lines := []string{}
	seen := map[string]bool{}
	for _, l := range strings.Split(strings.TrimSpace(current), "\n") {
		if l = strings.TrimSpace(l); l != "" && !seen[l] {
			seen[l] = true
			lines = append(lines, l)
		}
	}
	for _, w := range weak {
		if g, ok := guidance[w]; ok && !seen[g] {
			seen[g] = true
			lines = append(lines, g)
		}
	}
	out := strings.Join(lines, "\n")
	for len([]rune(out)) > profile.MaxPlaybookRunes && len(lines) > 1 {
		lines = lines[1:] // drop the oldest guidance first
		out = strings.Join(lines, "\n")
	}
	return out
}

// failureDigests describes the weakest runs without their content: only
// measured dimensions, so no run text reaches the proposer.
func failureDigests(scores []reasoning.RunScore, n int) []string {
	sorted := append([]reasoning.RunScore(nil), scores...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Metrics.Quality != sorted[j].Metrics.Quality {
			return sorted[i].Metrics.Quality < sorted[j].Metrics.Quality
		}
		return sorted[i].RunID < sorted[j].RunID
	})
	var out []string
	for _, s := range sorted {
		if len(out) >= n || s.Metrics.Success() {
			continue
		}
		m := s.Metrics
		brier := "n/a"
		if m.Brier != nil {
			brier = fmt.Sprintf("%.2f", *m.Brier)
		}
		out = append(out, fmt.Sprintf("service=%s outcome=%s quality=%.2f conclusion=%s hypotheses=%d verified=%d refuted=%d brier=%s grounding=%.2f redundant_calls=%d steps_to_verdict=%d",
			orDash(s.Service), s.Outcome, m.Quality, orDash(m.Conclusion), m.Hypotheses, m.Verified, m.Refuted, brier,
			m.Grounding, m.RedundantCalls, m.StepsToVerdict))
	}
	return out
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func equalReasoning(a, b profile.ReasoningSpec) bool {
	if strings.TrimSpace(a.Playbook) != strings.TrimSpace(b.Playbook) || len(a.PatternsExcluded) != len(b.PatternsExcluded) ||
		a.PatternProbeRate != b.PatternProbeRate {
		return false
	}
	x := append([]string(nil), a.PatternsExcluded...)
	y := append([]string(nil), b.PatternsExcluded...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}
