package reasoning

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// Summary aggregates the metrics of many runs.
type Summary struct {
	N                int      `json:"n"`
	MeanQuality      float64  `json:"mean_quality"`
	MeanBrier        *float64 `json:"mean_brier,omitempty"`
	CalibratedRuns   int      `json:"calibrated_runs"`
	Grounding        float64  `json:"grounding"`
	Redundancy       float64  `json:"redundancy"`
	VerifiedRate     float64  `json:"verified_conclusion_rate"`
	VerifiedLower95  float64  `json:"verified_conclusion_wilson_lower_95"`
	UnverifiedRate   float64  `json:"unverified_conclusion_rate"`
	MeanStepsVerdict float64  `json:"mean_steps_to_verdict"`
	NoVerdictRate    float64  `json:"no_verdict_rate"`
	SuccessRate      float64  `json:"success_rate"`
	CorrectN         int      `json:"feedback_n"`
	CorrectRate      float64  `json:"feedback_correct_rate"`
	// Structured is the mean share of structured thoughts per run.
	Structured float64 `json:"structured"`
	// Settled counts VERIFIED/REFUTED hypotheses; CalibrationCoverage is
	// the share of them that carried a confidence (pooled).
	Settled             int     `json:"settled_hypotheses"`
	CalibrationCoverage float64 `json:"calibration_coverage"`
	// Citations pools the citations of all runs; CitationValidity is the
	// resolved share. UnsupportedVerdictRate is the share of settled
	// hypotheses without a resolved citation; UnsupportedConclusionRate
	// the share of runs whose VERIFIED conclusion cited nothing resolved.
	Citations                 int     `json:"citations"`
	CitationValidity          float64 `json:"citation_validity"`
	UnsupportedVerdictRate    float64 `json:"unsupported_verdict_rate"`
	UnsupportedConclusionRate float64 `json:"unsupported_conclusion_rate"`
	// Plan aggregates the planned runs (nil: none was planned).
	Plan *PlanSummary `json:"plan,omitempty"`
}

// Summarize aggregates metrics. Grounding and redundancy are pooled over
// claims and tool calls, so long runs weigh more than short ones.
func Summarize(ms []Metrics) Summary {
	s := Summary{N: len(ms)}
	if s.N == 0 {
		return s
	}
	var quality, brier, stepsV, structured float64
	var claims, grounded, calls, redundant, verified, unverified, verdictRuns, noVerdict, success, correct int
	var calibrated, unresolved, unsupportedVerdicts, unsupportedConclusions int
	for _, m := range ms {
		s.Citations += m.Citations
		unresolved += m.Unresolved
		unsupportedVerdicts += m.UnsupportedVerdicts
		if m.UnsupportedConclusion {
			unsupportedConclusions++
		}
		quality += m.Quality
		structured += m.Structured
		s.Settled += m.Verified + m.Refuted
		calibrated += m.CalibrationN
		if m.Brier != nil {
			brier += *m.Brier
			s.CalibratedRuns++
		}
		claims += m.Claims
		grounded += m.Grounded
		calls += m.ToolCalls
		redundant += m.RedundantCalls
		switch {
		case m.VerifiedConclusion():
			verified++
		case m.Conclusion == "VERIFIED":
			unverified++ // self-declared, counted as unverified
		case m.Conclusion == "UNVERIFIED":
			unverified++
		}
		if m.StepsToVerdict > 0 {
			stepsV += float64(m.StepsToVerdict)
			verdictRuns++
		} else {
			noVerdict++
		}
		if m.Success() {
			success++
		}
		if m.Correct != nil {
			s.CorrectN++
			if *m.Correct {
				correct++
			}
		}
	}
	n := float64(s.N)
	s.MeanQuality = round(quality / n)
	if s.CalibratedRuns > 0 {
		b := round(brier / float64(s.CalibratedRuns))
		s.MeanBrier = &b
	}
	if claims > 0 {
		s.Grounding = round(float64(grounded) / float64(claims))
	}
	if calls > 0 {
		s.Redundancy = round(float64(redundant) / float64(calls))
	}
	s.VerifiedRate = round(float64(verified) / n)
	s.VerifiedLower95 = round(WilsonLower(verified, s.N))
	s.UnverifiedRate = round(float64(unverified) / n)
	if verdictRuns > 0 {
		s.MeanStepsVerdict = round(stepsV / float64(verdictRuns))
	}
	s.NoVerdictRate = round(float64(noVerdict) / n)
	s.SuccessRate = round(float64(success) / n)
	if s.CorrectN > 0 {
		s.CorrectRate = round(float64(correct) / float64(s.CorrectN))
	}
	s.Structured = round(structured / n)
	if s.Settled > 0 {
		s.CalibrationCoverage = round(float64(calibrated) / float64(s.Settled))
	}
	s.CitationValidity = 1
	if s.Citations > 0 {
		s.CitationValidity = round(float64(s.Citations-unresolved) / float64(s.Citations))
	}
	if s.Settled > 0 {
		s.UnsupportedVerdictRate = round(float64(unsupportedVerdicts) / float64(s.Settled))
	}
	s.UnsupportedConclusionRate = round(float64(unsupportedConclusions) / n)
	s.Plan = SummarizePlans(ms)
	return s
}

// WilsonLower is the lower bound of the 95% Wilson score interval.
func WilsonLower(k, n int) float64 {
	if n == 0 {
		return 0
	}
	z := 1.96
	p := float64(k) / float64(n)
	nn := float64(n)
	den := 1 + z*z/nn
	center := p + z*z/(2*nn)
	margin := z * math.Sqrt(p*(1-p)/nn+z*z/(4*nn*nn))
	return (center - margin) / den
}

// ---------- gate ----------

// GateConfig bounds how much worse a candidate's reasoning may be than the
// base on the same cases.
type GateConfig struct {
	MaxBrierIncrease      float64 `json:"max_brier_increase"`
	MaxGroundingDrop      float64 `json:"max_grounding_drop"`
	MaxRedundancyIncrease float64 `json:"max_redundancy_increase"`
	MaxQualityDrop        float64 `json:"max_quality_drop"`
	// MaxStructuredDrop and MaxCoverageDrop guard the structured trace the
	// other checks depend on (0 = default 0.10).
	MaxStructuredDrop float64 `json:"max_structured_drop,omitempty"`
	MaxCoverageDrop   float64 `json:"max_coverage_drop,omitempty"`
	// MaxCitationValidityDrop and MaxUnsupportedIncrease guard citation
	// checks (0 = default 0.10): the resolved share of citations, and the
	// share of settled hypotheses and conclusions without one.
	MaxCitationValidityDrop float64 `json:"max_citation_validity_drop,omitempty"`
	MaxUnsupportedIncrease  float64 `json:"max_unsupported_increase,omitempty"`
	// Plan bounds planning regressions (zero values: DefaultPlanGate).
	Plan PlanGateConfig `json:"plan,omitempty"`
}

// DefaultGate returns the default margins.
func DefaultGate() GateConfig {
	return GateConfig{MaxBrierIncrease: 0.05, MaxGroundingDrop: 0.10, MaxRedundancyIncrease: 0.10, MaxQualityDrop: 0.05,
		MaxStructuredDrop: 0.10, MaxCoverageDrop: 0.10}
}

// Gate returns the reasoning regressions of cand relative to base.
func Gate(base, cand Summary, cfg GateConfig) []string {
	var f []string
	if base.N == 0 || cand.N == 0 {
		return nil
	}
	if base.MeanBrier != nil && cand.MeanBrier != nil && *cand.MeanBrier-*base.MeanBrier > cfg.MaxBrierIncrease {
		f = append(f, fmt.Sprintf("calibration regressed: Brier %.3f → %.3f", *base.MeanBrier, *cand.MeanBrier))
	}
	if base.Grounding-cand.Grounding > cfg.MaxGroundingDrop {
		f = append(f, fmt.Sprintf("evidence grounding dropped: %.3f → %.3f", base.Grounding, cand.Grounding))
	}
	if cand.Redundancy-base.Redundancy > cfg.MaxRedundancyIncrease {
		f = append(f, fmt.Sprintf("redundant tool calls rose: %.3f → %.3f", base.Redundancy, cand.Redundancy))
	}
	if base.MeanQuality-cand.MeanQuality > cfg.MaxQualityDrop {
		f = append(f, fmt.Sprintf("reasoning quality dropped: %.3f → %.3f", base.MeanQuality, cand.MeanQuality))
	}
	sd, cd := cfg.MaxStructuredDrop, cfg.MaxCoverageDrop
	if sd <= 0 {
		sd = 0.10
	}
	if cd <= 0 {
		cd = 0.10
	}
	if base.Structured-cand.Structured > sd {
		f = append(f, fmt.Sprintf("structured trace share dropped: %.3f → %.3f", base.Structured, cand.Structured))
	}
	// Coverage is only comparable when both sides settled hypotheses.
	if base.Settled > 0 && cand.Settled > 0 && base.CalibrationCoverage-cand.CalibrationCoverage > cd {
		f = append(f, fmt.Sprintf("confidence coverage dropped: %.3f → %.3f", base.CalibrationCoverage, cand.CalibrationCoverage))
	}
	vd, ui := cfg.MaxCitationValidityDrop, cfg.MaxUnsupportedIncrease
	if vd <= 0 {
		vd = 0.10
	}
	if ui <= 0 {
		ui = 0.10
	}
	if base.Citations > 0 && cand.Citations > 0 && base.CitationValidity-cand.CitationValidity > vd {
		f = append(f, fmt.Sprintf("citation validity dropped: %.3f → %.3f", base.CitationValidity, cand.CitationValidity))
	}
	if base.Settled > 0 && cand.Settled > 0 && cand.UnsupportedVerdictRate-base.UnsupportedVerdictRate > ui {
		f = append(f, fmt.Sprintf("unsupported verdicts rose: %.3f → %.3f", base.UnsupportedVerdictRate, cand.UnsupportedVerdictRate))
	}
	if cand.UnsupportedConclusionRate-base.UnsupportedConclusionRate > ui {
		f = append(f, fmt.Sprintf("unsupported conclusions rose: %.3f → %.3f", base.UnsupportedConclusionRate, cand.UnsupportedConclusionRate))
	}
	return append(f, PlanGate(base.Plan, cand.Plan, cfg.Plan)...)
}

// ---------- stored scores and pattern statistics ----------

// RunScore is the stored score of one run.
type RunScore struct {
	TenantID       string    `json:"-"`
	RunID          string    `json:"run_id"`
	ProfileID      string    `json:"profile_id"`
	ProfileVersion int64     `json:"profile_version"`
	Service        string    `json:"service,omitempty"`
	Outcome        string    `json:"outcome"`
	Metrics        Metrics   `json:"metrics"`
	MemoryIDs      []string  `json:"memory_ids,omitempty"`
	ScoredAt       time.Time `json:"scored_at"`
}

// ScoreFilter selects stored scores.
type ScoreFilter struct {
	ProfileID string
	Version   int64 // 0: any
	Service   string
	Since     time.Time
	Limit     int // default 1000, max 10000
}

// PatternStat is the measured effectiveness of one memory.
//
// Raw rates are confounded by how hard a service is, so Effect compares the
// runs that were exposed to the memory with the other runs of the same
// service: Expected is the success count those runs would have had at their
// service's baseline rate (computed without the exposed runs), Lift the
// difference per run and Z its significance. Exposure is either retrieval
// (Effect) or an explicit citation by the model (CitedEffect).
type PatternStat struct {
	MemoryID       string  `json:"memory_id"`
	Uses           int     `json:"uses"`
	Successes      int     `json:"successes"`
	Rate           float64 `json:"success_rate"`
	Lower95        float64 `json:"success_wilson_lower_95"`
	Quality        float64 `json:"mean_quality"`
	Cited          int     `json:"cited"`
	CitedSuccesses int     `json:"cited_successes"`
	Effect         *Effect `json:"effect,omitempty"`
	CitedEffect    *Effect `json:"cited_effect,omitempty"`
}

// Effect is the baseline-relative effect of an exposure.
type Effect struct {
	// N counts exposed runs whose service has unexposed runs to compare with.
	N         int     `json:"n"`
	Successes int     `json:"successes"`
	Expected  float64 `json:"expected"`
	Lift      float64 `json:"lift"`
	Z         float64 `json:"z"`
}

// Decisive returns the effect to act on: citations when there are at least
// minUses cited runs, otherwise retrievals. nil when neither has minUses
// comparable runs.
func (p PatternStat) Decisive(minUses int) (*Effect, string) {
	if p.CitedEffect != nil && p.CitedEffect.N >= minUses {
		return p.CitedEffect, "cited"
	}
	if p.Effect != nil && p.Effect.N >= minUses {
		return p.Effect, "retrieved"
	}
	return nil, ""
}

// PatternStats measures every memory the scored runs retrieved or cited.
func PatternStats(scores []RunScore) map[string]PatternStat {
	acc := map[string]*PatternStat{}
	get := func(id string) *PatternStat {
		p := acc[id]
		if p == nil {
			p = &PatternStat{MemoryID: id}
			acc[id] = p
		}
		return p
	}
	retrieved := make([]map[string]bool, len(scores))
	cited := make([]map[string]bool, len(scores))
	for i, s := range scores {
		retrieved[i], cited[i] = set(s.MemoryIDs), set(s.Metrics.CitedMemories)
		ok := s.Metrics.Success()
		for id := range retrieved[i] {
			p := get(id)
			p.Uses++
			p.Quality += s.Metrics.Quality
			if ok {
				p.Successes++
			}
		}
		for id := range cited[i] {
			p := get(id)
			p.Cited++
			if ok {
				p.CitedSuccesses++
			}
		}
	}
	out := make(map[string]PatternStat, len(acc))
	for id, p := range acc {
		if p.Uses > 0 {
			p.Rate = round(float64(p.Successes) / float64(p.Uses))
			p.Lower95 = round(WilsonLower(p.Successes, p.Uses))
			p.Quality = round(p.Quality / float64(p.Uses))
		}
		p.Effect = effect(scores, func(i int) bool { return retrieved[i][id] })
		p.CitedEffect = effect(scores, func(i int) bool { return cited[i][id] })
		out[id] = *p
	}
	return out
}

// effect compares exposed runs with the unexposed runs of their service.
// Baselines use a Beta(1,1) prior so a service with few comparison runs
// does not produce a 0 or 1 baseline.
func effect(scores []RunScore, exposed func(i int) bool) *Effect {
	type agg struct{ n, ok int }
	base := map[string]*agg{}
	for i, s := range scores {
		if exposed(i) {
			continue
		}
		a := base[s.Service]
		if a == nil {
			a = &agg{}
			base[s.Service] = a
		}
		a.n++
		if s.Metrics.Success() {
			a.ok++
		}
	}
	e := Effect{}
	var variance float64
	for i, s := range scores {
		if !exposed(i) {
			continue
		}
		a := base[s.Service]
		if a == nil || a.n == 0 {
			continue // nothing to compare with
		}
		p := float64(a.ok+1) / float64(a.n+2)
		e.N++
		e.Expected += p
		variance += p * (1 - p)
		if s.Metrics.Success() {
			e.Successes++
		}
	}
	if e.N == 0 {
		return nil
	}
	diff := float64(e.Successes) - e.Expected
	e.Lift = round(diff / float64(e.N))
	if variance > 0 {
		e.Z = round(diff / math.Sqrt(variance))
	}
	e.Expected = round(e.Expected)
	return &e
}

func set(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		if x != "" {
			m[x] = true
		}
	}
	return m
}

// SortedStats returns stats ordered by uses, then id.
func SortedStats(m map[string]PatternStat) []PatternStat {
	out := make([]PatternStat, 0, len(m))
	for _, p := range m {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Uses != out[j].Uses {
			return out[i].Uses > out[j].Uses
		}
		return out[i].MemoryID < out[j].MemoryID
	})
	return out
}

// MetricsOf extracts the metrics of stored scores.
func MetricsOf(scores []RunScore) []Metrics {
	out := make([]Metrics, len(scores))
	for i, s := range scores {
		out[i] = s.Metrics
	}
	return out
}
