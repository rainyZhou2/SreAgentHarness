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
}

// Summarize aggregates metrics. Grounding and redundancy are pooled over
// claims and tool calls, so long runs weigh more than short ones.
func Summarize(ms []Metrics) Summary {
	s := Summary{N: len(ms)}
	if s.N == 0 {
		return s
	}
	var quality, brier, stepsV float64
	var claims, grounded, calls, redundant, verified, unverified, verdictRuns, noVerdict, success, correct int
	for _, m := range ms {
		quality += m.Quality
		if m.Brier != nil {
			brier += *m.Brier
			s.CalibratedRuns++
		}
		claims += m.Claims
		grounded += m.Grounded
		calls += m.ToolCalls
		redundant += m.RedundantCalls
		switch m.Conclusion {
		case "VERIFIED":
			verified++
		case "UNVERIFIED":
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
}

// DefaultGate returns the default margins.
func DefaultGate() GateConfig {
	return GateConfig{MaxBrierIncrease: 0.05, MaxGroundingDrop: 0.10, MaxRedundancyIncrease: 0.10, MaxQualityDrop: 0.05}
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
	return f
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
type PatternStat struct {
	MemoryID  string  `json:"memory_id"`
	Uses      int     `json:"uses"`
	Successes int     `json:"successes"`
	Rate      float64 `json:"success_rate"`
	Lower95   float64 `json:"success_wilson_lower_95"`
	Quality   float64 `json:"mean_quality"`
}

// PatternStats measures every memory the scored runs retrieved.
func PatternStats(scores []RunScore) map[string]PatternStat {
	acc := map[string]*PatternStat{}
	for _, s := range scores {
		seen := map[string]bool{}
		for _, id := range s.MemoryIDs {
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			p := acc[id]
			if p == nil {
				p = &PatternStat{MemoryID: id}
				acc[id] = p
			}
			p.Uses++
			p.Quality += s.Metrics.Quality
			if s.Metrics.Success() {
				p.Successes++
			}
		}
	}
	out := make(map[string]PatternStat, len(acc))
	for id, p := range acc {
		p.Rate = round(float64(p.Successes) / float64(p.Uses))
		p.Lower95 = round(WilsonLower(p.Successes, p.Uses))
		p.Quality = round(p.Quality / float64(p.Uses))
		out[id] = *p
	}
	return out
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
