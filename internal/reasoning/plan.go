package reasoning

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

// PlanExemplarSubject is the memory subject suffix of plan exemplars: flow
// plans that ran to completion and were not disputed, reused by the planner
// for similar goals.
const PlanExemplarSubject = "plan_exemplar"

// PlanMetrics describes how a run was planned and whether the plan worked.
// It is recorded for every run that went through the planner, so canary and
// candidate gates can compare planning quality, not only reasoning.
type PlanMetrics struct {
	Mode            string `json:"mode"`
	Repairs         int    `json:"repairs"`
	Fallback        bool   `json:"fallback"`
	Replans         int    `json:"replans"`
	ReviewRequested bool   `json:"review_requested"`
	ReviewRejected  bool   `json:"review_rejected"`
	Injection       bool   `json:"injection"`
	Cached          bool   `json:"cached"`
	// Succeeded is true when the run completed on its final plan.
	Succeeded bool `json:"succeeded"`
}

// planArt is the part of the kernel's plan artifact the recorder reads.
type planArt struct {
	Mode        string          `json:"mode"`
	Rationale   string          `json:"rationale"`
	Flow        json.RawMessage `json:"flow"`
	FlowHash    string          `json:"flow_hash"`
	Repairs     int             `json:"repairs"`
	Fallback    bool            `json:"fallback"`
	Generation  int             `json:"generation"`
	NeedsReview bool            `json:"needs_review"`
	Injection   []any           `json:"injection"`
	Cached      bool            `json:"cached"`
	Analysis    *struct {
		Tools []struct {
			Tool string `json:"tool"`
		} `json:"tools"`
	} `json:"analysis"`
}

// planRejectedPrefix is the failure reason the stores record when a human
// rejects a generated plan.
const planRejectedPrefix = "generated plan rejected"

// PlanMetricsOf reads the plan artifacts of a run. It returns nil when the
// run was not planned, and the final plan artifact otherwise.
func PlanMetricsOf(ctx context.Context, arts artifact.Store, run domain.Run, steps []domain.Step) (*PlanMetrics, *planArt) {
	sorted := append([]domain.Step(nil), steps...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Seq < sorted[j].Seq })
	var pm *PlanMetrics
	var last *planArt
	for _, st := range sorted {
		switch st.Kind {
		case domain.StepPlan:
			if st.Status != domain.StepSucceeded || st.OutputRef == nil {
				continue
			}
			data, err := arts.Get(ctx, run.TenantID, *st.OutputRef)
			if err != nil {
				continue
			}
			var p planArt
			if json.Unmarshal(data, &p) != nil || p.Mode == "" {
				continue
			}
			if pm == nil {
				pm = &PlanMetrics{}
			}
			// Repairs and fallbacks accumulate over generations; mode and
			// injection describe the final plan.
			pm.Repairs += p.Repairs
			pm.Fallback = pm.Fallback || p.Fallback
			pm.Cached = pm.Cached || p.Cached
			pm.Injection = pm.Injection || len(p.Injection) > 0
			pm.Mode = p.Mode
			if p.Generation > pm.Replans {
				pm.Replans = p.Generation
			}
			pm.ReviewRequested = pm.ReviewRequested || (p.Mode == "flow" && p.NeedsReview)
			cp := p
			last = &cp
		case domain.StepPlanReview:
			if pm != nil {
				pm.ReviewRequested = true
			}
		}
	}
	if pm == nil {
		return nil, nil
	}
	if run.FailureReason != nil && strings.HasPrefix(*run.FailureReason, planRejectedPrefix) {
		pm.ReviewRejected = true
	}
	pm.Succeeded = run.Status == domain.RunCompleted
	return pm, last
}

// IsPlanExemplar reports whether a memory is a plan exemplar.
func IsPlanExemplar(m memory.Memory) bool {
	return m.Type == "procedural" && strings.HasSuffix(m.Subject, ":"+PlanExemplarSubject)
}

// DistillPlanExemplar turns a run whose generated flow completed into a plan
// exemplar. It qualifies when the final plan is a model-generated flow (a
// fallback runbook adds nothing new), no input was flagged as injected, the
// run is not disputed and feedback does not mark it wrong. The content has
// no run id, so the same graph distilled twice deduplicates by content.
func DistillPlanExemplar(run domain.Run, goal, service string, p *planArt, pm *PlanMetrics, quality string, correct *bool) (memory.NewMemory, bool) {
	if p == nil || pm == nil || run.Status != domain.RunCompleted || p.Mode != "flow" || len(p.Flow) == 0 ||
		p.Fallback || pm.Injection || pm.ReviewRejected || quality == "DISPUTED" || (correct != nil && !*correct) {
		return memory.NewMemory{}, false
	}
	var compact json.RawMessage
	if b, err := compactJSON(p.Flow); err == nil {
		compact = b
	} else {
		compact = p.Flow
	}
	if len(compact) > 4000 {
		return memory.NewMemory{}, false // too large to be a useful example
	}
	scope, scopeID, subjectKey := "organization", "", "plan"
	if service != "" {
		scope, scopeID, subjectKey = "service", service, service
	}
	var b strings.Builder
	if service != "" {
		fmt.Fprintf(&b, "Plan exemplar for %s\n", service)
	} else {
		b.WriteString("Plan exemplar\n")
	}
	fmt.Fprintf(&b, "Goal: %s\n", clip(oneLine(goal), 300))
	if r := strings.TrimSpace(p.Rationale); r != "" {
		fmt.Fprintf(&b, "Why this graph: %s\n", clip(oneLine(r), 300))
	}
	if pm.Replans > 0 {
		fmt.Fprintf(&b, "Succeeded after %d replan(s); this is the graph that worked.\n", pm.Replans)
	}
	fmt.Fprintf(&b, "Flow: %s\n", compact)
	title := "Plan exemplar"
	if service != "" {
		title += " for " + service
	}
	title += ": " + clip(oneLine(goal), 100)
	status := "CANDIDATE"
	if quality == "VERIFIED" || (correct != nil && *correct) {
		status = "VALIDATING"
	}
	conf := 0.6
	if correct != nil && *correct {
		conf = 0.8
	}
	shards := []domain.MemoryShard{{ShardKey: "kind:plan_exemplar", Weight: 0.5, Source: "plan"}}
	if service != "" {
		shards = append(shards, domain.MemoryShard{ShardKey: "service:" + service, Weight: 1, Source: "scope"})
	}
	seen := map[string]bool{}
	if p.Analysis != nil {
		for _, t := range p.Analysis.Tools {
			if t.Tool != "" && t.Tool != "*" && !seen[t.Tool] && len(seen) < 8 {
				seen[t.Tool] = true
				shards = append(shards, domain.MemoryShard{ShardKey: "tool:" + t.Tool, Weight: 0.4, Source: "plan"})
			}
		}
	}
	return memory.NewMemory{TenantID: run.TenantID, Scope: scope, ScopeID: scopeID, Type: "procedural",
		Status: status, Subject: subjectKey + ":" + PlanExemplarSubject, Title: title,
		Content: strings.TrimSpace(b.String()), SourceRefs: []string{"run:" + run.ID}, Trust: "T3",
		Confidence: &conf, CreatedBy: DistillerID, Shards: shards}, true
}

func compactJSON(raw json.RawMessage) (json.RawMessage, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

// ---------- aggregation and gate ----------

// PlanSummary aggregates the plan metrics of many runs.
type PlanSummary struct {
	N int `json:"n"`
	// FlowRate is the share of runs whose final plan was a flow.
	FlowRate    float64 `json:"flow_rate"`
	RepairRate  float64 `json:"repair_rate"`
	MeanRepairs float64 `json:"mean_repairs"`
	// FallbackRate is the share of runs that used a fallback plan.
	FallbackRate float64 `json:"fallback_rate"`
	ReplanRate   float64 `json:"replan_rate"`
	ReviewRate   float64 `json:"review_rate"`
	// RejectRate is rejected reviews over requested reviews.
	RejectRate    float64 `json:"reject_rate"`
	SuccessRate   float64 `json:"success_rate"`
	InjectionRate float64 `json:"injection_rate"`
	CacheHitRate  float64 `json:"cache_hit_rate"`
}

// SummarizePlans aggregates the planned runs among ms (nil when none).
func SummarizePlans(ms []Metrics) *PlanSummary {
	var s PlanSummary
	var flows, repaired, repairs, fallbacks, replans, reviews, rejected, success, injection, cached int
	for _, m := range ms {
		p := m.Plan
		if p == nil {
			continue
		}
		s.N++
		if p.Mode == "flow" {
			flows++
		}
		if p.Repairs > 0 {
			repaired++
		}
		repairs += p.Repairs
		if p.Fallback {
			fallbacks++
		}
		if p.Replans > 0 {
			replans++
		}
		if p.ReviewRequested {
			reviews++
			if p.ReviewRejected {
				rejected++
			}
		}
		if p.Succeeded {
			success++
		}
		if p.Injection {
			injection++
		}
		if p.Cached {
			cached++
		}
	}
	if s.N == 0 {
		return nil
	}
	n := float64(s.N)
	s.FlowRate = round(float64(flows) / n)
	s.RepairRate = round(float64(repaired) / n)
	s.MeanRepairs = round(float64(repairs) / n)
	s.FallbackRate = round(float64(fallbacks) / n)
	s.ReplanRate = round(float64(replans) / n)
	s.ReviewRate = round(float64(reviews) / n)
	if reviews > 0 {
		s.RejectRate = round(float64(rejected) / float64(reviews))
	}
	s.SuccessRate = round(float64(success) / n)
	s.InjectionRate = round(float64(injection) / n)
	s.CacheHitRate = round(float64(cached) / n)
	return &s
}

// PlanGateConfig bounds how much worse a candidate may plan than its base.
// Zero values use the defaults in DefaultPlanGate.
type PlanGateConfig struct {
	MinRuns              int     `json:"min_runs,omitempty"`
	MaxFallbackIncrease  float64 `json:"max_fallback_increase,omitempty"`
	MaxRepairIncrease    float64 `json:"max_repair_increase,omitempty"`
	MaxReplanIncrease    float64 `json:"max_replan_increase,omitempty"`
	MaxRejectIncrease    float64 `json:"max_reject_increase,omitempty"`
	MaxSuccessDrop       float64 `json:"max_success_drop,omitempty"`
	MaxInjectionIncrease float64 `json:"max_injection_increase,omitempty"`
}

// DefaultPlanGate returns the default plan margins.
func DefaultPlanGate() PlanGateConfig {
	return PlanGateConfig{MinRuns: 5, MaxFallbackIncrease: 0.10, MaxRepairIncrease: 0.15, MaxReplanIncrease: 0.10,
		MaxRejectIncrease: 0.10, MaxSuccessDrop: 0.10, MaxInjectionIncrease: 0.05}
}

func (c PlanGateConfig) withDefaults() PlanGateConfig {
	d := DefaultPlanGate()
	if c.MinRuns <= 0 {
		c.MinRuns = d.MinRuns
	}
	set := func(v *float64, def float64) {
		if *v <= 0 {
			*v = def
		}
	}
	set(&c.MaxFallbackIncrease, d.MaxFallbackIncrease)
	set(&c.MaxRepairIncrease, d.MaxRepairIncrease)
	set(&c.MaxReplanIncrease, d.MaxReplanIncrease)
	set(&c.MaxRejectIncrease, d.MaxRejectIncrease)
	set(&c.MaxSuccessDrop, d.MaxSuccessDrop)
	set(&c.MaxInjectionIncrease, d.MaxInjectionIncrease)
	return c
}

// PlanGate returns the planning regressions of cand relative to base. Both
// sides need MinRuns planned runs; otherwise nothing is compared.
func PlanGate(base, cand *PlanSummary, cfg PlanGateConfig) []string {
	cfg = cfg.withDefaults()
	if base == nil || cand == nil || base.N < cfg.MinRuns || cand.N < cfg.MinRuns {
		return nil
	}
	var f []string
	if cand.FallbackRate-base.FallbackRate > cfg.MaxFallbackIncrease {
		f = append(f, fmt.Sprintf("plan fallback rate rose: %.3f → %.3f", base.FallbackRate, cand.FallbackRate))
	}
	if cand.RepairRate-base.RepairRate > cfg.MaxRepairIncrease {
		f = append(f, fmt.Sprintf("plan repair rate rose: %.3f → %.3f", base.RepairRate, cand.RepairRate))
	}
	if cand.ReplanRate-base.ReplanRate > cfg.MaxReplanIncrease {
		f = append(f, fmt.Sprintf("replan rate rose: %.3f → %.3f", base.ReplanRate, cand.ReplanRate))
	}
	if cand.RejectRate-base.RejectRate > cfg.MaxRejectIncrease {
		f = append(f, fmt.Sprintf("plan review rejection rate rose: %.3f → %.3f", base.RejectRate, cand.RejectRate))
	}
	if base.SuccessRate-cand.SuccessRate > cfg.MaxSuccessDrop {
		f = append(f, fmt.Sprintf("planned run success rate dropped: %.3f → %.3f", base.SuccessRate, cand.SuccessRate))
	}
	if cand.InjectionRate-base.InjectionRate > cfg.MaxInjectionIncrease {
		f = append(f, fmt.Sprintf("planner injection flags rose: %.3f → %.3f", base.InjectionRate, cand.InjectionRate))
	}
	return f
}
