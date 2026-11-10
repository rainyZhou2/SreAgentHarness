package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/evidenceplan"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/untrusted"
	"go-sre-harness/internal/work"
)

// CheckSource is the framework's view of the tools a run may call: the
// tenant's registered tools plus the hard constraints of the run's profile
// and policy (RFC §12.4). The orchestrator adds target and budget.
type CheckSource interface {
	Checks(ctx context.Context, tenantID, profileID string, profileVersion int64, routingKey string) ([]tools.Descriptor, evidenceplan.Constraints, error)
}

// EventEvidencePlanned records the checks planned for a stage's runs.
const EventEvidencePlanned = "work.evidence_planned"

// evidencePlan is what planning hands to the run starts.
type evidencePlan struct {
	text  map[string]string // agent role -> goal section
	event *work.NewEvent
}

// planEvidence ranks the read-only checks that best separate the Work's
// live hypotheses, per run of the stage. Planning is advisory for the run
// but bounded by the framework: a failure to plan never blocks the
// investigation, it only means the runs start without a plan.
func (d *driver) planEvidence(ctx context.Context, stage work.Stage, specs []work.AgentSpec, keys []string) evidencePlan {
	out := evidencePlan{text: map[string]string{}}
	spec := stage.EvidencePlan
	if d.o.Checks == nil || (spec != nil && spec.Disabled) {
		return out
	}
	w := &d.w
	hyps, err := d.o.Store.ListHypotheses(ctx, w.TenantID, w.ID)
	if err != nil {
		d.o.log().Warn("evidence planning skipped", "work_id", w.ID, "err", err)
		d.o.Metrics.evidencePlan("error")
		return out
	}
	var live []evidence.Hypothesis
	for _, h := range hyps {
		if evidenceplan.Live(h) {
			live = append(live, h)
		}
	}
	if len(live) == 0 {
		d.o.Metrics.evidencePlan("no_hypotheses")
		return out
	}
	prev, suggested := d.planHistory(ctx)
	k := evidenceplan.DefaultMaxChecks
	if spec != nil && spec.MaxChecks > 0 {
		k = min(spec.MaxChecks, evidenceplan.MaxChecks)
	}
	budget := budgetLeft(w)

	// Runs with the same profile share one candidate set; their checks are
	// dealt out by rank so parallel agents test different things.
	type group struct {
		profile string
		version int64
		idx     []int
	}
	var groups []*group
	byProfile := map[string]*group{}
	for i, a := range specs {
		gk := fmt.Sprintf("%s@%d", a.ProfileID, a.ProfileVersion)
		g := byProfile[gk]
		if g == nil {
			g = &group{profile: a.ProfileID, version: a.ProfileVersion}
			byProfile[gk] = g
			groups = append(groups, g)
		}
		g.idx = append(g.idx, i)
	}
	excluded := map[string]int{}
	type rolePlan struct {
		Role   string               `json:"role"`
		Checks []evidenceplan.Check `json:"checks"`
	}
	var plans []rolePlan
	merged := evidenceplan.Plan{Version: evidenceplan.Version, At: d.o.now(), Digest: evidenceplan.Digest(live), Live: len(live)}
	for _, g := range groups {
		descs, cons, err := d.o.Checks.Checks(ctx, w.TenantID, g.profile, g.version, keys[g.idx[0]])
		if err != nil {
			d.o.log().Warn("evidence planning skipped for a profile", "work_id", w.ID, "profile", g.profile, "err", err)
			d.o.Metrics.evidencePlan("error")
			continue
		}
		cons.Target, cons.BudgetLeft = w.Goal.PrimarySubject(), budget
		cands, ex := evidenceplan.Filter(descs, cons)
		for _, e := range ex {
			excluded[e.Reason]++
		}
		p := evidenceplan.Score(evidenceplan.Input{Hypotheses: live, Candidates: cands, Suggested: suggested,
			Previous: prev, Now: d.o.now(), K: k * len(g.idx)})
		merged.Candidates += p.Candidates
		if merged.Reason == "" {
			merged.Reason = p.Reason
		}
		per := make([][]evidenceplan.Check, len(g.idx))
		for r, c := range p.Checks {
			per[r%len(g.idx)] = append(per[r%len(g.idx)], c)
		}
		for j, i := range g.idx {
			role := specs[i].Role
			plans = append(plans, rolePlan{Role: role, Checks: per[j]})
			merged.Checks = append(merged.Checks, per[j]...)
			if len(per[j]) > 0 {
				out.text[role] = planText(live, per[j])
			}
		}
	}
	if len(merged.Checks) > 0 {
		merged.Reason = ""
		d.o.Metrics.evidencePlan("planned")
	} else {
		d.o.Metrics.evidencePlan("empty")
	}
	d.o.Metrics.evidenceExcluded(excluded)
	out.event = &work.NewEvent{Type: EventEvidencePlanned, Payload: map[string]any{"stage_id": stage.ID,
		"generation": w.Generation, "plan": merged, "runs": plans, "excluded": excluded}}
	return out
}

// planHistory returns the Work's latest plan and the checks the last
// completed run stage suggested.
func (d *driver) planHistory(ctx context.Context) (*evidenceplan.Plan, map[string]float64) {
	after := max(d.w.LastSeq-1000, 0)
	evs, err := d.o.Store.ListWorkEvents(ctx, d.w.TenantID, d.w.ID, after, 1000)
	if err != nil {
		return nil, nil
	}
	var prev *evidenceplan.Plan
	var suggested map[string]float64
	for _, e := range evs {
		switch e.Type {
		case EventEvidencePlanned:
			var p struct {
				Plan evidenceplan.Plan `json:"plan"`
			}
			if json.Unmarshal(e.Payload, &p) == nil {
				prev = &p.Plan
			}
		case "work.stage_completed":
			var p struct {
				NextChecks map[string]float64 `json:"next_checks"`
			}
			if json.Unmarshal(e.Payload, &p) == nil && len(p.NextChecks) > 0 {
				suggested = p.NextChecks
			}
		}
	}
	return prev, suggested
}

// budgetLeft is the share of the tighter of the Work's cost budgets left.
func budgetLeft(w *work.Work) float64 {
	left := 1.0
	spent := float64(w.CostMicros) / 1e6
	for _, lim := range []float64{w.Template.Limits.MaxCostUSD, w.Goal.Constraints.MaxCostUSD} {
		if lim > 0 {
			left = min(left, max(0, 1-spent/lim))
		}
	}
	return left
}

// planText is the goal section of one run. Tool names, scores and reasons
// are framework output; hypothesis statements come from earlier runs and
// stay marked as untrusted data.
func planText(live []evidence.Hypothesis, checks []evidenceplan.Check) string {
	idx := map[string]int{}
	var hb strings.Builder
	for i, h := range live {
		idx[h.Key] = i + 1
		fmt.Fprintf(&hb, "H%d [%s] %s\n", i+1, h.Status, cpkit.Truncate(h.Statement, 200))
	}
	var b strings.Builder
	b.WriteString("\n\nEvidence plan (framework): the competing hypotheses so far are\n")
	b.WriteString(untrusted.Wrap("source=work.hypotheses trust=T4", strings.TrimSpace(hb.String())).Text)
	b.WriteString("\nThese read-only checks separate them best; run them first, cite their results, and mark each " +
		"hypothesis SUPPORTED or REFUTED as the evidence settles it:\n")
	for i, c := range checks {
		var bears []string
		for _, k := range c.Bears {
			if n, ok := idx[k]; ok {
				bears = append(bears, fmt.Sprintf("H%d", n))
			}
		}
		on := ""
		if len(bears) > 0 {
			on = " (bears on " + strings.Join(bears, ", ") + ")"
		}
		fmt.Fprintf(&b, "%d. %s%s: %s\n", i+1, c.Tool, on, c.Rationale)
	}
	b.WriteString("Suggest further checks in next_checks; tools outside this plan follow your normal policy.")
	return b.String()
}
