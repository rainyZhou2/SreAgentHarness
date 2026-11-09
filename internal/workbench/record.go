package workbench

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/postmortem"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workeval"
)

// Source is the control-plane ledger a Work is recorded from
// (cpsql.Store).
type Source interface {
	postmortem.Ledger
}

// Record exports a closed (or open) Work as a self-contained Case:
//
//   - the pinned template and goal recreate the Work;
//   - each owner run becomes a scripted run whose result is rebuilt from
//     the ledgers (hypotheses it raised, the action it proposed, its cost),
//     never from model text the ledgers do not hold;
//   - each action becomes a simulated executor with its recorded outcome,
//     and its approvals become scripted approvals;
//   - metric objective values of the outcome evaluations become metric
//     series, so verification sees the same values at the same offsets;
//   - evidence records and human confirmations are replayed at their
//     offsets.
//
// The case expects the recorded status and scorecard verdict, so a replay
// that diverges fails.
func Record(ctx context.Context, src Source, tenantID, workID string) (Case, error) {
	w, err := src.GetWork(ctx, tenantID, workID)
	if err != nil {
		return Case{}, err
	}
	links, err := src.ListRunLinks(ctx, tenantID, workID)
	if err != nil {
		return Case{}, err
	}
	acts, err := src.ListActions(ctx, tenantID, workID)
	if err != nil {
		return Case{}, err
	}
	hyps, err := src.ListHypotheses(ctx, tenantID, workID)
	if err != nil {
		return Case{}, err
	}
	evs, err := src.ListEvidence(ctx, tenantID, workID, 500)
	if err != nil {
		return Case{}, err
	}
	evals, err := src.ListOutcomes(ctx, tenantID, workID, 500)
	if err != nil {
		return Case{}, err
	}
	events, err := src.ListWorkEvents(ctx, tenantID, workID, 0, 2000)
	if err != nil {
		return Case{}, err
	}
	start := w.CreatedAt.UTC()
	off := func(t time.Time) cpkit.Duration {
		if t.Before(start) {
			return 0
		}
		return cpkit.Duration(t.Sub(start).Round(time.Second))
	}
	tpl := w.Template
	c := Case{Name: "replay-" + w.ID, Description: fmt.Sprintf("Recorded from Work %s (%s): %s", w.ID, w.Status, w.Title),
		Tenant: tenantID, Start: start, Templates: []work.Template{tpl},
		Work: &WorkSpec{Kind: w.Kind, Title: w.Title, Template: tpl.ID, Goal: w.Goal}}

	// Runs: owner links in creation order, results rebuilt from ledgers.
	sort.SliceStable(links, func(i, j int) bool { return links[i].CreatedAt.Before(links[j].CreatedAt) })
	hypsOf, actOf := attribute(tpl, links, hyps, acts)
	for _, l := range links {
		if l.StageID == "" {
			continue
		}
		sr := ScriptedRun{Stage: l.StageID, Role: l.AgentRole, Duration: cpkit.Duration(time.Minute)}
		if l.FinishedAt != nil && l.FinishedAt.After(l.CreatedAt) {
			sr.Duration = cpkit.Duration(l.FinishedAt.Sub(l.CreatedAt).Round(time.Second))
		}
		if failed(l) {
			sr.Status, sr.FailureReason = "failed", "recorded run ended "+firstNonEmpty(l.Outcome, l.Status)
		}
		sr.Result = runResult(l, hypsOf[l.RunID], actOf[l.RunID])
		c.Runs = append(c.Runs, sr)
	}

	// Actions and approvals.
	seen := map[string]bool{}
	sort.SliceStable(acts, func(i, j int) bool { return acts[i].CreatedAt.Before(acts[j].CreatedAt) })
	for _, a := range acts {
		for _, ap := range a.ApprovalLog {
			c.Approvals = append(c.Approvals, Approval{Action: a.ActionName, Decision: strings.ToLower(firstNonEmpty(ap.Decision, "approve")),
				Approver: ap.ApproverID, Comment: "recorded"})
		}
		if seen[a.ActionName] {
			continue
		}
		seen[a.ActionName] = true
		kind, _, _ := strings.Cut(a.TargetRef, "://")
		sa := SimAction{Name: a.ActionName, Risk: string(a.Risk), ResourceKinds: []string{kind}}
		switch a.State {
		case action.StateEffectUnknown, action.StateManualRecovery:
			sa.Outcome = "unknown"
		case action.StateFailed:
			if !a.MayHaveDispatched {
				sa.Outcome = "not_applied"
			} else {
				sa.Outcome = "unknown"
			}
		}
		c.Actions = append(c.Actions, sa)
	}

	// Metric objectives -> series (oldest evaluation first).
	metricOf := map[string][2]string{}
	if w.Goal.Success != nil {
		for _, o := range append(slices.Clone(w.Goal.Success.AllOf), w.Goal.Success.AnyOf...) {
			if o.Metric != nil {
				metricOf[o.ID] = [2]string{o.Metric.Source, o.Metric.Query}
			}
		}
	}
	series := map[[2]string]*Series{}
	var order [][2]string
	for i := len(evals) - 1; i >= 0; i-- {
		e := evals[i]
		for _, ob := range e.Objectives {
			k, ok := metricOf[ob.ID]
			if !ok || ob.Value == nil {
				continue
			}
			s := series[k]
			if s == nil {
				s = &Series{Source: k[0], Query: k[1]}
				series[k] = s
				order = append(order, k)
			}
			// Values hold from a window before the evaluation, so the
			// replayed evaluation aggregates the same value.
			at := off(e.EvaluatedAt.Add(-time.Minute))
			if n := len(s.Points); n > 0 && s.Points[n-1].At >= at {
				at = s.Points[n-1].At + cpkit.Duration(time.Second)
			}
			s.Points = append(s.Points, Point{At: at, V: *ob.Value})
		}
	}
	for _, k := range order {
		c.Metrics = append(c.Metrics, *series[k])
	}

	// Evidence not produced by runs (runs re-produce theirs).
	for _, r := range evs {
		if r.RunID != "" {
			continue
		}
		c.Evidence = append(c.Evidence, Evidence{At: off(r.CapturedAt), SourceType: r.SourceType, SourceRef: r.SourceRef,
			TrustTier: r.TrustTier, Summary: r.Summary})
	}

	// Human confirmations at the stage they were given.
	for _, e := range events {
		if e.Type != "work.confirmed" {
			continue
		}
		var p struct {
			Verdict string `json:"verdict"`
			StageID string `json:"stage_id"`
			Comment string `json:"comment"`
		}
		if json.Unmarshal(e.Payload, &p) != nil || p.StageID == "" {
			continue
		}
		payload, _ := json.Marshal(map[string]string{"verdict": strings.ToLower(p.Verdict), "comment": p.Comment})
		c.Human = append(c.Human, HumanStep{Stage: p.StageID, Command: work.CmdConfirm, Payload: payload, Actor: firstNonEmpty(e.ActorID, "bench-human")})
	}

	// Horizon: the recorded duration plus slack.
	end := time.Now().UTC()
	if w.ClosedAt != nil {
		end = *w.ClosedAt
	}
	hz := end.Sub(start) + 30*time.Minute
	hz = min(max(hz, time.Hour), MaxHorizon)
	c.Horizon = cpkit.Duration(hz)
	if tick := hz / maxTicks; tick > DefaultTick {
		c.Tick = cpkit.Duration(tick.Round(time.Second) + time.Second)
	}
	if w.Status.Terminal() {
		c.Expect.Status = w.Status
	}
	return c, c.Normalize()
}

// attribute maps hypotheses and proposed actions to the runs that produced
// them. Actions carry their stage: the proposal came from the latest run of
// that stage's from_proposal stage started before the action. Hypotheses
// name their proponents (run ids or agent roles); unattributed ones go to
// the first successful run.
func attribute(tpl work.Template, links []work.RunLink, hyps []evidence.Hypothesis, acts []action.Record) (map[string][]evidence.Hypothesis, map[string]*action.Record) {
	hypsOf := map[string][]evidence.Hypothesis{}
	actOf := map[string]*action.Record{}
	for i := range acts {
		a := &acts[i]
		i, ok := tpl.Index(a.StageID)
		if !ok || tpl.Stages[i].Action == nil || tpl.Stages[i].Action.FromProposal == "" {
			continue
		}
		from := tpl.Stages[i].Action.FromProposal
		var pick string
		for _, l := range links {
			if l.StageID == from && !l.CreatedAt.After(a.CreatedAt) {
				pick = l.RunID
			}
		}
		if pick != "" && actOf[pick] == nil {
			actOf[pick] = a
		}
	}
	first := ""
	for _, l := range links {
		if l.StageID != "" && !failed(l) {
			first = l.RunID
			break
		}
	}
	for _, h := range hyps {
		placed := false
		for _, l := range links {
			if slices.Contains(h.Proponents, l.RunID) || (l.AgentRole != "" && slices.Contains(h.Proponents, l.AgentRole)) || h.CreatedBy == l.RunID {
				hypsOf[l.RunID] = append(hypsOf[l.RunID], h)
				placed = true
			}
		}
		if !placed && first != "" {
			hypsOf[first] = append(hypsOf[first], h)
		}
	}
	return hypsOf, actOf
}

func failed(l work.RunLink) bool {
	switch strings.ToUpper(firstNonEmpty(l.Outcome, l.Status)) {
	case "FAILED", "CANCELLED", "TIMED_OUT", "REJECTED":
		return true
	}
	return false
}

// runResult rebuilds the result document of one run from the ledgers.
func runResult(l work.RunLink, hyps []evidence.Hypothesis, act *action.Record) json.RawMessage {
	doc := map[string]any{"answer": "recorded run " + l.RunID}
	var hs []map[string]any
	for _, h := range hyps {
		status := "UNVERIFIED"
		switch h.Status {
		case evidence.HypSupported:
			status = "VERIFIED"
		case evidence.HypRefuted:
			status = "REFUTED"
		}
		hs = append(hs, map[string]any{"id": h.Key, "statement": h.Statement, "category": h.Category, "status": status,
			"supports": h.Supporting, "refutes": h.Refuting})
	}
	if len(hs) > 0 {
		doc["hypotheses"] = hs
	}
	if act != nil {
		args := act.Args
		if len(args) == 0 {
			args = json.RawMessage(`{}`)
		}
		doc["proposed_action"] = map[string]any{"action": act.ActionName, "target": act.TargetRef, "args": args}
	}
	if l.CostMicros > 0 {
		doc["loop"] = map[string]any{"cost_usd": float64(l.CostMicros) / 1e6}
	}
	b, _ := json.Marshal(doc)
	return b
}

// Comparison is a replay against the recorded Work.
type Comparison struct {
	WorkID   string             `json:"work_id"`
	Original workeval.Scorecard `json:"original"`
	Replay   Result             `json:"replay"`
	Diffs    []string           `json:"diffs,omitempty"`
	Match    bool               `json:"match"`
}

// Replay records a Work, runs it, and compares the replay with the
// original on the decision-relevant dimensions.
func Replay(ctx context.Context, r *Runner, src Source, eval *workeval.Evaluator, tenantID, workID string) (Comparison, error) {
	cmp := Comparison{WorkID: workID}
	orig, err := eval.Score(ctx, tenantID, workID)
	if err != nil {
		return cmp, err
	}
	cmp.Original = orig
	c, err := Record(ctx, src, tenantID, workID)
	if err != nil {
		return cmp, err
	}
	res, err := r.Run(ctx, c)
	if err != nil {
		return cmp, err
	}
	cmp.Replay = res
	cmp.Diffs = Diff(orig, res.Scorecard)
	cmp.Match = len(cmp.Diffs) == 0 && res.Passed
	return cmp, nil
}

// Diff lists the decision-relevant differences of two scorecards.
func Diff(a, b workeval.Scorecard) []string {
	var d []string
	if a.Status != b.Status {
		d = append(d, fmt.Sprintf("status %s -> %s", a.Status, b.Status))
	}
	if a.Pass != b.Pass {
		d = append(d, fmt.Sprintf("pass %v -> %v", a.Pass, b.Pass))
	}
	ga, gb := slices.Clone(a.FailedGates), slices.Clone(b.FailedGates)
	slices.Sort(ga)
	slices.Sort(gb)
	if !slices.Equal(ga, gb) {
		d = append(d, fmt.Sprintf("failed gates %v -> %v", ga, gb))
	}
	if vb, va := boolp(b.Verified), boolp(a.Verified); va != vb {
		d = append(d, fmt.Sprintf("verified %s -> %s", va, vb))
	}
	for _, x := range []struct {
		name string
		a, b int
	}{{"actions proposed", a.ActionsProposed, b.ActionsProposed}, {"actions applied", a.ActionsApplied, b.ActionsApplied},
		{"rule violations", a.RuleViolations, b.RuleViolations}, {"repeated unknown effects", a.RepeatedUnknownEffects, b.RepeatedUnknownEffects},
		{"unresolved effects", a.UnresolvedEffects, b.UnresolvedEffects}, {"supported conclusions", a.SupportedConclusions, b.SupportedConclusions},
		{"objectives met", a.RequiredMet, b.RequiredMet}} {
		if x.a != x.b {
			d = append(d, fmt.Sprintf("%s %d -> %d", x.name, x.a, x.b))
		}
	}
	return d
}

func boolp(b *bool) string {
	if b == nil {
		return "none"
	}
	if *b {
		return "true"
	}
	return "false"
}
