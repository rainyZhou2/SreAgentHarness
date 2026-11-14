// Package postmortem builds factual reports from persisted decisions and outcome ledgers.
package postmortem

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workeval"
)

// Ledger is what the report reads (cpsql.Store implements it).
type Ledger interface {
	workeval.Ledger
	ListWorkEvents(ctx context.Context, tenantID, workID string, afterSeq int64, limit int) ([]work.Event, error)
}

// Bounds keep the report small enough to fit a goal description.
const (
	MaxTimeline = 200
	maxText     = 240
)

// Entry is one timeline line.
type Entry struct {
	At     time.Time `json:"at"`
	Source string    `json:"source"` // work | action | outcome
	Type   string    `json:"type"`
	Text   string    `json:"text"`
	Ref    string    `json:"ref,omitempty"`
	// Count > 1 when identical consecutive entries (a re-sent alert) were
	// collapsed; Until is the last one.
	Count int        `json:"count,omitempty"`
	Until *time.Time `json:"until,omitempty"`
}

// Cause is a supported conclusion with its evidence.
type Cause struct {
	Statement    string   `json:"statement"`
	Category     string   `json:"category,omitempty"`
	EvidenceRefs []string `json:"evidence_refs"`
}

// ActionLine summarizes one controlled action.
type ActionLine struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Target string `json:"target"`
	Risk   string `json:"risk"`
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
}

// Outcome is the latest evaluation of the Work's success spec.
type Outcome struct {
	Verdict    string   `json:"verdict"`
	Verified   bool     `json:"verified"` // SATISFIED and the Work succeeded
	Objectives []string `json:"objectives,omitempty"`
}

// FollowUp is a derived follow-up item.
type FollowUp struct {
	Title    string `json:"title"`
	Why      string `json:"why"`
	Priority string `json:"priority"` // high | medium | low
}

// Report is the deterministic postmortem.
type Report struct {
	WorkID          string     `json:"work_id"`
	Title           string     `json:"title"`
	Kind            string     `json:"kind"`
	Status          string     `json:"status"`
	Subjects        []string   `json:"subjects"`
	OpenedAt        time.Time  `json:"opened_at"`
	ClosedAt        *time.Time `json:"closed_at,omitempty"`
	DurationSeconds float64    `json:"duration_seconds"`
	Trigger         string     `json:"trigger"`

	Timeline  []Entry `json:"timeline"`
	Truncated bool    `json:"timeline_truncated,omitempty"`

	// RootCause is present only for one explicitly confirmed root-cause decision.
	Decisions      []evidence.DecisionVerdict `json:"decisions,omitempty"`
	RootCause      *Cause                     `json:"root_cause,omitempty"`
	Contributing   []Cause                    `json:"contributing_factors,omitempty"`
	OpenHypotheses []string                   `json:"open_hypotheses,omitempty"`
	Refuted        []string                   `json:"refuted_hypotheses,omitempty"`
	Actions        []ActionLine               `json:"actions,omitempty"`
	Outcome        *Outcome                   `json:"outcome,omitempty"`

	Scorecard *workeval.Scorecard `json:"scorecard,omitempty"`
	Gaps      []string            `json:"gaps,omitempty"`
	FollowUps []FollowUp          `json:"follow_ups,omitempty"`
}

// Builder builds reports.
type Builder struct {
	Ledger Ledger
	// Eval adds the scorecard and its gaps (optional).
	Eval *workeval.Evaluator
	// DiagnosisTarget is the time-to-diagnosis beyond which a detection
	// gap is reported (default 15m).
	DiagnosisTarget time.Duration
}

// Build reads the ledgers of one Work.
func (b *Builder) Build(ctx context.Context, tenantID, workID string) (Report, error) {
	l := b.Ledger
	w, err := l.GetWork(ctx, tenantID, workID)
	if err != nil {
		return Report{}, err
	}
	r := Report{WorkID: w.ID, Title: w.Title, Kind: string(w.Kind), Status: string(w.Status), Subjects: w.Goal.SubjectRefs,
		OpenedAt: w.CreatedAt, ClosedAt: w.ClosedAt}
	if w.ClosedAt != nil {
		r.DurationSeconds = w.ClosedAt.Sub(w.CreatedAt).Seconds()
	}
	events, err := allEvents(ctx, l, tenantID, workID)
	if err != nil {
		return r, err
	}
	acts, err := l.ListActions(ctx, tenantID, workID)
	if err != nil {
		return r, err
	}
	evals, err := l.ListOutcomes(ctx, tenantID, workID, 50)
	if err != nil {
		return r, err
	}
	hyps, err := l.ListHypotheses(ctx, tenantID, workID)
	if err != nil {
		return r, err
	}
	r.Trigger = trigger(w, events)
	r.timeline(events, acts, evals)
	r.causes(w, hyps)
	for _, a := range acts {
		r.Actions = append(r.Actions, ActionLine{ID: a.ID, Name: a.ActionName, Target: a.TargetRef, Risk: string(a.Risk),
			State: string(a.State), Reason: clip(a.Reason)})
	}
	if len(evals) > 0 {
		e := evals[0] // newest first
		o := &Outcome{Verdict: string(e.Verdict), Verified: e.Verdict == outcome.Satisfied && w.Status == work.StatusSucceeded}
		for _, ob := range e.Objectives {
			line := fmt.Sprintf("%s: %s", ob.ID, ob.Verdict)
			if ob.Value != nil {
				line += fmt.Sprintf(" (value %g)", *ob.Value)
			}
			if ob.Optional {
				line += " [optional]"
			}
			if ob.Reason != "" {
				line += " - " + clip(ob.Reason)
			}
			o.Objectives = append(o.Objectives, line)
		}
		r.Outcome = o
	}
	if b.Eval != nil {
		sc, err := b.Eval.Score(ctx, tenantID, workID)
		if err != nil {
			return r, err
		}
		r.Scorecard = &sc
	}
	r.derive(b.target(), evals)
	return r, nil
}

func (b *Builder) target() time.Duration {
	if b.DiagnosisTarget > 0 {
		return b.DiagnosisTarget
	}
	return 15 * time.Minute
}

func allEvents(ctx context.Context, l Ledger, tenantID, workID string) ([]work.Event, error) {
	var out []work.Event
	var after int64
	for {
		page, err := l.ListWorkEvents(ctx, tenantID, workID, after, 200)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < 200 || len(out) >= 5000 {
			return out, nil
		}
		after = page[len(page)-1].Seq
	}
}

// trigger is the first attached signal, else the creation reason.
func trigger(w work.Work, events []work.Event) string {
	for _, e := range events {
		if e.Type == "work.event_attached" {
			return "signal: " + summarize(e.Payload)
		}
	}
	for _, e := range events {
		if e.Type == "work.created" {
			s := "created by " + firstNonEmpty(e.ActorType, "system")
			if e.ActorID != "" {
				s += " " + e.ActorID
			}
			return s + ": " + firstNonEmpty(w.Goal.Intent, w.Title)
		}
	}
	return firstNonEmpty(w.Goal.Intent, w.Title)
}

func (r *Report) timeline(events []work.Event, acts []action.Record, evals []outcome.Evaluation) {
	var tl []Entry
	for _, e := range events {
		switch e.Type {
		case "work.wake", "work.command_applied":
			continue // bookkeeping
		}
		tl = append(tl, Entry{At: e.OccurredAt, Source: "work", Type: strings.TrimPrefix(e.Type, "work."), Text: summarize(e.Payload),
			Ref: e.CausationID})
	}
	for _, a := range acts {
		tl = append(tl, Entry{At: a.CreatedAt, Source: "action", Type: "proposed",
			Text: fmt.Sprintf("%s on %s (%s)", a.ActionName, a.TargetRef, a.Risk), Ref: a.ID})
		if a.DispatchedAt != nil {
			tl = append(tl, Entry{At: *a.DispatchedAt, Source: "action", Type: "dispatched", Text: a.ActionName, Ref: a.ID})
		}
		if a.State.Terminal() {
			tl = append(tl, Entry{At: a.UpdatedAt, Source: "action", Type: strings.ToLower(string(a.State)),
				Text: strings.TrimSpace(a.ActionName + " " + clip(a.Reason)), Ref: a.ID})
		}
	}
	// Evaluations: only verdict changes, oldest first.
	last := ""
	for i := len(evals) - 1; i >= 0; i-- {
		e := evals[i]
		if string(e.Verdict) == last {
			continue
		}
		last = string(e.Verdict)
		tl = append(tl, Entry{At: e.EvaluatedAt, Source: "outcome", Type: "verdict", Text: string(e.Verdict), Ref: e.ID})
	}
	sort.SliceStable(tl, func(i, j int) bool { return tl[i].At.Before(tl[j].At) })
	tl = collapse(tl)
	if len(tl) > MaxTimeline {
		// Keep the beginning and the end; the middle is the least telling.
		head, tail := tl[:MaxTimeline/2], tl[len(tl)-MaxTimeline/2:]
		tl = append(slices.Clone(head), tail...)
		r.Truncated = true
	}
	r.Timeline = tl
}

// collapse merges identical consecutive entries.
func collapse(tl []Entry) []Entry {
	out := tl[:0]
	for _, e := range tl {
		if n := len(out); n > 0 {
			p := &out[n-1]
			if p.Source == e.Source && p.Type == e.Type && p.Text == e.Text {
				if p.Count == 0 {
					p.Count = 1
				}
				p.Count++
				at := e.At
				p.Until = &at
				continue
			}
		}
		out = append(out, e)
	}
	return out
}

func (r *Report) causes(w work.Work, hyps []evidence.Hypothesis) {
	scope := evidence.DecisionScope{TenantID: w.TenantID, WorkID: w.ID, Generation: w.Generation}
	var roots []Cause
	for _, h := range hyps {
		d := h.Decision
		if !h.CurrentDecisionFor(scope) {
			r.OpenHypotheses = append(r.OpenHypotheses, clip(h.Statement)+" (unadjudicated or obsolete decision)")
			continue
		}
		r.Decisions = append(r.Decisions, *d)
		switch {
		case d.Verdict == evidence.DecisionConfirmed:
			c := Cause{Statement: clip(h.Statement), Category: h.Category, EvidenceRefs: d.AcceptedEvidenceRefs}
			switch h.Category {
			case evidence.CatRootCause:
				roots = append(roots, c)
			case evidence.CatContributing:
				r.Contributing = append(r.Contributing, c)
			default:
				r.OpenHypotheses = append(r.OpenHypotheses, clip(h.Statement)+" (confirmed finding; causal role unclassified)")
			}
		case d.IndependentRefuteCount > 0 && d.IndependentSupportCount == 0:
			r.Refuted = append(r.Refuted, clip(h.Statement))
		default:
			r.OpenHypotheses = append(r.OpenHypotheses, fmt.Sprintf("%s (%s)", clip(h.Statement), d.Verdict))
		}
	}
	if len(roots) == 1 {
		r.RootCause = &roots[0]
	} else if len(roots) > 1 {
		for _, c := range roots {
			r.OpenHypotheses = append(r.OpenHypotheses, c.Statement+" (multiple confirmed root-cause candidates; requires adjudication)")
		}
	}
	sort.Strings(r.OpenHypotheses)
	sort.Strings(r.Refuted)
}

// derive turns the scorecard and ledgers into gaps and follow-ups.
func (r *Report) derive(target time.Duration, evals []outcome.Evaluation) {
	add := func(prio, title, why string) {
		for _, f := range r.FollowUps {
			if f.Title == title {
				return
			}
		}
		r.FollowUps = append(r.FollowUps, FollowUp{Title: title, Why: why, Priority: prio})
	}
	gap := func(s string) { r.Gaps = append(r.Gaps, s) }

	if r.RootCause == nil {
		gap("root cause unknown: no unique confirmed root-cause decision")
		add("high", "Close the diagnosis gap", "the root cause is unknown; add the telemetry or evidence that would confirm or refute: "+
			joinMax(r.OpenHypotheses, 3))
	}
	if r.Outcome == nil {
		gap("no machine-verifiable success spec was evaluated")
		add("medium", "Add a machine-checkable success spec for this kind of Work", "the outcome was never evaluated against objectives")
	} else if !r.Outcome.Verified {
		gap("the outcome was not verified (latest verdict " + r.Outcome.Verdict + ")")
	}
	if len(evals) > 0 {
		for _, ob := range evals[0].Objectives {
			if !ob.Optional && ob.Verdict == outcome.NotSatisfied {
				add("high", "Restore objective "+ob.ID, "required objective "+ob.ID+" is not satisfied at close")
			}
		}
	}
	for _, a := range r.Actions {
		if a.State == string(action.StateEffectUnknown) {
			add("high", "Reconcile "+a.Name+" on "+a.Target, "its effect is unknown; it must be inspected and resolved, never replayed blindly")
		}
	}
	sc := r.Scorecard
	if sc == nil {
		return
	}
	if sc.TimeToFirstActionProposalSeconds != nil && *sc.TimeToFirstActionProposalSeconds > target.Seconds() {
		gap(fmt.Sprintf("response gap: first action proposed after %s (target %s)",
			time.Duration(*sc.TimeToFirstActionProposalSeconds*float64(time.Second)).Round(time.Second), target))
		add("medium", "Shorten time to first action proposal", "review the delay between Work creation and the first action proposal")
	}
	if sc.RuleViolations > 0 {
		gap(fmt.Sprintf("%d proposed actions were refused by safety rules", sc.RuleViolations))
		add("medium", "Review refused actions", "the agent proposed actions the guards refused: "+joinMax(sc.ViolationReasons, 3))
	}
	if sc.RepeatedUnknownEffects > 0 {
		gap(fmt.Sprintf("%d actions were retried after an unknown effect", sc.RepeatedUnknownEffects))
		add("high", "Make the affected adapters idempotent and inspectable", "an action was re-proposed while an earlier attempt had an unknown effect")
	}
	if sc.OutOfScopeActions > 0 {
		gap(fmt.Sprintf("%d actions targeted resources outside the Work", sc.OutOfScopeActions))
	}
	if len(sc.SecondaryIncidents) > 0 {
		gap("secondary incidents followed an action: " + joinMax(sc.SecondaryIncidents, 5))
		add("high", "Investigate the blast radius of the mitigation", "new incidents opened on the target after the action")
	}
	if len(sc.UngroundedRefs) > 0 {
		gap("conclusions lack a current supported decision: " + joinMax(sc.UngroundedRefs, 5))
		add("low", "Ground conclusions in T1-T3 evidence", "supported hypotheses rely on evidence the ledger cannot back")
	}
	if sc.Runs > 0 && sc.ToolCalls > 40*sc.Runs {
		gap(fmt.Sprintf("investigation used %d tool calls over %d runs", sc.ToolCalls, sc.Runs))
	}
}

// Markdown renders the report.
func Markdown(r Report) string {
	var b strings.Builder
	p := func(format string, a ...any) { fmt.Fprintf(&b, format, a...) }
	p("# Postmortem: %s\n\n", firstNonEmpty(r.Title, r.WorkID))
	p("- Work: `%s` (%s, %s)\n", r.WorkID, r.Kind, r.Status)
	p("- Subjects: %s\n", strings.Join(r.Subjects, ", "))
	p("- Opened: %s\n", r.OpenedAt.UTC().Format(time.RFC3339))
	if r.ClosedAt != nil {
		p("- Closed: %s (duration %s)\n", r.ClosedAt.UTC().Format(time.RFC3339),
			time.Duration(r.DurationSeconds*float64(time.Second)).Round(time.Second))
	}
	p("- Trigger: %s\n\n", r.Trigger)

	p("## Outcome\n\n")
	if r.Outcome == nil {
		p("Not evaluated.\n\n")
	} else {
		v := "not verified"
		if r.Outcome.Verified {
			v = "verified"
		}
		p("Latest verdict **%s** (%s).\n\n", r.Outcome.Verdict, v)
		for _, o := range r.Outcome.Objectives {
			p("- %s\n", o)
		}
		p("\n")
	}

	p("## Root cause\n\n")
	if r.RootCause == nil {
		p("Unknown. No unique confirmed root-cause decision.\n\n")
	} else {
		p("%s\n\nEvidence: %s\n\n", r.RootCause.Statement, strings.Join(r.RootCause.EvidenceRefs, ", "))
	}
	if len(r.Contributing) > 0 {
		p("### Contributing factors\n\n")
		for _, c := range r.Contributing {
			p("- %s (evidence: %s)\n", c.Statement, strings.Join(c.EvidenceRefs, ", "))
		}
		p("\n")
	}
	if len(r.OpenHypotheses) > 0 {
		p("### Open hypotheses\n\n")
		for _, h := range r.OpenHypotheses {
			p("- %s\n", h)
		}
		p("\n")
	}
	if len(r.Refuted) > 0 {
		p("### Ruled out\n\n")
		for _, h := range r.Refuted {
			p("- %s\n", h)
		}
		p("\n")
	}

	for _, d := range r.Decisions {
		if d.Verdict == evidence.DecisionConfirmed {
			continue
		}
		p("### Decision %s: %s\n\n", d.DecisionKey, d.Verdict)
		if len(d.AcceptedEvidenceRefs) > 0 {
			p("Accepted evidence: %s\n\n", strings.Join(d.AcceptedEvidenceRefs, ", "))
		}
		if len(d.UnresolvedConflicts) > 0 {
			p("Unresolved conflicts: %s\n\n", strings.Join(d.UnresolvedConflicts, ", "))
		}
		for _, check := range d.RequiredNextChecks {
			p("- Next check: %s\n", check)
		}
		p("\n")
	}

	if len(r.Actions) > 0 {
		p("## Actions\n\n| Action | Target | Risk | State | Reason |\n|---|---|---|---|---|\n")
		for _, a := range r.Actions {
			p("| %s | %s | %s | %s | %s |\n", cell(a.Name), cell(a.Target), a.Risk, a.State, cell(a.Reason))
		}
		p("\n")
	}

	p("## Timeline (UTC)\n\n")
	for _, e := range r.Timeline {
		line := fmt.Sprintf("- %s [%s] %s", e.At.UTC().Format("2006-01-02 15:04:05"), e.Source, e.Type)
		if e.Text != "" {
			line += ": " + e.Text
		}
		if e.Count > 1 && e.Until != nil {
			line += fmt.Sprintf(" (×%d until %s)", e.Count, e.Until.UTC().Format("15:04:05"))
		}
		p("%s\n", line)
	}
	if r.Truncated {
		p("- (timeline truncated)\n")
	}
	p("\n")

	if sc := r.Scorecard; sc != nil {
		p("## Scorecard\n\n")
		p("- Pass: %v", sc.Pass)
		if len(sc.FailedGates) > 0 {
			p(" (failed gates: %s)", strings.Join(sc.FailedGates, ", "))
		}
		p("\n- Grounding: %.2f, runs: %d, tool calls: %d, cost: $%.2f\n", sc.Grounding(), sc.Runs, sc.ToolCalls, sc.CostUSD)
		p("- Time to first mitigation: %.0fs, duration: %.0fs\n\n", sc.DiagnosisSeconds, sc.DurationSeconds)
	}
	if len(r.Gaps) > 0 {
		p("## Gaps\n\n")
		for _, g := range r.Gaps {
			p("- %s\n", g)
		}
		p("\n")
	}
	if len(r.FollowUps) > 0 {
		p("## Follow-ups\n\n")
		for _, f := range r.FollowUps {
			p("- [%s] **%s**: %s\n", f.Priority, f.Title, f.Why)
		}
	}
	return b.String()
}

// Brief is Markdown bounded to n bytes (cut at a line boundary), for use as
// a goal description.
func Brief(r Report, n int) string {
	s := Markdown(r)
	if len(s) <= n {
		return s
	}
	const marker = "\n... (report truncated; GET /v1/works/{id}/postmortem has the full report)\n"
	cut := s[:max(0, n-len(marker))]
	if i := strings.LastIndexByte(cut, '\n'); i > 0 {
		cut = cut[:i]
	}
	return cut + marker
}

// summarize renders a few well-known payload fields deterministically.
func summarize(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return clip(string(raw))
	}
	keys := []string{"type", "subject_ref", "title", "stage_id", "from", "to", "status", "state", "decision", "reason", "action_id", "error"}
	var parts []string
	for _, k := range keys {
		v, ok := m[k]
		if !ok || v == nil || v == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	return clip(strings.Join(parts, " "))
}

func clip(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= maxText {
		return s
	}
	cut := maxText
	for cut > 0 && !utf8.RuneStart(s[cut]) { // do not split a rune
		cut--
	}
	return s[:cut] + "…"
}

func cell(s string) string { return strings.ReplaceAll(s, "|", `\|`) }

func joinMax(xs []string, n int) string {
	if len(xs) == 0 {
		return "none recorded"
	}
	if len(xs) > n {
		return strings.Join(xs[:n], "; ") + fmt.Sprintf("; +%d more", len(xs)-n)
	}
	return strings.Join(xs, "; ")
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if x != "" {
			return x
		}
	}
	return ""
}
