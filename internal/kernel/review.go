package kernel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/reasoning"
	"go-sre-harness/internal/verify"
)

// DefaultMaxBacktracks is how many times a conclusion the independent
// verifier did not accept is handed back to the agent in one run.
const DefaultMaxBacktracks = 1

// reviewDoc is the argument (and result) of a conclusion review step.
type reviewDoc struct {
	Verdict      string   `json:"verdict"`
	Issues       []string `json:"issues"`
	Agreement    float64  `json:"agreement,omitempty"`
	Alternatives []string `json:"alternatives,omitempty"`
	Instruction  string   `json:"instruction"`
}

const reviewInstruction = "An independent verifier did not accept the proposed conclusion. Do not repeat it unchanged. " +
	"Go back to the hypotheses: for each issue, either gather the missing evidence with one discriminating check, " +
	"revise or refute the hypothesis, or conclude with verdict UNVERIFIED and say what remains uncertain."

// conclusionClaims extracts the settled claims of a final decision, with
// citations resolved against what the model was shown.
func conclusionClaims(d domain.Decision, refs *cognitive.RefSet) ([]verify.Claim, bool) {
	resolve := func(raws []string) []string {
		var out []string
		seen := map[string]bool{}
		for _, r := range raws {
			if c, ok := refs.Resolve(r); ok && !seen[c] {
				seen[c] = true
				out = append(out, c)
			}
		}
		return out
	}
	var claims []verify.Claim
	for i, h := range d.Hypotheses {
		st := strings.ToUpper(strings.TrimSpace(h.Status))
		if st != "VERIFIED" && st != "REFUTED" {
			continue
		}
		id := strings.TrimSpace(h.ID)
		if id == "" {
			id = fmt.Sprintf("h%d", i+1)
		}
		claims = append(claims, verify.Claim{ID: id, Kind: "hypothesis", Statement: h.Statement, Status: st,
			Supports: resolve(h.Supports), Refutes: resolve(h.Refutes)})
	}
	verified := false
	if d.Thought != nil && d.Thought.Verification != nil {
		v := d.Thought.Verification
		st := strings.ToUpper(strings.TrimSpace(v.Verdict))
		verified = st == "VERIFIED"
		if st == "VERIFIED" || st == "REFUTED" {
			stmt := strings.TrimSpace("expected: " + v.Expected + "; observed: " + v.Observed)
			claims = append(claims, verify.Claim{ID: "verification", Kind: "verification", Statement: stmt, Status: st,
				Supports: resolve(d.EvidenceRefs)})
		}
	}
	return claims, verified
}

// conclusionEvidence indexes the citable items of a reasoning input by
// canonical ref.
func conclusionEvidence(in cognitive.ReasoningInput) map[string]verify.Evidence {
	ev := map[string]verify.Evidence{}
	for _, o := range in.Observations {
		text := string(o.Content)
		if o.Note != "" {
			text = strings.TrimSpace(text + "\nnote: " + o.Note)
		}
		r := cognitive.ObsPartRef(o.StepSeq, o.Part)
		ev[r] = verify.Evidence{Ref: r, Kind: "observation", OK: o.Outcome == "ok", Text: o.Tool + " → " + text}
		if o.Part > 0 {
			// A batch step cited as a whole: all of its calls, ok if any is.
			whole := cognitive.ObsRef(o.StepSeq)
			cur := ev[whole]
			cur.Ref, cur.Kind = whole, "observation"
			cur.OK = cur.OK || o.Outcome == "ok"
			cur.Text = strings.TrimSpace(cur.Text + "\n[" + r + "] " + o.Tool + " → " + text)
			ev[whole] = cur
		}
	}
	for i := int64(1); i < in.CompactedBefore; i++ {
		// Compacted steps (and their batch parts) are judged on the summary.
		for k := 0; k <= MaxBatchParts; k++ {
			r := cognitive.ObsPartRef(i, k)
			if _, ok := ev[r]; !ok {
				ev[r] = verify.Evidence{Ref: r, Kind: "observation", OK: true, Text: "(summarized) " + in.Summary}
			}
		}
	}
	for _, e := range in.Evidence {
		r := fmt.Sprintf("kv:%s@%d", e.Key, e.Version)
		ev[r] = verify.Evidence{Ref: r, Kind: "evidence", OK: true, Text: e.Content}
	}
	for _, m := range append(append([]cognitive.MemoryRef(nil), in.Memories...), in.RecalledMemories...) {
		r := fmt.Sprintf("memory:%s@%d", m.ID, m.Version)
		if _, ok := ev[r]; !ok {
			ev[r] = verify.Evidence{Ref: r, Kind: "memory", OK: true, Text: strings.TrimSpace(m.Title + "\n" + m.Content)}
		}
	}
	return ev
}

// reviewOutcome is what the kernel does with a final decision.
type reviewOutcome struct {
	report    *verify.Report
	backtrack *domain.PlannedStep
	thought   *domain.Thought
}

// reviewConclusion runs the independent verifier on a final task decision.
// A conclusion it does not accept is handed back to the agent through a
// kernel-owned review step while backtracks and steps remain; otherwise the
// run completes with the report attached.
func (w *Worker) reviewConclusion(ctx context.Context, lease domain.Lease, in cognitive.ReasoningInput,
	d domain.Decision) (reviewOutcome, error) {
	if w.cfg.Verifier == nil || d.Kind != domain.DecisionFinal || d.Intent != domain.IntentTask ||
		(in.FlowNode != nil && in.FlowNode.Type == "tool") {
		return reviewOutcome{}, nil
	}
	refs := cognitive.NewRefSet(in)
	claims, verified := conclusionClaims(d, refs)
	if len(claims) == 0 && !verified && d.Consistency == nil {
		return reviewOutcome{}, nil
	}
	vctx, cancel := context.WithTimeout(ctx, w.verifyTimeout())
	defer cancel()
	v := w.cfg.Verifier.Bind(gw.Scope{TenantID: lease.TenantID, RunID: lease.RunID, StepID: lease.StepID,
		StepAttemptID: lease.AttemptID, WorkerID: w.cfg.ID, FenceToken: lease.FenceToken,
		ContextSnapshotID: lease.ContextSnapshotID})
	rep := v.Verify(vctx, in.Goal, claims, conclusionEvidence(in), verified)
	if c := d.Consistency; c != nil {
		rep.Agreement, rep.Alternatives = c.Agreement, c.Alternatives
		if c.Agreement > 0 && c.Agreement < w.minAgreement() && rep.Verdict == verify.Supported {
			rep.Verdict = verify.Insufficient
			rep.Claims = append(rep.Claims, verify.ClaimResult{ClaimID: "consistency", Kind: "verification",
				Status: "VERIFIED", Verdict: verify.Insufficient, Checker: "self_consistency",
				Reason: fmt.Sprintf("only %.0f%% of %d sampled conclusions agreed", c.Agreement*100, c.Samples)})
		}
	}
	out := reviewOutcome{report: &rep, thought: verifierThought(rep)}
	if rep.Verdict == verify.Supported || !w.canBacktrack(in) {
		return out, nil
	}
	doc := reviewDoc{Verdict: rep.Verdict, Issues: rep.Issues(), Agreement: rep.Agreement,
		Alternatives: rep.Alternatives, Instruction: reviewInstruction}
	call := domain.ToolCall{Name: verify.ReviewToolName, Args: mustJSON(doc),
		Followup: &domain.ToolFollowup{Role: domain.FollowupReview, OfStep: lease.StepSeq}}
	rules, err := w.rules(ctx, lease)
	if err != nil {
		return reviewOutcome{}, err
	}
	step, pd, err := w.followupStep(ctx, lease, rules, call, false)
	if err != nil {
		return reviewOutcome{}, err
	}
	if pd.Verdict != policy.Allow {
		return out, nil // a review never waits for approval: complete with the report
	}
	out.backtrack = &step
	return out, nil
}

// canBacktrack reports whether another review fits the run's budgets.
func (w *Worker) canBacktrack(in cognitive.ReasoningInput) bool {
	limit := w.cfg.MaxBacktracks
	if limit == 0 {
		limit = DefaultMaxBacktracks
	}
	if limit < 0 {
		return false
	}
	used := 0
	for _, o := range in.Observations {
		if o.Tool == verify.ReviewToolName {
			used++
		}
	}
	if used >= limit {
		return false
	}
	// The review step and at least two more reasoning steps must fit.
	return in.MaxSteps <= 0 || in.StepSeq+3 <= int64(in.MaxSteps)
}

func (w *Worker) verifyTimeout() time.Duration {
	if w.cfg.VerifyTimeout > 0 {
		return w.cfg.VerifyTimeout
	}
	return 60 * time.Second
}

func (w *Worker) minAgreement() float64 {
	if w.cfg.MinAgreement > 0 {
		return w.cfg.MinAgreement
	}
	return 0.6
}

// verifierThought records the independent verdict. Scoring reads it by the
// checker attribute, separately from the agent's own verification.
func verifierThought(rep verify.Report) *domain.Thought {
	status := domain.ThoughtUnverified
	switch rep.Verdict {
	case verify.Supported:
		status = domain.ThoughtVerified
	case verify.Contradicted:
		status = domain.ThoughtRefuted
	}
	claims := make([]map[string]any, 0, len(rep.Claims))
	for _, c := range rep.Claims {
		claims = append(claims, map[string]any{"id": c.ClaimID, "kind": c.Kind, "status": c.Status,
			"verdict": c.Verdict, "checker": c.Checker, "reason": truncate(c.Reason, 300)})
	}
	attrs := map[string]any{reasoning.CheckerAttr: reasoning.IndependentChecker, "verdict": rep.Verdict,
		"checker_layers": rep.Checker, "claims": claims}
	if rep.Agreement > 0 {
		attrs["agreement"] = rep.Agreement
	}
	if rep.Error != "" {
		attrs["error"] = truncate(rep.Error, 300)
	}
	summary := "independent verifier: " + rep.Verdict
	if issues := rep.Issues(); len(issues) > 0 {
		summary += " — " + strings.Join(issues, "; ")
	}
	return &domain.Thought{Kind: domain.ThoughtVerification, Summary: truncate(summary, 1000), Status: status,
		Structured: true, Attrs: attrs}
}

// CalibrationSource supplies a tenant profile's confidence calibrator.
type CalibrationSource interface {
	Calibrator(ctx context.Context, tenantID, profileID string) (*reasoning.Calibrator, error)
}

// calibrator returns the run profile's calibrator, or nil. Failures only
// lose the calibration note; they never fail the step.
func (w *Worker) calibrator(ctx context.Context, lease domain.Lease) *reasoning.Calibrator {
	if w.cfg.Calibration == nil || lease.ProfileID == "" {
		return nil
	}
	c, err := w.cfg.Calibration.Calibrator(ctx, lease.TenantID, lease.ProfileID)
	if err != nil {
		w.log.Warn("calibration", "err", err)
		return nil
	}
	return c
}

// ledger merges the hypotheses of earlier decisions by id (or statement).
type ledger struct {
	order []string
	byKey map[string]*cognitive.LedgerEntry
}

func newLedger() *ledger { return &ledger{byKey: map[string]*cognitive.LedgerEntry{}} }

// MaxLedgerEntries bounds the ledger shown to the model.
const MaxLedgerEntries = 12

func (l *ledger) add(step int64, hs []domain.Hypothesis) {
	for _, h := range hs {
		stmt := strings.TrimSpace(h.Statement)
		id := ledgerID(h.ID)
		key := "id:" + strings.ToLower(id)
		if id == "" {
			if stmt == "" {
				continue
			}
			key = "s:" + strings.ToLower(strings.Join(strings.Fields(stmt), " "))
		}
		e, ok := l.byKey[key]
		if !ok {
			if len(l.order) >= 4*MaxLedgerEntries {
				continue
			}
			if id == "" {
				id = fmt.Sprintf("h%d", len(l.order)+1)
			}
			e = &cognitive.LedgerEntry{ID: id, Since: step}
			l.byKey[key] = e
			l.order = append(l.order, key)
		}
		if stmt != "" {
			e.Statement = truncate(stmt, 300)
		}
		if st := ledgerStatus(h.Status); st != "" {
			e.Status = st
		} else if e.Status == "" {
			e.Status = "UNVERIFIED"
		}
		if h.ConfidenceEstimate > 0 && h.ConfidenceEstimate <= 1 {
			e.Confidence = h.ConfidenceEstimate
		}
		e.Updated = step
	}
}

// challenge marks the hypotheses settled by a reviewed decision as
// CHALLENGED: the verifier did not accept the conclusion they supported.
func (l *ledger) challenge(step int64) {
	for _, k := range l.order {
		e := l.byKey[k]
		if e.Updated == step && (e.Status == "VERIFIED" || e.Status == "REFUTED") {
			e.Status = "CHALLENGED"
		}
	}
}

// entries returns the ledger, open hypotheses first, newest updates first,
// bounded to MaxLedgerEntries.
func (l *ledger) entries() []cognitive.LedgerEntry {
	var open, settled []cognitive.LedgerEntry
	for _, k := range l.order {
		e := *l.byKey[k]
		if e.Status == "VERIFIED" || e.Status == "REFUTED" {
			settled = append(settled, e)
		} else {
			open = append(open, e)
		}
	}
	out := append(open, settled...)
	if len(out) > MaxLedgerEntries {
		out = out[:MaxLedgerEntries]
	}
	return out
}

// ledgerID keeps model-chosen ids to a safe structural vocabulary.
func ledgerID(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 32 {
		return ""
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return ""
		}
	}
	return s
}

func ledgerStatus(s string) string {
	switch st := strings.ToUpper(strings.TrimSpace(s)); st {
	case "VERIFIED", "REFUTED", "UNVERIFIED":
		return st
	}
	return ""
}
