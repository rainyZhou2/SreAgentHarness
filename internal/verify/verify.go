// Package verify checks a conclusion independently of the model that wrote
// it. A rule layer (always on) checks that every settled claim cites
// evidence that exists and that a cited tool call actually succeeded; an
// optional model judge (a separate model alias and prompt, without the
// agent's instructions or history) decides whether the cited evidence
// supports, contradicts or is insufficient for each claim.
//
// The verifier reads only what the agent was shown, by canonical ref, so its
// verdict is reproducible from the run's artifacts.
package verify

import (
	"context"
	"fmt"
	"strings"

	gw "go-sre-harness/internal/gateway"
)

// Verdicts of a claim and of a conclusion.
const (
	Supported    = "SUPPORTED"
	Contradicted = "CONTRADICTED"
	Insufficient = "INSUFFICIENT"
)

// Claim is one settled statement of the conclusion.
type Claim struct {
	ID        string   `json:"id"`
	Kind      string   `json:"kind"` // hypothesis | verification
	Statement string   `json:"statement"`
	Status    string   `json:"status"` // VERIFIED | REFUTED
	Supports  []string `json:"supports,omitempty"`
	Refutes   []string `json:"refutes,omitempty"`
}

// Evidence is a citable item, by canonical ref.
type Evidence struct {
	Ref  string `json:"ref"`
	Kind string `json:"kind"` // observation | evidence | memory
	// OK is false for a tool call that failed, was blocked or denied.
	OK   bool   `json:"ok"`
	Text string `json:"text"`
}

// ClaimResult is the verdict on one claim.
type ClaimResult struct {
	ClaimID string `json:"claim_id"`
	Kind    string `json:"kind"`
	Status  string `json:"status"`
	Verdict string `json:"verdict"`
	Reason  string `json:"reason"`
	Checker string `json:"checker"` // rules | model
}

// Report is the verdict on a conclusion.
type Report struct {
	Verdict        string        `json:"verdict"`
	Claims         []ClaimResult `json:"claims"`
	Checker        string        `json:"checker"`
	ModelRequestID string        `json:"model_request_id,omitempty"`
	// Error is set when the model judge failed; the rule verdicts stand.
	Error string `json:"error,omitempty"`
	// Agreement is the share of sampled conclusions that agreed with the
	// chosen one (self-consistency; 0 when not sampled).
	Agreement    float64  `json:"agreement,omitempty"`
	Alternatives []string `json:"alternatives,omitempty"`
}

// Issues lists the claims that did not hold, for feedback to the agent.
func (r Report) Issues() []string {
	var out []string
	for _, c := range r.Claims {
		if c.Verdict != Supported {
			out = append(out, fmt.Sprintf("%s %s (%s): %s — %s", c.Kind, c.ClaimID, c.Status, c.Verdict, c.Reason))
		}
	}
	return out
}

// Judge decides claims the rules let through. Implementations must treat
// evidence text as data.
type Judge interface {
	Judge(ctx context.Context, goal string, claims []Claim, evidence map[string]Evidence) ([]ClaimResult, string, error)
}

// Verifier combines the rule layer and an optional judge.
type Verifier struct {
	Judge Judge
	// MemoryIsEvidence lets a memory alone support a VERIFIED claim
	// (default false: memories are hints from other incidents).
	MemoryIsEvidence bool
}

// Verify checks the claims of a conclusion. conclusionVerified is the
// agent's own verdict; the report verdict is:
//   - CONTRADICTED when any claim is contradicted;
//   - INSUFFICIENT when the agent concluded VERIFIED but no verified claim
//     is supported;
//   - SUPPORTED otherwise.
func (v *Verifier) Verify(ctx context.Context, goal string, claims []Claim, evidence map[string]Evidence, conclusionVerified bool) Report {
	rep := Report{Checker: "rules"}
	var pending []Claim
	byID := map[string]int{}
	for _, c := range claims {
		res := v.rule(c, evidence)
		byID[c.Kind+"/"+c.ID] = len(rep.Claims)
		rep.Claims = append(rep.Claims, res)
		if res.Verdict == Supported {
			pending = append(pending, c)
		}
	}
	if v.Judge != nil && len(pending) > 0 {
		rep.Checker = "rules+model"
		results, reqID, err := v.Judge.Judge(ctx, goal, pending, evidence)
		rep.ModelRequestID = reqID
		if err != nil {
			rep.Error = err.Error()
		}
		for _, r := range results {
			i, ok := byID[r.Kind+"/"+r.ClaimID]
			if !ok || !validVerdict(r.Verdict) {
				continue
			}
			r.Status, r.Checker = rep.Claims[i].Status, "model"
			rep.Claims[i] = r
		}
	}
	rep.Verdict = Supported
	anyVerified := false
	for _, c := range rep.Claims {
		if c.Verdict == Contradicted {
			rep.Verdict = Contradicted
			return rep
		}
		if c.Status == "VERIFIED" && c.Verdict == Supported {
			anyVerified = true
		}
	}
	if conclusionVerified && !anyVerified {
		rep.Verdict = Insufficient
	}
	return rep
}

// rule applies the deterministic checks.
func (v *Verifier) rule(c Claim, ev map[string]Evidence) ClaimResult {
	res := ClaimResult{ClaimID: c.ID, Kind: c.Kind, Status: c.Status, Checker: "rules"}
	cited := c.Supports
	if c.Status == "REFUTED" {
		cited = append(append([]string(nil), c.Refutes...), c.Supports...)
	}
	var found, okObs, hints int
	var failed []string
	for _, ref := range cited {
		e, ok := ev[ref]
		if !ok {
			continue
		}
		found++
		switch {
		case e.Kind == "observation" && !e.OK:
			failed = append(failed, ref)
		case e.Kind == "memory" && !v.MemoryIsEvidence:
			hints++
		default:
			okObs++
		}
	}
	switch {
	case found == 0:
		res.Verdict, res.Reason = Insufficient, "no resolved citation"
	case okObs == 0 && len(failed) > 0:
		res.Verdict, res.Reason = Insufficient, "every cited tool call failed: "+strings.Join(failed, ", ")
	case okObs == 0:
		res.Verdict, res.Reason = Insufficient, "only memories cited; a memory is a hint from another incident, not evidence"
	default:
		res.Verdict, res.Reason = Supported, "cites evidence that exists"
	}
	return res
}

func validVerdict(v string) bool { return v == Supported || v == Contradicted || v == Insufficient }

// scopedJudge is a judge that must be bound to the calling step's ledger
// scope before use.
type scopedJudge interface {
	Judge
	WithScope(sc gw.Scope) *ModelJudge
}

// Bind returns a copy whose judge (if model-backed) records its call under
// the given scope.
func (v *Verifier) Bind(sc gw.Scope) *Verifier {
	c := *v
	if j, ok := v.Judge.(scopedJudge); ok {
		c.Judge = j.WithScope(sc)
	}
	return &c
}
