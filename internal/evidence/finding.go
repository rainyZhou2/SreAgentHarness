package evidence

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
)

// Finding verdicts.
const (
	FindingSupported    = "supported"
	FindingRefuted      = "refuted"
	FindingInsufficient = "insufficient"
	// FindingContested: agents reached opposite, each evidenced, verdicts on
	// the same hypothesis. It is resolved by a distinguishing check, never
	// by counting agents.
	FindingContested = "contested"
)

// Finding is one agent's structured statement about one hypothesis.
type Finding struct {
	ID           string      `json:"id,omitempty"`
	Generation   int         `json:"generation,omitempty"`
	TenantID     string      `json:"tenant_id,omitempty"`
	WorkID       string      `json:"work_id,omitempty"`
	RunID        string      `json:"run_id"`
	Role         string      `json:"role"`
	Statement    string      `json:"statement"`
	HypothesisID string      `json:"hypothesis_id"`
	Confidence   float64     `json:"confidence"`
	Supports     []string    `json:"supports,omitempty"`
	Refutes      []string    `json:"refutes,omitempty"`
	SourceTimes  []time.Time `json:"source_times,omitempty"`
	Gaps         []string    `json:"gaps,omitempty"`
	NextChecks   []string    `json:"next_checks,omitempty"`
	Verdict      string      `json:"verdict"` // supported|refuted|insufficient|contested
	// Claimed is what the agent itself concluded; Verdict is what the
	// synthesis could confirm from the evidence it cited.
	Claimed string `json:"claimed,omitempty"`
	// Independent counts the distinct observations behind Supports/Refutes.
	IndependentSupport int `json:"independent_support"`
	IndependentRefute  int `json:"independent_refute"`
	// Rejected lists cited refs that were not accepted, with the reason.
	Rejected  []string  `json:"rejected_refs,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

// PayloadHash identifies a finding's content (idempotent recording).
func (f Finding) PayloadHash() string {
	return cpkit.MustHash(struct {
		Run, Role, Hyp, Statement, Verdict string
		Generation                         int
		Supports, Refutes                  []string
	}{f.RunID, f.Role, f.HypothesisID, f.Statement, f.Verdict, f.Generation, f.Supports, f.Refutes})
}

// Synthesis is the validated result of a stage's agents.
type Synthesis struct {
	Findings []Finding `json:"findings"`
	// Contested are hypothesis keys on which agents disagree.
	Contested []string `json:"contested,omitempty"`
	// NextChecks are the distinguishing checks proposed for contested or
	// insufficient hypotheses.
	NextChecks []string `json:"next_checks,omitempty"`
	// Unavailable are agents that produced no usable result, with why. Their
	// absence is recorded; the findings of the others stand.
	Unavailable map[string]string `json:"unavailable,omitempty"`
	Rejected    int               `json:"rejected_refs"`
}

// Synthesize validates what the agents of a stage reported against the
// Work's evidence ledger (design §11.2). An agent may cite evidence; it
// cannot make it true by citing it:
//
//   - a cited ref must be a record of this Work's ledger (which is tenant-
//     scoped), captured no later than the investigation time. Refs to
//     anything else — another Work, a made-up id, a tool call the ledger
//     does not hold — do not count;
//   - stale, partial, injected and unresolved evidence does not count;
//   - several refs to one observation (the same source, query and window)
//     are one piece of evidence, however many agents cite it;
//   - evidence that only correlates cannot support a causal statement;
//   - opposite verdicts on one hypothesis make it contested, with a
//     distinguishing check — the number of agents on each side is ignored.
func Synthesize(envs []ResultEnvelope, ledger []Record, at time.Time) Synthesis {
	scope := DecisionScope{}
	if len(ledger) > 0 {
		scope.TenantID, scope.WorkID = ledger[0].TenantID, ledger[0].WorkID
	}
	return synthesize(scope, envs, ledger, at)
}

func synthesize(scope DecisionScope, envs []ResultEnvelope, ledger []Record, at time.Time) Synthesis {
	resolver := newReferenceResolverAt(scope, ledger, at)
	out := Synthesis{Unavailable: map[string]string{}}
	for _, env := range envs {
		who := firstNonEmpty(env.AgentRole, env.RunID)
		if env.Status != "COMPLETED" {
			// Timeouts, failures and cancellations are gaps, not opinions.
			out.Unavailable[who] = firstNonEmpty(env.FailureReason, strings.ToLower(env.Status))
			continue
		}
		for _, h := range env.Hypotheses {
			f := Finding{TenantID: scope.TenantID, WorkID: scope.WorkID, Generation: scope.Generation, RunID: env.RunID, Role: who, Statement: cpkit.Truncate(h.Statement, MaxStatement),
				HypothesisID: HypothesisKey(h.Statement), Confidence: h.Confidence, Claimed: strings.ToLower(h.Status),
				NextChecks: env.NextChecks, Gaps: append([]string(nil), env.Uncertainties...), CreatedAt: at}
			var sawCausal bool
			accept := func(refs []string, supporting bool) ([]string, int) {
				groups := map[string]bool{}
				var kept []string
				for _, ref := range refs {
					rec, err := resolver.Resolve(ref, env.RunID)
					switch {
					case err != nil:
						f.Rejected = append(f.Rejected, ref+": "+err.Error())
					case rec.CapturedAt.IsZero() || rec.CapturedAt.After(at):
						f.Rejected = append(f.Rejected, ref+": captured after the investigation")
					case !rec.Usable() || rec.Kind == KindInference:
						f.Rejected = append(f.Rejected, ref+": evidence is "+rec.Quality)
					case rec.TrustTier != "T1" && rec.TrustTier != "T2" && rec.TrustTier != "T3":
						f.Rejected = append(f.Rejected, ref+": untrusted evidence")
					case supporting && rec.Relation != RelSupports && rec.Relation != RelCausedBy && rec.Relation != RelCorrelates && rec.Relation != RelPrecedes:
						f.Rejected = append(f.Rejected, ref+": relation does not support the claim")
					case !supporting && rec.Relation != RelRefutes:
						f.Rejected = append(f.Rejected, ref+": relation does not refute the claim")
					default:
						kept = appendUnique(kept, "evidence:"+rec.ID)
						groups[rec.Group()] = true
						f.SourceTimes = append(f.SourceTimes, rec.CapturedAt)
						if rec.Relation != RelCorrelates && rec.Relation != RelPrecedes {
							sawCausal = true
						}
					}
				}
				return kept, len(groups)
			}
			f.Supports, f.IndependentSupport = accept(h.Supports, true)
			supportIsCausal := sawCausal
			f.Refutes, f.IndependentRefute = accept(h.Refutes, false)
			out.Rejected += len(f.Rejected)
			switch {
			case f.IndependentSupport > 0 && supportIsCausal && f.IndependentRefute > 0:
				f.Verdict = FindingContested
			case f.IndependentRefute > 0:
				f.Verdict = FindingRefuted
			case f.IndependentSupport > 0 && !supportIsCausal:
				// "It changed just before" is a lead, not a cause.
				f.Verdict = FindingInsufficient
				f.Gaps = append(f.Gaps, "the cited evidence only correlates; compare versions or observe a rollback")
			case f.IndependentSupport > 0:
				f.Verdict = FindingSupported
			default:
				f.Verdict = FindingInsufficient
				if f.Claimed == "verified" || f.Claimed == "supported" {
					f.Gaps = append(f.Gaps, "the conclusion cites no usable evidence")
				}
			}
			out.Findings = append(out.Findings, f)
		}
	}
	// Disagreement between agents on one hypothesis.
	verdicts := map[string]map[string]bool{}
	for _, f := range out.Findings {
		if verdicts[f.HypothesisID] == nil {
			verdicts[f.HypothesisID] = map[string]bool{}
		}
		verdicts[f.HypothesisID][f.Verdict] = true
	}
	contested := map[string]bool{}
	for k, v := range verdicts {
		if v[FindingContested] || (v[FindingSupported] && v[FindingRefuted]) {
			contested[k] = true
			out.Contested = append(out.Contested, k)
		}
	}
	sort.Strings(out.Contested)
	checks := map[string]bool{}
	for i := range out.Findings {
		f := &out.Findings[i]
		if contested[f.HypothesisID] {
			f.Verdict = FindingContested
			check := fmt.Sprintf("distinguish %q: find an observation that only one side predicts", cpkit.Truncate(f.Statement, 120))
			if !checks[check] {
				checks[check] = true
				out.NextChecks = append(out.NextChecks, check)
			}
		}
	}
	if len(out.Unavailable) == 0 {
		out.Unavailable = nil
	}
	return out
}

// FindingStore persists findings (internal/store/cpsql).
type FindingStore interface {
	AddFinding(ctx context.Context, tenantID, workID string, f Finding) (Finding, bool, error)
	ListFindings(ctx context.Context, tenantID, workID string, limit int) ([]Finding, error)
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}
