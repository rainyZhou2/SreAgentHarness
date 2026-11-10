// Package evidenceplan chooses the read-only checks that best separate a
// Work's competing hypotheses (RFC §12.4):
//
//	candidates = tools the next run may call            (Filter, hard)
//	score      = discrimination × reliability × freshness
//	             − latency − cost − operational load      (Score)
//	plan       = top K checks with a positive score
//
// The framework owns the candidate set: only tools the run's profile shows,
// that are read-only (R0 after risk overrides), that the policy allows
// without approval and that fit the Work's remaining budget can ever be
// planned. A model may suggest checks (next_checks in a run result); a
// suggestion only adjusts the score of a candidate that already passed the
// filters, never adds one.
//
// The package is pure: no SQL, no HTTP, no model calls.
package evidenceplan

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/tools"
)

// Version identifies the scoring rule recorded with every plan.
const Version = "heuristic-v1"

// Operator labels on tool descriptors (set through tool overlays).
const (
	// LabelTopics lists what a check can tell apart ("cpu,throttling,cfs").
	// Without it the tool's name and description are used.
	LabelTopics = "evidence.topics"
	// LabelLatency is fast | medium | slow (default medium).
	LabelLatency = "evidence.latency"
	// LabelCost is low | medium | high (default low).
	LabelCost = "evidence.cost"
	// LabelLoad is the operational load the query puts on production
	// systems: low | medium | high (default low).
	LabelLoad = "evidence.load"
	// LabelExclude = "true" keeps a tool out of planning (e.g. a search
	// tool that cannot discriminate anything).
	LabelExclude = "evidence.exclude"
)

// Limits.
const (
	DefaultMaxChecks = 3
	MaxChecks        = 8
	// MaxCandidates bounds the work per plan.
	MaxCandidates = 256
	// MinScore drops checks that are not worth the call.
	MinScore = 0.05
	// FreshWindow is how long a planned check counts as recently done.
	FreshWindow = 10 * time.Minute
)

// Exclusion reasons.
const (
	ExclNotVisible  = "not_visible"   // the run's profile does not show it
	ExclNotReadOnly = "not_read_only" // effective risk above R0
	ExclPolicy      = "policy"        // the policy denies it or needs approval
	ExclBudget      = "budget"        // too expensive for the remaining budget
	ExclOperator    = "operator"      // labeled evidence.exclude
	ExclBuiltin     = "builtin"       // framework tools read the run itself
)

// Constraints are the framework's hard filters for one run.
type Constraints struct {
	// Visible decides whether the run's profile shows a tool and returns its
	// effective descriptor (risk overrides applied). Nil: every tool.
	Visible func(tools.Descriptor) (tools.Descriptor, bool)
	// Gate is the policy gate; Target the resource the run investigates
	// (the Work's primary subject), used for tools that take a target.
	Gate   policy.Gate
	Target string
	// BudgetLeft is the share of the Work's cost budget still available
	// (1 = untouched or unlimited). Below 0.25 expensive checks are out.
	BudgetLeft float64
}

// Exclusion records why a tool cannot be planned.
type Exclusion struct {
	Tool   string `json:"tool"`
	Reason string `json:"reason"`
}

// Filter applies the hard constraints and returns the candidates.
func Filter(ds []tools.Descriptor, c Constraints) ([]tools.Descriptor, []Exclusion) {
	var out []tools.Descriptor
	var ex []Exclusion
	for _, d := range ds {
		if tools.IsBuiltin(d.Name) {
			ex = append(ex, Exclusion{d.Name, ExclBuiltin})
			continue
		}
		if strings.EqualFold(d.Labels[LabelExclude], "true") {
			ex = append(ex, Exclusion{d.Name, ExclOperator})
			continue
		}
		if c.Visible != nil {
			eff, ok := c.Visible(d)
			if !ok {
				ex = append(ex, Exclusion{d.Name, ExclNotVisible})
				continue
			}
			d = eff
		}
		if d.Risk != domain.RiskR0 {
			ex = append(ex, Exclusion{d.Name, ExclNotReadOnly})
			continue
		}
		if c.Gate == nil {
			ex = append(ex, Exclusion{d.Name, ExclPolicy})
			continue
		}
		target := ""
		if d.TargetArg != "" {
			target = c.Target
		}
		if dec := c.Gate.Evaluate(&d, target); dec.Verdict != policy.Allow || dec.RequiredApprovals > 0 {
			ex = append(ex, Exclusion{d.Name, ExclPolicy})
			continue
		}
		if c.BudgetLeft < 0.25 && level(d.Labels[LabelCost], "low") == 2 {
			ex = append(ex, Exclusion{d.Name, ExclBudget})
			continue
		}
		if len(out) < MaxCandidates {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, ex
}

// Input is everything one plan is scored from.
type Input struct {
	Hypotheses []evidence.Hypothesis
	Candidates []tools.Descriptor
	// Suggested maps a tool to the share of recent runs that suggested it
	// as a next check ([0,1]); only candidates are considered.
	Suggested map[string]float64
	// Previous is the latest earlier plan of this Work (nil: none).
	Previous *Plan
	Now      time.Time
	K        int
}

// Check is one planned read-only check.
type Check struct {
	Tool           string  `json:"tool"`
	Score          float64 `json:"score"`
	Discrimination float64 `json:"discrimination"`
	Reliability    float64 `json:"reliability"`
	Freshness      float64 `json:"freshness"`
	Penalty        float64 `json:"penalty"`
	// Bears lists the keys of the hypotheses the check bears on.
	Bears []string `json:"bears_on"`
	// Rationale is framework-generated text (no model output).
	Rationale string `json:"rationale"`
}

// Plan is the outcome of one planning pass.
type Plan struct {
	Version string    `json:"version"`
	At      time.Time `json:"at"`
	// Digest identifies the hypothesis state the plan was made for: a
	// repeated check against an unchanged state brings no new information.
	Digest     string   `json:"hypotheses_digest"`
	Live       int      `json:"live_hypotheses"`
	Checks     []Check  `json:"checks"`
	Candidates int      `json:"candidates"`
	Reason     string   `json:"reason,omitempty"` // why the plan is empty
	Tools      []string `json:"-"`
}

// Live reports whether a hypothesis still competes (not refuted).
func Live(h evidence.Hypothesis) bool {
	switch h.Status {
	case evidence.HypRefuted:
		return false
	}
	return strings.TrimSpace(h.Statement) != ""
}

// Digest hashes the live hypotheses and their evidence counts.
func Digest(hs []evidence.Hypothesis) string {
	var parts []string
	for _, h := range hs {
		if Live(h) {
			parts = append(parts, fmt.Sprintf("%s:%s:%d:%d", h.Key, h.Status, len(h.Supporting), len(h.Refuting)))
		}
	}
	sort.Strings(parts)
	return cpkit.HashBytes([]byte(strings.Join(parts, "|")))[7:23]
}

// Score ranks the candidates and returns the top K checks.
func Score(in Input) Plan {
	k := in.K
	if k <= 0 {
		k = DefaultMaxChecks
	}
	k = min(k, MaxChecks*MaxChecks) // agents stages plan K per agent
	p := Plan{Version: Version, At: in.Now, Digest: Digest(in.Hypotheses), Candidates: len(in.Candidates)}
	var live []evidence.Hypothesis
	for _, h := range in.Hypotheses {
		if Live(h) {
			live = append(live, h)
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].Key < live[j].Key })
	p.Live = len(live)
	switch {
	case len(live) == 0:
		p.Reason = "no competing hypotheses yet"
		return p
	case len(in.Candidates) == 0:
		p.Reason = "no read-only check is available to this run"
		return p
	}
	hypTokens := make([]map[string]bool, len(live))
	weights := make([]float64, len(live))
	for i, h := range live {
		hypTokens[i] = tokens(h.Statement)
		weights[i] = weight(h.Status)
	}
	var scored []Check
	for _, d := range in.Candidates {
		ct := checkTokens(d)
		rel := make([]float64, len(live))
		var bears []string
		for i := range live {
			rel[i] = relevance(ct, hypTokens[i])
			if rel[i] > 0 {
				bears = append(bears, live[i].Key)
			}
		}
		disc := discrimination(rel, weights)
		if s, ok := in.Suggested[d.Name]; ok && s > 0 {
			disc = 0.7*disc + 0.3*math.Min(s, 1)
		}
		c := Check{Tool: d.Name, Discrimination: round(disc), Reliability: reliability(d.Trust),
			Freshness: freshness(d.Name, p.Digest, in.Previous, in.Now), Bears: bears}
		c.Penalty = round(penalty(d.Labels[LabelLatency], "medium", 0, 0.05, 0.15) +
			penalty(d.Labels[LabelCost], "low", 0, 0.05, 0.15) + penalty(d.Labels[LabelLoad], "low", 0, 0.05, 0.1))
		c.Score = round(c.Discrimination*c.Reliability*c.Freshness - c.Penalty)
		if c.Score < MinScore {
			continue
		}
		c.Rationale = rationale(c, live)
		scored = append(scored, c)
	}
	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].Score != scored[j].Score {
			return scored[i].Score > scored[j].Score
		}
		return scored[i].Tool < scored[j].Tool
	})
	if len(scored) > k {
		scored = scored[:k]
	}
	p.Checks = scored
	if len(scored) == 0 {
		p.Reason = "no available check bears on the current hypotheses"
	}
	for _, c := range scored {
		p.Tools = append(p.Tools, c.Tool)
	}
	return p
}

// discrimination is high when a check bears on some live hypotheses but not
// others (it can tell them apart) and zero when it bears on all of them
// alike. With one hypothesis, confirming or refuting it is what counts.
func discrimination(rel, w []float64) float64 {
	if len(rel) == 1 {
		return rel[0]
	}
	var sum, wsum float64
	for i := range rel {
		sum += w[i] * rel[i]
		wsum += w[i]
	}
	mean := sum / wsum
	var dev float64
	for i := range rel {
		dev += w[i] * math.Abs(rel[i]-mean)
	}
	return math.Min(1, 2*dev/wsum)
}

// weight favors hypotheses the evidence disagrees on.
func weight(status string) float64 {
	switch status {
	case evidence.HypChallenged:
		return 1.5
	case evidence.HypSupported:
		return 0.8
	}
	return 1
}

// reliability maps the tool's output trust tier.
func reliability(trust string) float64 {
	switch strings.ToUpper(trust) {
	case "T0", "T1":
		return 1
	case "T2":
		return 0.9
	case "T3":
		return 0.8
	case "T4":
		return 0.5
	}
	return 0.6
}

// freshness discounts a check planned before: against the same hypothesis
// state it would repeat a query without new information.
func freshness(tool, digest string, prev *Plan, now time.Time) float64 {
	if prev == nil {
		return 1
	}
	planned := false
	for _, c := range prev.Checks {
		if c.Tool == tool {
			planned = true
		}
	}
	switch {
	case !planned:
		return 1
	case prev.Digest == digest:
		return 0.2
	case now.Sub(prev.At) < FreshWindow:
		return 0.7
	}
	return 1
}

func level(v, def string) int {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "":
		return level(def, "low")
	case "fast", "low":
		return 0
	case "slow", "high":
		return 2
	}
	return 1
}

func penalty(v, def string, lo, mid, hi float64) float64 {
	return [3]float64{lo, mid, hi}[level(v, def)]
}

func round(f float64) float64 { return math.Round(f*1000) / 1000 }

func rationale(c Check, live []evidence.Hypothesis) string {
	switch {
	case len(c.Bears) == 0:
		return "suggested by earlier runs"
	case len(live) == 1:
		return "can confirm or refute the only open hypothesis"
	case len(c.Bears) < len(live):
		return fmt.Sprintf("bears on %d of %d competing hypotheses, so it can tell them apart", len(c.Bears), len(live))
	}
	return "bears on all competing hypotheses with different strength"
}

// ---- relevance ----

var stop = map[string]bool{"the": true, "and": true, "for": true, "with": true, "from": true, "that": true, "this": true,
	"into": true, "are": true, "was": true, "were": true, "has": true, "have": true, "not": true, "its": true, "via": true,
	"get": true, "list": true, "query": true, "read": true, "show": true, "tool": true, "returns": true, "return": true,
	"current": true, "service": true, "services": true, "due": true, "caused": true, "cause": true, "because": true,
	"after": true, "before": true, "high": true, "low": true, "increase": true, "increased": true, "issue": true}

// stem is deliberately crude: it only needs throttling ~ throttled.
func stem(w string) string {
	for _, suf := range []string{"ing", "ed", "es", "s"} {
		if len(w) > len(suf)+3 && strings.HasSuffix(w, suf) {
			return strings.TrimSuffix(w, suf)
		}
	}
	return w
}

func tokens(s string) map[string]bool {
	out := map[string]bool{}
	for _, f := range strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		if len(f) < 3 || stop[f] {
			continue
		}
		out[stem(f)] = true
		if len(out) >= 64 {
			break
		}
	}
	return out
}

func checkTokens(d tools.Descriptor) map[string]bool {
	if t := d.Labels[LabelTopics]; t != "" {
		return tokens(strings.ReplaceAll(t, ",", " "))
	}
	return tokens(d.Name + " " + cpkit.Truncate(d.Description, 400))
}

// relevance is the share of a hypothesis' key terms a check covers
// (saturating at two shared terms).
func relevance(check, hyp map[string]bool) float64 {
	if len(hyp) == 0 {
		return 0
	}
	n := 0
	for t := range hyp {
		if check[t] {
			n++
		}
	}
	return math.Min(1, float64(n)/math.Min(2, float64(len(hyp))))
}
