// Package compacteval measures how well a compactor preserves what an
// investigation needs, with checks that need no judge model:
//
//   - fact recall: how many of a case's key facts (a number, an error name,
//     a deploy id) are still in the summary;
//   - input coverage: the same measure on what a model-backed compactor is
//     *shown*. A model cannot keep what it never saw, so this is an upper
//     bound on its recall and is computable offline;
//   - citation retention: every obs:step<N> reference survives;
//   - injection leakage: text planted in tool output that tries to instruct
//     the agent must not reach the summary;
//   - compression: summary size against the size of the folded output.
//
// Run it with a real model (`harness compact-eval`) to measure the model's
// recall against that upper bound.
package compacteval

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
)

// Fact is something the summary must still convey. Any of Any counts (a
// number may be written 2400 or 2.4s); matching ignores case and spacing.
type Fact struct {
	Name string
	Any  []string
}

// Case is one investigation history to fold.
type Case struct {
	Name         string
	Goal         string
	Observations []domain.Observation
	Facts        []Fact
	// Planted are strings from injected tool output that must not appear.
	Planted []string
}

// Result is the measurement of one compactor on one case.
type Result struct {
	Case          string
	InputBytes    int
	SummaryBytes  int
	Ratio         float64 // summary / input
	FactRecall    float64
	Missing       []string
	CitationsKept float64
	Leaked        []string
	// InputCoverage is the fact recall of what the model is shown (only for
	// compactors with an InputDigest).
	InputCoverage float64
	HasCoverage   bool
	Err           error
}

// Compactor is what Run needs; *kernel.LLMCompactor and
// kernel.DeterministicCompactor both satisfy it.
type Compactor interface {
	Compact(ctx context.Context, in kernel.CompactInput) (string, error)
}

type digester interface {
	InputDigest(ctx context.Context, in kernel.CompactInput) (string, error)
}

var refRe = regexp.MustCompile(`obs:step\d+(?:\.\d+)?`)

func norm(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), ""))
}

func recall(text string, facts []Fact) (float64, []string) {
	if len(facts) == 0 {
		return 1, nil
	}
	n := norm(text)
	var missing []string
	hit := 0
	for _, f := range facts {
		ok := false
		for _, a := range f.Any {
			if strings.Contains(n, norm(a)) {
				ok = true
				break
			}
		}
		if ok {
			hit++
		} else {
			missing = append(missing, f.Name)
		}
	}
	return float64(hit) / float64(len(facts)), missing
}

// Run folds every case with c and measures the summary.
func Run(ctx context.Context, c Compactor, cases []Case) []Result {
	out := make([]Result, 0, len(cases))
	for _, cs := range cases {
		in := kernel.CompactInput{TenantID: "eval", RunID: "eval-" + cs.Name, Goal: cs.Goal, Observations: cs.Observations,
			IsSideEffecting: func(string) bool { return false }}
		r := Result{Case: cs.Name}
		for _, o := range cs.Observations {
			r.InputBytes += len(o.Content)
		}
		sum, err := c.Compact(ctx, in)
		if err != nil {
			r.Err = err
			out = append(out, r)
			continue
		}
		r.SummaryBytes = len(sum)
		if r.InputBytes > 0 {
			r.Ratio = float64(r.SummaryBytes) / float64(r.InputBytes)
		}
		r.FactRecall, r.Missing = recall(sum, cs.Facts)
		want, have := map[string]bool{}, map[string]bool{}
		for _, o := range cs.Observations {
			want[fmt.Sprintf("obs:step%d", o.StepSeq)] = true
		}
		for _, ref := range refRe.FindAllString(sum, -1) {
			have[strings.SplitN(ref, ".", 2)[0]] = true
		}
		kept := 0
		for ref := range want {
			if have[ref] {
				kept++
			}
		}
		if len(want) > 0 {
			r.CitationsKept = float64(kept) / float64(len(want))
		}
		low := strings.ToLower(sum)
		for _, p := range cs.Planted {
			if strings.Contains(low, strings.ToLower(p)) {
				r.Leaked = append(r.Leaked, p)
			}
		}
		if d, ok := c.(digester); ok {
			if dg, err := d.InputDigest(ctx, in); err == nil {
				r.InputCoverage, _ = recall(dg, cs.Facts)
				r.HasCoverage = true
			}
		}
		out = append(out, r)
	}
	return out
}

// Summary averages results.
type Summary struct {
	Cases         int
	FactRecall    float64
	InputCoverage float64
	HasCoverage   bool
	CitationsKept float64
	Ratio         float64
	Leaks         int
	Errors        int
}

// Summarize averages the per-case results.
func Summarize(rs []Result) Summary {
	var s Summary
	cov := 0
	for _, r := range rs {
		s.Cases++
		if r.Err != nil {
			s.Errors++
			continue
		}
		s.FactRecall += r.FactRecall
		s.CitationsKept += r.CitationsKept
		s.Ratio += r.Ratio
		s.Leaks += len(r.Leaked)
		if r.HasCoverage {
			s.InputCoverage += r.InputCoverage
			cov++
		}
	}
	if n := float64(s.Cases - s.Errors); n > 0 {
		s.FactRecall /= n
		s.CitationsKept /= n
		s.Ratio /= n
	}
	if cov > 0 {
		s.InputCoverage /= float64(cov)
		s.HasCoverage = true
	}
	return s
}
