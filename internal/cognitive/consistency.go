package cognitive

import (
	"context"
	"strings"
	"unicode"

	"go-sre-harness/internal/domain"
)

// Sampler is a strategy that can draw an extra, side-effect-free sample of
// its decision (no streaming to the operator).
type Sampler interface {
	Sample(ctx context.Context, in ReasoningInput) (domain.Decision, error)
}

// Sample draws a decision without publishing stream deltas.
func (l *LLM) Sample(ctx context.Context, in ReasoningInput) (domain.Decision, error) {
	c := *l
	c.Deltas = nil
	return c.Decide(ctx, in)
}

// SelfConsistency samples a final task conclusion n times in total and
// keeps the conclusion of the largest cluster of agreeing samples. Samples
// agree when they reach the same verdict about the same leading hypothesis
// (token overlap of the statements). The first decision is kept when it is
// in the largest cluster, so a unanimous vote changes nothing but the
// recorded agreement. Failed samples are skipped.
func SelfConsistency(ctx context.Context, s Strategy, in ReasoningInput, first domain.Decision, n int) domain.Decision {
	if n < 2 || first.Kind != domain.DecisionFinal || first.Intent != domain.IntentTask {
		return first
	}
	draw := s.Decide
	if sp, ok := s.(Sampler); ok {
		draw = sp.Sample
	}
	samples := []domain.Decision{first}
	var ids []string
	for i := 1; i < n && ctx.Err() == nil; i++ {
		d, err := draw(ctx, in)
		if err != nil || Validate(d) != nil || d.Kind != domain.DecisionFinal {
			continue
		}
		samples = append(samples, d)
		if d.ModelRequestID != "" {
			ids = append(ids, d.ModelRequestID)
		}
	}
	keys := make([]conclusionKey, len(samples))
	for i, d := range samples {
		keys[i] = keyOf(d)
	}
	// Greedy clustering around the earliest member.
	cluster := make([]int, len(samples))
	var heads []int
	for i := range samples {
		cluster[i] = -1
		for c, h := range heads {
			if keys[h].agrees(keys[i]) {
				cluster[i] = c
				break
			}
		}
		if cluster[i] < 0 {
			cluster[i] = len(heads)
			heads = append(heads, i)
		}
	}
	size := make([]int, len(heads))
	for _, c := range cluster {
		size[c]++
	}
	best := 0
	for c := range heads {
		if size[c] > size[best] {
			best = c
		}
	}
	chosen := samples[heads[best]]
	var alts []string
	for c, h := range heads {
		if c != best && len(alts) < 4 {
			alts = append(alts, keys[h].describe())
		}
	}
	chosen.Consistency = &domain.Consistency{Samples: len(samples),
		Agreement: float64(size[best]) / float64(len(samples)), Alternatives: alts, ModelRequestIDs: ids}
	return chosen
}

type conclusionKey struct {
	verdict string
	lead    string
	tokens  map[string]bool
}

func keyOf(d domain.Decision) conclusionKey {
	k := conclusionKey{verdict: "UNVERIFIED"}
	if d.Thought != nil && d.Thought.Verification != nil {
		if v := strings.ToUpper(strings.TrimSpace(d.Thought.Verification.Verdict)); v != "" {
			k.verdict = v
		}
	}
	best := -1.0
	for _, h := range d.Hypotheses {
		if strings.EqualFold(h.Status, "VERIFIED") && h.ConfidenceEstimate > best {
			best, k.lead = h.ConfidenceEstimate, h.Statement
		}
	}
	if k.lead == "" {
		k.lead = d.Answer
		if i := strings.IndexAny(k.lead, ".。\n"); i > 0 {
			k.lead = k.lead[:i]
		}
	}
	k.tokens = tokenSet(k.lead)
	return k
}

func (a conclusionKey) agrees(b conclusionKey) bool {
	if a.verdict != b.verdict {
		return false
	}
	if len(a.tokens) == 0 || len(b.tokens) == 0 {
		return len(a.tokens) == len(b.tokens)
	}
	inter := 0
	for t := range a.tokens {
		if b.tokens[t] {
			inter++
		}
	}
	// Overlap coefficient: a restatement that adds detail still agrees.
	return float64(inter)/float64(min(len(a.tokens), len(b.tokens))) >= 0.5
}

func (a conclusionKey) describe() string {
	s := strings.Join(strings.Fields(a.lead), " ")
	if r := []rune(s); len(r) > 160 {
		s = string(r[:160]) + "…"
	}
	return a.verdict + ": " + s
}

// Tokens is tokenSet for other packages (tool selection).
func Tokens(s string) map[string]bool { return tokenSet(s) }

// tokenSet splits text into lower-case words; CJK text contributes
// character bigrams so Chinese statements compare too.
func tokenSet(s string) map[string]bool {
	out := map[string]bool{}
	var word, cjk []rune
	flushWord := func() {
		if len(word) > 2 {
			out[string(word)] = true
		}
		word = word[:0]
	}
	flushCJK := func() {
		for i := 0; i+1 < len(cjk); i++ {
			out[string(cjk[i:i+2])] = true
		}
		if len(cjk) == 1 {
			out[string(cjk)] = true
		}
		cjk = cjk[:0]
	}
	for _, r := range strings.ToLower(s) {
		switch {
		case unicode.Is(unicode.Han, r):
			flushWord()
			cjk = append(cjk, r)
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			flushCJK()
			word = append(word, r)
		default:
			flushWord()
			flushCJK()
		}
	}
	flushWord()
	flushCJK()
	return out
}
