package kernel

import (
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

var (
	digitsRe = regexp.MustCompile(`[0-9a-fA-F]{6,}|\d+`)
	// Words that mark a line worth keeping even when it is not rare.
	alertRe = regexp.MustCompile(`(?i)\b(error|err|warn(ing)?|fatal|panic|critical|crit|fail(ed|ure)?|exhaust\w*|oom\w*|killed|timeout|timed out|refused|denied|unavailable|saturat\w*|throttl\w*|back-?off|crashloop\w*|quorum|xid|ecc|down|lost|degraded|leak\w*|deadlock\w*|retry|reset)\b`)
)

// salientExcerpt shortens a long text to about max bytes while keeping what
// explains an incident. A head clip keeps the preamble and loses the one
// decisive line buried in a log; this keeps:
//
//   - the first and last quarter of the budget (context and the latest state),
//   - and, in the middle, lines ranked by how unusual they are: lines whose
//     shape (digits and ids removed) occurs once or twice, and lines with
//     failure vocabulary, beat the hundredth identical heartbeat.
//
// Kept lines stay in their original order, and omitted stretches are marked.
func salientExcerpt(s string, max int) string {
	if len(s) <= max {
		return s
	}
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) < 4 {
		return clipBytes(s, max) // one long line: nothing to select from
	}
	shape := make([]string, len(lines))
	count := map[string]int{}
	for i, l := range lines {
		shape[i] = digitsRe.ReplaceAllString(strings.TrimSpace(l), "#")
		count[shape[i]]++
	}
	keep := make([]bool, len(lines))
	used := 0
	take := func(i int) bool {
		cost := len(lines[i]) + 1
		if keep[i] || used+cost > max {
			return false
		}
		keep[i], used = true, used+cost
		return true
	}
	quarter := max / 4
	for i := 0; i < len(lines) && used < quarter; i++ {
		take(i)
	}
	tail := 0
	for i := len(lines) - 1; i >= 0 && tail < quarter; i-- {
		if take(i) {
			tail += len(lines[i]) + 1
		}
	}
	type cand struct{ i, score int }
	var cands []cand
	for i, l := range lines {
		if keep[i] || strings.TrimSpace(l) == "" {
			continue
		}
		score := 0
		switch c := count[shape[i]]; {
		case c == 1:
			score += 3
		case c <= 3:
			score += 2
		}
		if alertRe.MatchString(l) {
			score += 4
		}
		if score > 0 {
			cands = append(cands, cand{i, score})
		}
	}
	// Highest score first; earlier lines win ties.
	sort.SliceStable(cands, func(a, b int) bool { return cands[a].score > cands[b].score })
	for _, c := range cands {
		take(c.i)
	}
	var b strings.Builder
	skipped := false
	for i, l := range lines {
		if !keep[i] {
			skipped = true
			continue
		}
		if skipped {
			b.WriteString("[…]\n")
			skipped = false
		}
		b.WriteString(l)
		b.WriteByte('\n')
	}
	if skipped {
		b.WriteString("[…]\n")
	}
	out := strings.TrimRight(b.String(), "\n")
	if !utf8.ValidString(out) {
		out = strings.ToValidUTF8(out, "")
	}
	return out
}
