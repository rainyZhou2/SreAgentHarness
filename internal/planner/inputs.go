package planner

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/untrusted"
)

// InjectionFlag records instruction-like spans found in one planner input.
// Any flag forces a human review of a generated flow.
type InjectionFlag struct {
	Source string   `json:"source"`
	Spans  []string `json:"spans"`
}

// Replan describes the failed previous attempt of a generated flow. The
// kernel builds it from durable flow state; node details are untrusted.
type Replan struct {
	// Generation is the generation the new plan will run as (>= 1).
	Generation        int          `json:"generation"`
	PreviousPlanHash  string       `json:"previous_plan_hash,omitempty"`
	PreviousFlowHash  string       `json:"previous_flow_hash,omitempty"`
	PreviousRationale string       `json:"previous_rationale,omitempty"`
	PreviousFallback  bool         `json:"previous_fallback,omitempty"`
	Failure           string       `json:"failure"`
	Nodes             []ReplanNode `json:"nodes,omitempty"`
	// PreviousFlow is the failed graph, for the reviewer's diff.
	PreviousFlow json.RawMessage `json:"previous_flow,omitempty"`
}

// Exemplar is a memory shown to the planner: a plan that worked for a
// similar goal ("plan") or a distilled reasoning pattern ("pattern").
type Exemplar struct {
	MemoryID string `json:"memory_id"`
	Kind     string `json:"kind"`
	Title    string `json:"title"`
	Content  string `json:"content"`
}

// ReplanNode is one node of the failed flow and what it produced.
type ReplanNode struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	Status     string `json:"status"`
	Tool       string `json:"tool,omitempty"`
	SideEffect bool   `json:"side_effect,omitempty"`
	// Detail is an excerpt of the node output or error (untrusted).
	Detail string `json:"detail,omitempty"`
}

const (
	maxToolDescription = 240
	maxHistoryMessage  = 1200
	maxReplanDetail    = 400
	maxReplanNodes     = 24
	// History selection: the most recent messages are always shown, older
	// ones only when they are relevant to the goal, within a total budget.
	historyRecent    = 4
	historyRelevant  = 4
	historyBudget    = 6000
	historyMinSim    = 0.08
	maxExemplars     = 4
	maxExemplarRunes = 1800
)

// sanitizeTools neutralizes tool descriptions and withholds those that look
// like injected instructions. Tool descriptions come from plugins and tenant
// MCP servers, so they are as untrusted as tool output.
func sanitizeTools(ds []tools.Descriptor) ([]tools.Descriptor, []InjectionFlag) {
	out := make([]tools.Descriptor, len(ds))
	var flags []InjectionFlag
	for i, d := range ds {
		desc := untrusted.Neutralize(d.Description)
		if spans := untrusted.Scan(desc); len(spans) > 0 {
			flags = append(flags, InjectionFlag{Source: "tool:" + d.Name, Spans: spans})
			desc = "[description withheld: injection_suspected]"
		}
		d.Description = desc
		out[i] = d
	}
	return out, flags
}

// promptInputs renders the data sections of the planner prompt. Every
// section that carries text written by someone other than the operator of
// this deployment is enclosed in an untrusted boundary.
type promptInputs struct {
	goal, session, capabilities, previous, exemplars string
	flags                                            []InjectionFlag
	exemplarIDs                                      []string
}

func renderInputs(in Input) promptInputs {
	var p promptInputs
	wrap := func(source, attrs, body string) string {
		blk := untrusted.Wrap(attrs, body)
		if len(blk.Flags) > 0 {
			p.flags = append(p.flags, InjectionFlag{Source: source, Spans: blk.Flags})
		}
		return blk.Text
	}
	p.goal = wrap("goal", "source=goal role=task", strings.TrimSpace(in.Goal))
	if len(in.History) > 0 {
		var b strings.Builder
		picked, omitted := selectHistory(in.History, in.Goal)
		if omitted > 0 {
			fmt.Fprintf(&b, "(%d earlier messages omitted as unrelated to the goal)\n", omitted)
		}
		for _, m := range picked {
			fmt.Fprintf(&b, "%s: %s\n", roleLabel(m.Role), truncate(m.Text, maxHistoryMessage))
		}
		p.session = wrap("session", "source=session", b.String())
	}
	if len(in.Exemplars) > 0 {
		var b strings.Builder
		for i, e := range in.Exemplars {
			if i >= maxExemplars {
				break
			}
			kind := "plan"
			if e.Kind == "pattern" {
				kind = "pattern"
			}
			// Structural fields come from the server; the body is memory
			// written from earlier runs and stays inside a boundary.
			fmt.Fprintf(&b, "- memory=%s kind=%s\n", e.MemoryID, kind)
			b.WriteString(wrap("exemplar:"+e.MemoryID, "source=memory kind="+kind,
				truncate(e.Title, 200)+"\n"+clipRunes(e.Content, maxExemplarRunes)))
			b.WriteString("\n")
			p.exemplarIDs = append(p.exemplarIDs, e.MemoryID)
		}
		p.exemplars = b.String()
	}
	if len(in.Capabilities) > 0 {
		p.capabilities = wrap("capabilities", "source=capability_map", "- "+strings.Join(in.Capabilities, "\n- "))
	}
	if r := in.Replan; r != nil {
		var meta strings.Builder
		fmt.Fprintf(&meta, "generation=%d previous_plan_hash=%s previous_fallback=%t\n",
			r.Generation, r.PreviousPlanHash, r.PreviousFallback)
		var detail strings.Builder
		fmt.Fprintf(&detail, "failure: %s\n", truncate(r.Failure, maxReplanDetail))
		if r.PreviousRationale != "" {
			fmt.Fprintf(&detail, "previous rationale: %s\n", truncate(r.PreviousRationale, maxReplanDetail))
		}
		nodes := r.Nodes
		if len(nodes) > maxReplanNodes {
			nodes = nodes[:maxReplanNodes]
		}
		for _, n := range nodes {
			// Structural fields are written by the server, outside the block.
			fmt.Fprintf(&meta, "- node=%s type=%s status=%s", n.ID, n.Type, n.Status)
			if n.Tool != "" {
				fmt.Fprintf(&meta, " tool=%s", n.Tool)
			}
			if n.SideEffect {
				meta.WriteString(" side_effect=true")
			}
			meta.WriteByte('\n')
			if n.Detail != "" {
				fmt.Fprintf(&detail, "[%s] %s\n", n.ID, truncate(n.Detail, maxReplanDetail))
			}
		}
		p.previous = meta.String() + wrap("previous_attempt", "source=previous_attempt", detail.String())
	}
	return p
}

// roleLabel keeps the role prefix a fixed vocabulary, so a stored message
// cannot spoof a "system:" line.
func roleLabel(r string) string {
	switch strings.ToLower(r) {
	case "user", "assistant", "operator", "tool":
		return strings.ToLower(r)
	}
	return "message"
}

// selectHistory keeps the most recent messages and the older ones most
// similar to the goal, in their original order, within historyBudget
// characters. It returns the kept messages and how many were dropped.
func selectHistory(h []Message, goal string) ([]Message, int) {
	if len(h) <= historyRecent {
		return h, 0
	}
	keep := map[int]bool{}
	for i := len(h) - historyRecent; i < len(h); i++ {
		keep[i] = true
	}
	q := ngramRaw(goal)
	normalize(q)
	type cand struct {
		i   int
		sim float64
	}
	var cs []cand
	for i := 0; i < len(h)-historyRecent; i++ {
		v := ngramRaw(h[i].Text)
		normalize(v)
		if sim := dot(q, v); sim >= historyMinSim {
			cs = append(cs, cand{i, sim})
		}
	}
	sort.SliceStable(cs, func(a, b int) bool { return cs[a].sim > cs[b].sim })
	for i, c := range cs {
		if i >= historyRelevant {
			break
		}
		keep[c.i] = true
	}
	// Enforce the budget, dropping the oldest relevant messages first and
	// never the most recent ones.
	size := func() int {
		n := 0
		for i := range keep {
			n += len([]rune(truncate(h[i].Text, maxHistoryMessage)))
		}
		return n
	}
	for i := 0; i < len(h)-historyRecent && size() > historyBudget; i++ {
		delete(keep, i)
	}
	var out []Message
	for i, m := range h {
		if keep[i] {
			out = append(out, m)
		}
	}
	return out, len(h) - len(out)
}
