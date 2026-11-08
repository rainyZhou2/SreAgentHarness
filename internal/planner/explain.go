package planner

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"go-sre-harness/internal/flow"
)

// Review is the human-readable view of a plan shown to approvers: what will
// run in which order, what can change state, how the dry run went, and how
// the plan differs from the one it replaces or from the fallback runbook.
type Review struct {
	Summary    string      `json:"summary"`
	Steps      []string    `json:"steps"`
	Risks      []string    `json:"risks,omitempty"`
	Simulation *Simulation `json:"simulation,omitempty"`
	// DiffBase names what Diff compares against: "previous_generation" or
	// "fallback:<profile>@<version>".
	DiffBase string   `json:"diff_base,omitempty"`
	Diff     []string `json:"diff,omitempty"`
}

// Explain renders a plan for review.
func Explain(spec flow.Spec, a *Analysis, sim *Simulation, rationale string) Review {
	r := Review{Simulation: sim}
	r.Steps = explainSteps(spec, "")
	if a != nil {
		parts := []string{fmt.Sprintf("%d nodes", a.Nodes)}
		if a.SideEffects > 0 {
			parts = append(parts, fmt.Sprintf("%d side-effecting tool use(s), highest risk %s", a.SideEffects, a.HighestRisk))
		} else {
			parts = append(parts, "read-only")
		}
		if a.ApprovalNodes > 0 {
			parts = append(parts, fmt.Sprintf("%d approval gate(s)", a.ApprovalNodes))
		}
		parts = append(parts, fmt.Sprintf("up to %d child runs and %d model calls", a.EstimatedRuns, a.EstimatedModelCalls))
		if a.EstimatedOutputTokens > 0 {
			parts = append(parts, fmt.Sprintf("~%d output tokens expected (worst case %d)", a.EstimatedOutputTokens, a.WorstCaseOutputTokens))
		}
		r.Summary = strings.Join(parts, "; ")
		for _, f := range a.Findings {
			if f.Severity == "warning" || f.Severity == "error" {
				r.Risks = append(r.Risks, f.Message)
			}
		}
	}
	if rationale = strings.TrimSpace(rationale); rationale != "" {
		r.Summary = clipRunes(oneLine(rationale), 300) + " — " + r.Summary
	}
	if sim != nil {
		if sim.Completed {
			r.Steps = append(r.Steps, fmt.Sprintf("dry run: default path completes in %d steps via %s",
				sim.Steps, strings.Join(sim.Path, " → ")))
		} else {
			r.Risks = append(r.Risks, "dry run did not complete: "+sim.Failure)
		}
	}
	return r
}

func explainSteps(spec flow.Spec, indent string) []string {
	var out []string
	for _, n := range orderedNodes(spec) {
		line := indent + "- " + n.ID + ": "
		switch n.Type {
		case flow.NodeTool:
			line += "run tool " + n.Tool
			if len(n.Args) > 0 {
				line += " with " + clipRunes(string(n.Args), 160)
			}
		case flow.NodeAgent:
			role := n.Role
			if role == "" {
				role = "agent"
			}
			line += fmt.Sprintf("%s agent", role)
			if len(n.Tools) > 0 {
				line += " using " + strings.Join(n.Tools, ", ")
			} else {
				line += " using any allowed tool"
			}
			if n.MaxSteps > 0 {
				line += fmt.Sprintf(" (≤%d steps)", n.MaxSteps)
			}
		case flow.NodeApproval:
			approvers := n.Approvers
			if approvers < 1 {
				approvers = 1
			}
			line += fmt.Sprintf("wait for %d human approval(s) at risk %s", approvers, n.Risk)
		case flow.NodeInput:
			line += "ask a human for " + n.Output
		case flow.NodeParallel:
			line += "run in parallel: " + strings.Join(n.Branches, ", ")
		case flow.NodeJoin:
			line += "wait for " + strings.Join(n.Wait, ", ")
			if n.Join != "" {
				line += " (" + n.Join + ")"
			}
		case flow.NodeLoop:
			line += fmt.Sprintf("repeat %s until %s (≤%d times)", n.Body, n.Until, n.Iterations)
		case flow.NodeSubflow:
			line += "subflow"
		case flow.NodeEnd:
			line += "finish"
		default:
			line += string(n.Type)
		}
		var edges []string
		for _, e := range n.Next {
			if e.When != "" {
				edges = append(edges, e.To+" if "+e.When)
			} else {
				edges = append(edges, e.To)
			}
		}
		if len(edges) > 0 {
			line += " → " + strings.Join(edges, "; else ")
		}
		out = append(out, line)
		if n.Type == flow.NodeSubflow && n.Flow != nil {
			out = append(out, explainSteps(*n.Flow, indent+"  ")...)
		}
	}
	return out
}

// orderedNodes returns nodes in breadth-first order from the entry, then any
// unreachable ones in spec order.
func orderedNodes(spec flow.Spec) []flow.Node {
	index := map[string]flow.Node{}
	for _, n := range spec.Nodes {
		index[n.ID] = n
	}
	seen := map[string]bool{}
	var out []flow.Node
	queue := []string{spec.Entry}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		n, ok := index[id]
		if !ok || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, n)
		queue = append(queue, n.Branches...)
		if n.Body != "" {
			queue = append(queue, n.Body)
		}
		for _, e := range n.Next {
			queue = append(queue, e.To)
		}
	}
	for _, n := range spec.Nodes {
		if !seen[n.ID] {
			out = append(out, n)
		}
	}
	return out
}

// Diff lists node-level changes from base to next.
func Diff(base, next flow.Spec) []string {
	b, n := map[string]flow.Node{}, map[string]flow.Node{}
	for _, x := range base.Nodes {
		b[x.ID] = x
	}
	for _, x := range next.Nodes {
		n[x.ID] = x
	}
	var out []string
	for _, x := range next.Nodes {
		old, ok := b[x.ID]
		if !ok {
			out = append(out, fmt.Sprintf("+ %s (%s%s)", x.ID, x.Type, nodeTarget(x)))
			continue
		}
		if changes := nodeChanges(old, x); len(changes) > 0 {
			out = append(out, fmt.Sprintf("~ %s: %s", x.ID, strings.Join(changes, ", ")))
		}
	}
	for _, x := range base.Nodes {
		if _, ok := n[x.ID]; !ok {
			out = append(out, fmt.Sprintf("- %s (%s%s)", x.ID, x.Type, nodeTarget(x)))
		}
	}
	if base.Entry != next.Entry {
		out = append(out, fmt.Sprintf("~ entry: %s → %s", base.Entry, next.Entry))
	}
	if base.Limits != next.Limits {
		out = append(out, fmt.Sprintf("~ limits: %+v → %+v", base.Limits, next.Limits))
	}
	return out
}

func nodeTarget(n flow.Node) string {
	switch {
	case n.Tool != "":
		return " " + n.Tool
	case len(n.Tools) > 0:
		return " " + strings.Join(n.Tools, ",")
	}
	return ""
}

func nodeChanges(a, b flow.Node) []string {
	var c []string
	if a.Type != b.Type {
		c = append(c, fmt.Sprintf("type %s → %s", a.Type, b.Type))
	}
	if a.Tool != b.Tool {
		c = append(c, fmt.Sprintf("tool %s → %s", a.Tool, b.Tool))
	}
	if strings.Join(sortedCopy(a.Tools), ",") != strings.Join(sortedCopy(b.Tools), ",") {
		c = append(c, fmt.Sprintf("tools [%s] → [%s]", strings.Join(a.Tools, ","), strings.Join(b.Tools, ",")))
	}
	if !jsonEqual(a.Args, b.Args) {
		c = append(c, "args changed")
	}
	if a.Risk != b.Risk || a.Approvers != b.Approvers {
		c = append(c, fmt.Sprintf("approval %s/%d → %s/%d", a.Risk, a.Approvers, b.Risk, b.Approvers))
	}
	if a.MaxSteps != b.MaxSteps {
		c = append(c, fmt.Sprintf("max_steps %d → %d", a.MaxSteps, b.MaxSteps))
	}
	ea, _ := json.Marshal(a.Next)
	eb, _ := json.Marshal(b.Next)
	if string(ea) != string(eb) {
		c = append(c, "edges changed")
	}
	if a.Role != b.Role || a.Prompt != b.Prompt {
		c = append(c, "role/prompt changed")
	}
	return c
}

func sortedCopy(xs []string) []string {
	out := append([]string(nil), xs...)
	sort.Strings(out)
	return out
}

func jsonEqual(a, b json.RawMessage) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return string(a) == string(b)
	}
	ja, _ := json.Marshal(x)
	jb, _ := json.Marshal(y)
	return string(ja) == string(jb)
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func clipRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
