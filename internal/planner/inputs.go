package planner

import (
	"fmt"
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
	maxHistoryMessage  = 600
	maxReplanDetail    = 400
	maxReplanNodes     = 24
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
	goal, session, capabilities, previous string
	flags                                 []InjectionFlag
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
		for _, m := range in.History {
			fmt.Fprintf(&b, "%s: %s\n", roleLabel(m.Role), truncate(m.Text, maxHistoryMessage))
		}
		p.session = wrap("session", "source=session", b.String())
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
