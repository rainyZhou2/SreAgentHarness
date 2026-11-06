package cognitive

import (
	"context"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/gateway/scripted"
	"go-sre-harness/internal/tools"
)

var (
	obsHeader = regexp.MustCompile(`^\[observation step=(\d+) tool=(\S+) outcome=(\S+) trust=(\S*)\]\n?`)
	evHeader  = regexp.MustCompile(`^\[evidence ref=kv:(\S+)@(\d+) source=(\S*) actor=(\S*) [^\]]*\]\n?`)
)

// ParseRequest reconstructs a ReasoningInput from a rendered request. It is
// the inverse of Assembler.Build for the parts a scripted policy needs, which
// lets deterministic strategies run behind the real gateway path.
func ParseRequest(req gw.Request) ReasoningInput {
	var in ReasoningInput
	for _, m := range req.Messages {
		if m.Role != gw.RoleUser {
			continue
		}
		for _, p := range m.Parts {
			t := p.Text
			switch {
			case strings.HasPrefix(t, "[goal]\n"):
				in.Goal = strings.TrimPrefix(t, "[goal]\n")
			case obsHeader.MatchString(t):
				g := obsHeader.FindStringSubmatch(t)
				seq, _ := strconv.ParseInt(g[1], 10, 64)
				body := t[len(g[0]):]
				o := domain.Observation{StepSeq: seq, Tool: g[2], Outcome: g[3], TrustTag: g[4]}
				if note, ok := cutNote(body); ok {
					o.Note = note
					body = strings.TrimSpace(body[:strings.LastIndex(body, "\nnote: ")])
				}
				if json.Valid([]byte(body)) {
					o.Content = json.RawMessage(body)
				}
				in.Observations = append(in.Observations, o)
			case evHeader.MatchString(t):
				g := evHeader.FindStringSubmatch(t)
				v, _ := strconv.ParseInt(g[2], 10, 64)
				in.Evidence = append(in.Evidence, domain.Evidence{Key: g[1], Version: v, Source: g[3], ActorID: g[4],
					Content: t[len(g[0]):]})
			}
		}
	}
	for _, t := range req.Tools {
		if t.Name != ConclusionTool {
			in.Tools = append(in.Tools, toolsDescriptor(t))
		}
	}
	return in
}

func cutNote(body string) (string, bool) {
	i := strings.LastIndex(body, "\nnote: ")
	if i < 0 {
		if strings.HasPrefix(body, "note: ") {
			return strings.TrimPrefix(body, "note: "), true
		}
		return "", false
	}
	return body[i+len("\nnote: "):], true
}

// ScriptedModel adapts a Strategy into a scripted gateway provider: the
// strategy decides, and its decision is streamed back as tool calls exactly
// like a real model's output.
func ScriptedModel(s Strategy) *scripted.Provider {
	return scripted.New(func(req gw.Request) (scripted.Script, error) {
		in := ParseRequest(req)
		d, err := s.Decide(context.Background(), in)
		if err != nil {
			return scripted.Script{}, err
		}
		if d.Kind == domain.DecisionTool && d.Tool != nil {
			return scripted.Script{Text: d.ReasonSummary, ToolCalls: []gw.ToolCall{{Name: d.Tool.Name, Args: d.Tool.Args}}}, nil
		}
		concl := map[string]any{"answer": d.Answer, "reason_summary": d.ReasonSummary}
		if len(d.Hypotheses) > 0 {
			concl["hypotheses"] = d.Hypotheses
		}
		if len(d.EvidenceRefs) > 0 {
			concl["evidence_refs"] = d.EvidenceRefs
		}
		args, _ := json.Marshal(concl)
		return scripted.Script{Text: d.ReasonSummary, ToolCalls: []gw.ToolCall{{Name: ConclusionTool, Args: args}}}, nil
	})
}

func toolsDescriptor(t gw.ToolSchema) tools.Descriptor {
	return tools.Descriptor{Name: t.Name, InputSchema: t.InputSchema}
}
