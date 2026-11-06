// Package scripted is a deterministic in-process model provider. It streams a
// scripted response through the same normalized event model as a real
// provider, so the full gateway path (ledger, routing, aggregation, tool-call
// validation, delta streaming) runs without an API key, in tests and in
// recorded-replay evaluation.
package scripted

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	gw "go-sre-harness/internal/gateway"
)

type Script struct {
	Text      string
	ToolCalls []gw.ToolCall
	Finish    string // default: tool_use when calls exist, else end_turn
	Err       *gw.Error
}

type Provider struct {
	Name string
	Fn   func(req gw.Request) (Script, error)
}

func New(fn func(gw.Request) (Script, error)) *Provider { return &Provider{Name: "scripted", Fn: fn} }

func (p *Provider) ID() string { return p.Name }

func (p *Provider) Stream(ctx context.Context, ep gw.Endpoint, req gw.Request, emit gw.Sink) error {
	s, err := p.Fn(req)
	if err != nil {
		return err
	}
	if s.Err != nil {
		return s.Err
	}
	emit(gw.Event{Type: gw.EvStarted, ProviderRequestID: "scripted", ResolvedModel: ep.ModelID})
	for _, chunk := range splitChunks(s.Text, 24) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		emit(gw.Event{Type: gw.EvTextDelta, Text: chunk})
	}
	for i, c := range s.ToolCalls {
		if c.ID == "" {
			c.ID = fmt.Sprintf("call_%d", i)
		}
		wire := gw.WireToolName(c.Name)
		emit(gw.Event{Type: gw.EvToolCallDelta, Index: i + 1, ToolCallID: c.ID, ToolName: wire})
		for _, chunk := range splitChunks(string(c.Args), 16) {
			emit(gw.Event{Type: gw.EvToolCallDelta, Index: i + 1, ToolCallID: c.ID, ArgsDelta: chunk})
		}
		emit(gw.Event{Type: gw.EvToolCallComplete, Index: i + 1, ToolCallID: c.ID, ToolName: wire})
	}
	finish := s.Finish
	if finish == "" {
		finish = "end_turn"
		if len(s.ToolCalls) > 0 {
			finish = "tool_use"
		}
	}
	in, _ := json.Marshal(req)
	out := int64(len(s.Text)/4 + 8)
	for _, c := range s.ToolCalls {
		out += int64(len(c.Args)/4 + 4)
	}
	u := gw.Usage{InputTokens: int64(len(in) / 4), OutputTokens: out}
	emit(gw.Event{Type: gw.EvCompleted, FinishReason: finish, Usage: &u})
	return nil
}

func splitChunks(s string, n int) []string {
	if s == "" {
		return nil
	}
	var out []string
	r := []rune(s)
	for len(r) > 0 {
		k := n
		if k > len(r) {
			k = len(r)
		}
		out = append(out, string(r[:k]))
		r = r[k:]
	}
	return out
}

// LastToolResults returns the tool_result texts of the final user message,
// keyed by tool name (resolved through the preceding assistant tool_use).
func LastToolResults(req gw.Request) map[string]string {
	names := map[string]string{}
	out := map[string]string{}
	for _, m := range req.Messages {
		for _, p := range m.Parts {
			switch p.Type {
			case gw.PartToolUse:
				names[p.ToolUseID] = p.ToolName
			case gw.PartToolResult:
				out[names[p.ToolUseID]] = p.Text
			}
		}
	}
	return out
}

// Transcript flattens all text parts (used by scripted policies to read the goal).
func Transcript(req gw.Request) string {
	var b strings.Builder
	for _, m := range req.Messages {
		for _, p := range m.Parts {
			if p.Type == gw.PartText {
				b.WriteString(p.Text)
				b.WriteByte('\n')
			}
		}
	}
	return b.String()
}
