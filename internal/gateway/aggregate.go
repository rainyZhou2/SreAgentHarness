package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Provider is a model adapter. Stream sends one request to one endpoint and
// emits normalized events; it must emit EvToolCallComplete only once a tool
// call's content block has ended (§35.3), and must classify every error as a
// *gateway.Error (Sent=false only when nothing reached the provider).
type Provider interface {
	ID() string
	Stream(ctx context.Context, ep Endpoint, req Request, emit Sink) error
}

// Aggregator folds the normalized event stream into a Response and validates
// tool calls before anything downstream (policy gate) can see them.
type Aggregator struct {
	start     time.Time
	firstTok  time.Time
	text      strings.Builder
	calls     map[int]*partialCall
	completed []ToolCall
	resp      Response
	tools     map[string]ToolSchema
	finished  bool
}

type partialCall struct {
	id, name string
	args     bytes.Buffer
}

func NewAggregator(tools []ToolSchema) *Aggregator {
	m := map[string]ToolSchema{}
	for _, t := range tools {
		m[t.Name] = t
	}
	return &Aggregator{start: time.Now(), calls: map[int]*partialCall{}, tools: m}
}

func (a *Aggregator) Add(ev Event) {
	switch ev.Type {
	case EvStarted:
		if ev.ProviderRequestID != "" {
			a.resp.ProviderRequestID = ev.ProviderRequestID
		}
		if ev.ResolvedModel != "" {
			a.resp.ResolvedModel = ev.ResolvedModel
		}
	case EvTextDelta:
		a.markFirst()
		a.text.WriteString(ev.Text)
	case EvReasoningSummary:
		a.markFirst()
	case EvToolCallDelta:
		a.markFirst()
		pc := a.calls[ev.Index]
		if pc == nil {
			pc = &partialCall{}
			a.calls[ev.Index] = pc
		}
		if ev.ToolCallID != "" {
			pc.id = ev.ToolCallID
		}
		if ev.ToolName != "" {
			pc.name = ev.ToolName
		}
		pc.args.WriteString(ev.ArgsDelta)
	case EvToolCallComplete:
		a.markFirst()
		call := ToolCall{ID: ev.ToolCallID, Name: ev.ToolName, Args: ev.Args}
		if pc := a.calls[ev.Index]; pc != nil {
			if call.ID == "" {
				call.ID = pc.id
			}
			if call.Name == "" {
				call.Name = pc.name
			}
			if len(call.Args) == 0 {
				call.Args = append(json.RawMessage(nil), pc.args.Bytes()...)
			}
			delete(a.calls, ev.Index)
		}
		a.completed = append(a.completed, call)
	case EvUsage:
		if ev.Usage != nil {
			a.resp.Usage = *ev.Usage
		}
	case EvCompleted:
		a.finished = true
		a.resp.FinishReason = ev.FinishReason
		if ev.Usage != nil {
			a.resp.Usage = *ev.Usage
		}
	}
}

func (a *Aggregator) markFirst() {
	if a.firstTok.IsZero() {
		a.firstTok = time.Now()
	}
}

// TTFT is the time to the first content event (0 if none yet).
func (a *Aggregator) TTFT() time.Duration {
	if a.firstTok.IsZero() {
		return 0
	}
	return a.firstTok.Sub(a.start)
}

// Usage returns the usage reported so far (partial on interruption).
func (a *Aggregator) Usage() Usage { return a.resp.Usage }

// Result validates and returns the aggregated response. Unfinished tool
// calls, invalid JSON or schema violations are INVALID_TOOL_CALL (§31.3); a
// stream that never completed is STREAM_INTERRUPTED.
func (a *Aggregator) Result() (Response, error) {
	if !a.finished {
		return Response{}, errf(ErrStreamInterrupted, true, "stream ended without completion")
	}
	switch a.resp.FinishReason {
	case "refusal":
		return Response{}, errf(ErrRefused, true, "model refused")
	case "max_tokens", "length":
		if len(a.calls) > 0 || len(a.completed) == 0 {
			return Response{}, errf(ErrOutputTruncated, true, "output truncated at max tokens")
		}
	case "model_context_window_exceeded":
		return Response{}, errf(ErrContextWindowExceeded, true, "context window exceeded")
	}
	if len(a.calls) > 0 {
		return Response{}, errf(ErrInvalidToolCall, true, "%d tool call(s) never completed", len(a.calls))
	}
	r := a.resp
	r.Text = a.text.String()
	for _, c := range a.completed {
		if len(bytes.TrimSpace(c.Args)) == 0 {
			c.Args = json.RawMessage(`{}`)
		}
		if err := a.validate(c); err != nil {
			return Response{}, err
		}
		r.ToolCalls = append(r.ToolCalls, c)
	}
	r.TTFT = a.TTFT()
	return r, nil
}

func (a *Aggregator) validate(c ToolCall) error {
	if c.Name == "" {
		return errf(ErrInvalidToolCall, true, "tool call without name")
	}
	raw := c.Args
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var args map[string]any
	if err := json.Unmarshal(raw, &args); err != nil || args == nil {
		return errf(ErrInvalidToolCall, true, "tool %s: arguments are not a JSON object: %v", c.Name, err)
	}
	schema, ok := a.tools[c.Name]
	if !ok {
		// Unknown tools are not an INVALID_TOOL_CALL here: the policy gate
		// denies them and the denial becomes an observation (§4.6).
		return nil
	}
	if strings.HasPrefix(schema.Source, "mcp:") {
		// The MCP server owns its schema; a rejected call comes back as an
		// observation so the model can correct it instead of retrying the
		// whole reason step.
		return nil
	}
	return ValidateArgs(schema.InputSchema, args)
}

// ValidateArgs checks the subset of JSON Schema the tool registry uses:
// required properties, primitive types and enums at the top level.
func ValidateArgs(schemaJSON json.RawMessage, args map[string]any) error {
	if len(schemaJSON) == 0 {
		return nil
	}
	var s struct {
		Required   []string `json:"required"`
		Properties map[string]struct {
			Type string `json:"type"`
			Enum []any  `json:"enum"`
		} `json:"properties"`
		AdditionalProperties *bool `json:"additionalProperties"`
	}
	if err := json.Unmarshal(schemaJSON, &s); err != nil {
		return nil
	}
	for _, r := range s.Required {
		if v, ok := args[r]; !ok || v == nil {
			return errf(ErrInvalidToolCall, true, "missing required argument %q", r)
		}
	}
	keys := make([]string, 0, len(args))
	for k := range args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		p, ok := s.Properties[k]
		if !ok {
			if s.AdditionalProperties != nil && !*s.AdditionalProperties {
				return errf(ErrInvalidToolCall, true, "unexpected argument %q", k)
			}
			continue
		}
		if !typeMatches(p.Type, args[k]) {
			return errf(ErrInvalidToolCall, true, "argument %q: expected %s", k, p.Type)
		}
		if len(p.Enum) > 0 {
			found := false
			for _, e := range p.Enum {
				if fmt.Sprint(e) == fmt.Sprint(args[k]) {
					found = true
				}
			}
			if !found {
				return errf(ErrInvalidToolCall, true, "argument %q: value not allowed", k)
			}
		}
	}
	return nil
}

func typeMatches(t string, v any) bool {
	switch t {
	case "", "any":
		return true
	case "string":
		_, ok := v.(string)
		return ok
	case "number":
		_, ok := v.(float64)
		return ok
	case "integer":
		f, ok := v.(float64)
		return ok && f == float64(int64(f))
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "object":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	}
	return true
}
