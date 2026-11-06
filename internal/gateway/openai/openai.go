// Package openai is an OpenAI-compatible chat/completions adapter. It covers
// self-hosted vLLM and SGLang servers as well as hosted compatible APIs.
package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	gw "go-sre-harness/internal/gateway"
)

type Config struct {
	BaseURL string // e.g. http://vllm:8000/v1
	APIKey  string // optional for self-hosted servers
	// UseMaxCompletionTokens sends max_completion_tokens instead of max_tokens.
	UseMaxCompletionTokens bool
	HTTPClient             *http.Client
}

type Provider struct{ cfg Config }

func New(cfg Config) *Provider {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Minute}
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	return &Provider{cfg: cfg}
}

func (p *Provider) ID() string { return "openai" }

type wireMsg struct {
	Role       string     `json:"role"`
	Content    *string    `json:"content"`
	ToolCalls  []wireCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

type wireCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

func str(s string) *string { return &s }

func (p *Provider) body(ep gw.Endpoint, req gw.Request) ([]byte, error) {
	var msgs []wireMsg
	var sys []string
	for _, s := range req.System {
		sys = append(sys, s.Text)
	}
	if len(sys) > 0 {
		msgs = append(msgs, wireMsg{Role: "system", Content: str(strings.Join(sys, "\n\n"))})
	}
	for _, m := range req.Messages {
		var text []string
		var calls []wireCall
		for _, part := range m.Parts {
			switch part.Type {
			case gw.PartText:
				text = append(text, part.Text)
			case gw.PartToolUse:
				c := wireCall{ID: part.ToolUseID, Type: "function"}
				c.Function.Name = gw.WireToolName(part.ToolName)
				c.Function.Arguments = string(part.Input)
				calls = append(calls, c)
			case gw.PartToolResult:
				content := part.Text
				if part.IsError {
					content = "ERROR: " + content
				}
				msgs = append(msgs, wireMsg{Role: "tool", ToolCallID: part.ToolUseID, Content: str(content)})
			}
		}
		if len(text) == 0 && len(calls) == 0 {
			continue
		}
		wm := wireMsg{Role: string(m.Role), ToolCalls: calls}
		if len(text) > 0 {
			wm.Content = str(strings.Join(text, "\n"))
		}
		msgs = append(msgs, wm)
	}
	maxTok := req.MaxOutputTokens
	if maxTok <= 0 || (ep.MaxOutput > 0 && maxTok > ep.MaxOutput) {
		maxTok = ep.MaxOutput
	}
	body := map[string]any{
		"model":          ep.ModelID,
		"messages":       msgs,
		"stream":         true,
		"stream_options": map[string]any{"include_usage": true},
	}
	if p.cfg.UseMaxCompletionTokens {
		body["max_completion_tokens"] = maxTok
	} else {
		body["max_tokens"] = maxTok
	}
	if len(req.Tools) > 0 {
		var tools []map[string]any
		for _, t := range req.Tools {
			schema := json.RawMessage(t.InputSchema)
			if len(schema) == 0 {
				schema = json.RawMessage(`{"type":"object","properties":{}}`)
			}
			tools = append(tools, map[string]any{"type": "function", "function": map[string]any{
				"name": gw.WireToolName(t.Name), "description": t.Description, "parameters": schema}})
		}
		body["tools"] = tools
		body["tool_choice"] = "auto"
	}
	return json.Marshal(body)
}

type chunk struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Delta struct {
			Content   *string `json:"content"`
			Reasoning *string `json:"reasoning_content"`
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens        int64 `json:"prompt_tokens"`
		CompletionTokens    int64 `json:"completion_tokens"`
		PromptTokensDetails *struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

type pending struct {
	id, name string
	args     strings.Builder
}

func (p *Provider) Stream(ctx context.Context, ep gw.Endpoint, req gw.Request, emit gw.Sink) error {
	body, err := p.body(ep, req)
	if err != nil {
		return err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return &gw.Error{Code: gw.ErrCapabilityMismatch, Msg: err.Error()}
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/event-stream")
	if p.cfg.APIKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
	}
	resp, err := p.cfg.HTTPClient.Do(hreq)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &gw.Error{Code: gw.ErrProviderUnavailable, Msg: err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		e := &gw.Error{Status: resp.StatusCode, Msg: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(msg))}
		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			e.Code = gw.ErrRateLimited
			if s, perr := strconv.Atoi(resp.Header.Get("Retry-After")); perr == nil {
				e.RetryAfter = time.Duration(s) * time.Second
			}
		case resp.StatusCode == http.StatusBadRequest && bytes.Contains(bytes.ToLower(msg), []byte("context length")):
			e.Code = gw.ErrContextWindowExceeded
		case resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 401 && resp.StatusCode != 403:
			e.Code = gw.ErrCapabilityMismatch
		default:
			e.Code = gw.ErrProviderUnavailable
		}
		return e
	}

	calls := map[int]*pending{}
	finish := ""
	var usage *gw.Usage
	started, completed := false, false
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		data, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			completed = true
			break
		}
		var c chunk
		if err := json.Unmarshal([]byte(data), &c); err != nil {
			return &gw.Error{Code: gw.ErrStreamInterrupted, Sent: true, Msg: "bad chunk: " + err.Error()}
		}
		if c.Error != nil {
			return &gw.Error{Code: gw.ErrStreamInterrupted, Sent: true, Msg: c.Error.Message}
		}
		if !started {
			started = true
			emit(gw.Event{Type: gw.EvStarted, ProviderRequestID: c.ID, ResolvedModel: c.Model})
		}
		for _, ch := range c.Choices {
			if ch.Delta.Content != nil && *ch.Delta.Content != "" {
				emit(gw.Event{Type: gw.EvTextDelta, Text: *ch.Delta.Content})
			}
			if ch.Delta.Reasoning != nil && *ch.Delta.Reasoning != "" {
				emit(gw.Event{Type: gw.EvReasoningSummary, Text: *ch.Delta.Reasoning})
			}
			for _, tc := range ch.Delta.ToolCalls {
				pc := calls[tc.Index]
				if pc == nil {
					pc = &pending{}
					calls[tc.Index] = pc
				}
				if tc.ID != "" {
					pc.id = tc.ID
				}
				if tc.Function.Name != "" {
					pc.name = tc.Function.Name
				}
				pc.args.WriteString(tc.Function.Arguments)
				emit(gw.Event{Type: gw.EvToolCallDelta, Index: tc.Index, ToolCallID: pc.id, ToolName: tc.Function.Name,
					ArgsDelta: tc.Function.Arguments})
			}
			if ch.FinishReason != nil && *ch.FinishReason != "" {
				finish = *ch.FinishReason
			}
		}
		if c.Usage != nil {
			u := gw.Usage{InputTokens: c.Usage.PromptTokens, OutputTokens: c.Usage.CompletionTokens}
			if d := c.Usage.PromptTokensDetails; d != nil {
				u.CachedInputTokens = d.CachedTokens
				u.InputTokens -= d.CachedTokens // normalize to uncached input
			}
			usage = &u
			emit(gw.Event{Type: gw.EvUsage, Usage: &u})
		}
	}
	if err := sc.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &gw.Error{Code: gw.ErrStreamInterrupted, Sent: true, Msg: err.Error()}
	}
	if !completed && finish == "" {
		return &gw.Error{Code: gw.ErrStreamInterrupted, Sent: true, Msg: "stream closed before [DONE]"}
	}
	// OpenAI tool calls have no per-call end marker: they are complete when
	// the choice finishes.
	idx := make([]int, 0, len(calls))
	for i := range calls {
		idx = append(idx, i)
	}
	sort.Ints(idx)
	if finish == "tool_calls" || finish == "stop" || finish == "" {
		for _, i := range idx {
			pc := calls[i]
			emit(gw.Event{Type: gw.EvToolCallComplete, Index: i, ToolCallID: pc.id, ToolName: pc.name,
				Args: json.RawMessage(nonEmpty(pc.args.String()))})
		}
	}
	switch finish {
	case "tool_calls":
		finish = "tool_use"
	case "stop":
		finish = "end_turn"
	case "content_filter":
		finish = "refusal"
	}
	emit(gw.Event{Type: gw.EvCompleted, FinishReason: finish, Usage: usage})
	return nil
}

func nonEmpty(s string) string {
	if strings.TrimSpace(s) == "" {
		return "{}"
	}
	return s
}
