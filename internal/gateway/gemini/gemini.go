package gemini

// Package gemini adapts the Google Gemini API (generativelanguage, v1beta
// streamGenerateContent with server-sent events) to the gateway's
// provider-neutral event model.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	gw "go-sre-harness/internal/gateway"
)

// DefaultBaseURL is the public Gemini API.
const DefaultBaseURL = "https://generativelanguage.googleapis.com/v1beta"

type Config struct {
	APIKey  string
	BaseURL string // default DefaultBaseURL; tests point it at an httptest server
	// HTTPClient overrides the client (tenant models pass one that enforces
	// their network policy).
	HTTPClient *http.Client
}

type Provider struct{ cfg Config }

func New(cfg Config) *Provider {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Minute}
	}
	return &Provider{cfg: cfg}
}

func (p *Provider) ID() string { return "gemini" }

type part struct {
	Text             string        `json:"text,omitempty"`
	Thought          bool          `json:"thought,omitempty"`
	FunctionCall     *functionCall `json:"functionCall,omitempty"`
	FunctionResponse *functionResp `json:"functionResponse,omitempty"`
}

type functionCall struct {
	ID   string          `json:"id,omitempty"`
	Name string          `json:"name"`
	Args json.RawMessage `json:"args,omitempty"`
}

type functionResp struct {
	ID       string         `json:"id,omitempty"`
	Name     string         `json:"name"`
	Response map[string]any `json:"response"`
}

type content struct {
	Role  string `json:"role,omitempty"`
	Parts []part `json:"parts"`
}

func (p *Provider) body(ep gw.Endpoint, req gw.Request) ([]byte, error) {
	body := map[string]any{}
	var sys []string
	for _, s := range req.System {
		sys = append(sys, s.Text)
	}
	if len(sys) > 0 {
		body["systemInstruction"] = content{Parts: []part{{Text: strings.Join(sys, "\n\n")}}}
	}
	// Function responses are matched by name; remember the name of every call.
	callName := map[string]string{}
	var contents []content
	for _, m := range req.Messages {
		role := "user"
		if m.Role == gw.RoleAssistant {
			role = "model"
		}
		c := content{Role: role}
		for _, pt := range m.Parts {
			switch pt.Type {
			case gw.PartText:
				if pt.Text != "" {
					c.Parts = append(c.Parts, part{Text: pt.Text})
				}
			case gw.PartToolUse:
				name := gw.WireToolName(pt.ToolName)
				callName[pt.ToolUseID] = name
				args := pt.Input
				if len(bytes.TrimSpace(args)) == 0 {
					args = json.RawMessage(`{}`)
				}
				c.Parts = append(c.Parts, part{FunctionCall: &functionCall{ID: pt.ToolUseID, Name: name, Args: args}})
			case gw.PartToolResult:
				resp := map[string]any{"content": pt.Text}
				if pt.IsError {
					resp = map[string]any{"error": pt.Text}
				}
				c.Parts = append(c.Parts, part{FunctionResponse: &functionResp{ID: pt.ToolUseID,
					Name: callName[pt.ToolUseID], Response: resp}})
			}
		}
		if len(c.Parts) > 0 {
			contents = append(contents, c)
		}
	}
	body["contents"] = contents
	maxTok := req.MaxOutputTokens
	if maxTok <= 0 || (ep.MaxOutput > 0 && maxTok > ep.MaxOutput) {
		maxTok = ep.MaxOutput
	}
	gen := map[string]any{"thinkingConfig": map[string]any{"includeThoughts": true}}
	if maxTok > 0 {
		gen["maxOutputTokens"] = maxTok
	}
	body["generationConfig"] = gen
	if len(req.Tools) > 0 {
		var decls []map[string]any
		for _, t := range req.Tools {
			d := map[string]any{"name": gw.WireToolName(t.Name), "description": t.Description}
			if schema := cleanSchema(t.InputSchema); schema != nil {
				d["parameters"] = schema
			}
			decls = append(decls, d)
		}
		body["tools"] = []map[string]any{{"functionDeclarations": decls}}
		body["toolConfig"] = map[string]any{"functionCallingConfig": map[string]any{"mode": "AUTO"}}
	}
	return json.Marshal(body)
}

// unsupportedSchemaKeys are JSON Schema keywords Gemini's parameter schema
// (an OpenAPI subset) rejects.
var unsupportedSchemaKeys = map[string]bool{"additionalProperties": true, "$schema": true, "$id": true, "$ref": true,
	"$defs": true, "definitions": true, "patternProperties": true, "unevaluatedProperties": true}

// cleanSchema drops the keywords Gemini rejects; an object without
// properties is sent as no schema at all.
func cleanSchema(raw json.RawMessage) any {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return nil
	}
	v = clean(v)
	if m, ok := v.(map[string]any); ok {
		if props, _ := m["properties"].(map[string]any); m["type"] == "object" && len(props) == 0 {
			return nil
		}
	}
	return v
}

func clean(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			if unsupportedSchemaKeys[k] {
				continue
			}
			if k == "properties" {
				props := map[string]any{}
				if pm, ok := val.(map[string]any); ok {
					for name, ps := range pm {
						props[name] = clean(ps) // property names are data, not keywords
					}
				}
				out[k] = props
				continue
			}
			out[k] = clean(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = clean(e)
		}
		return out
	}
	return v
}

type chunk struct {
	Candidates []struct {
		Content struct {
			Parts []part `json:"parts"`
		} `json:"content"`
		FinishReason string `json:"finishReason"`
	} `json:"candidates"`
	PromptFeedback *struct {
		BlockReason string `json:"blockReason"`
	} `json:"promptFeedback"`
	UsageMetadata *struct {
		PromptTokenCount        int64 `json:"promptTokenCount"`
		CandidatesTokenCount    int64 `json:"candidatesTokenCount"`
		CachedContentTokenCount int64 `json:"cachedContentTokenCount"`
		ThoughtsTokenCount      int64 `json:"thoughtsTokenCount"`
	} `json:"usageMetadata"`
	ModelVersion string `json:"modelVersion"`
	ResponseID   string `json:"responseId"`
	Error        *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

func (p *Provider) Stream(ctx context.Context, ep gw.Endpoint, req gw.Request, emit gw.Sink) error {
	body, err := p.body(ep, req)
	if err != nil {
		return &gw.Error{Code: gw.ErrCapabilityMismatch, Msg: err.Error()}
	}
	u := p.cfg.BaseURL + "/models/" + url.PathEscape(ep.ModelID) + ":streamGenerateContent?alt=sse"
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return &gw.Error{Code: gw.ErrCapabilityMismatch, Msg: err.Error()}
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/event-stream")
	hreq.Header.Set("x-goog-api-key", p.cfg.APIKey) // a header, so the key never appears in URLs or logs
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
		return statusError(resp, bytes.TrimSpace(msg))
	}

	started := false
	finish := ""
	index := 0
	var usage *gw.Usage
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "" {
			continue
		}
		var c chunk
		if err := json.Unmarshal([]byte(data), &c); err != nil {
			return &gw.Error{Code: gw.ErrStreamInterrupted, Sent: true, Msg: "bad chunk: " + err.Error()}
		}
		if c.Error != nil {
			return &gw.Error{Code: gw.ErrStreamInterrupted, Sent: true, Msg: c.Error.Status + ": " + c.Error.Message}
		}
		if !started {
			started = true
			emit(gw.Event{Type: gw.EvStarted, ProviderRequestID: c.ResponseID, ResolvedModel: c.ModelVersion})
		}
		if c.PromptFeedback != nil && c.PromptFeedback.BlockReason != "" {
			finish = "refusal"
		}
		for _, cand := range c.Candidates {
			for _, pt := range cand.Content.Parts {
				switch {
				case pt.FunctionCall != nil:
					// Gemini sends each call whole: emit it as one delta plus its completion.
					args := pt.FunctionCall.Args
					if len(bytes.TrimSpace(args)) == 0 {
						args = json.RawMessage(`{}`)
					}
					id := pt.FunctionCall.ID
					if id == "" {
						id = "call_" + strconv.Itoa(index)
					}
					emit(gw.Event{Type: gw.EvToolCallDelta, Index: index, ToolCallID: id, ToolName: pt.FunctionCall.Name, ArgsDelta: string(args)})
					emit(gw.Event{Type: gw.EvToolCallComplete, Index: index, ToolCallID: id, ToolName: pt.FunctionCall.Name, Args: args})
					index++
				case pt.Thought && pt.Text != "":
					emit(gw.Event{Type: gw.EvReasoningSummary, Text: pt.Text})
				case pt.Text != "":
					emit(gw.Event{Type: gw.EvTextDelta, Text: pt.Text})
				}
			}
			if cand.FinishReason != "" && cand.FinishReason != "FINISH_REASON_UNSPECIFIED" {
				finish = cand.FinishReason
			}
		}
		if m := c.UsageMetadata; m != nil {
			// Thinking tokens are billed as output; cached prompt tokens are reported separately.
			u := gw.Usage{InputTokens: m.PromptTokenCount - m.CachedContentTokenCount,
				OutputTokens: m.CandidatesTokenCount + m.ThoughtsTokenCount, CachedInputTokens: m.CachedContentTokenCount}
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
	if finish == "" {
		return &gw.Error{Code: gw.ErrStreamInterrupted, Sent: true, Msg: "stream closed before a finish reason"}
	}
	switch finish {
	case "STOP":
		finish = "end_turn"
		if index > 0 {
			finish = "tool_use"
		}
	case "MAX_TOKENS":
		finish = "max_tokens"
	case "MALFORMED_FUNCTION_CALL", "UNEXPECTED_TOOL_CALL":
		return &gw.Error{Code: gw.ErrInvalidToolCall, Sent: true, Msg: "gemini: " + strings.ToLower(finish)}
	case "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII", "IMAGE_SAFETY", "LANGUAGE", "refusal":
		finish = "refusal"
	default:
		finish = strings.ToLower(finish)
	}
	emit(gw.Event{Type: gw.EvCompleted, FinishReason: finish, Usage: usage})
	return nil
}

// statusError classifies a non-200 response. Nothing was generated, so
// none of these were "sent" in the billing sense.
func statusError(resp *http.Response, msg []byte) error {
	e := &gw.Error{Status: resp.StatusCode, Msg: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, msg)}
	lower := bytes.ToLower(msg)
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		e.Code = gw.ErrRateLimited
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			e.RetryAfter = time.Duration(s) * time.Second
		}
	case resp.StatusCode == http.StatusBadRequest && (bytes.Contains(lower, []byte("exceeds the maximum number of tokens")) ||
		bytes.Contains(lower, []byte("token count"))):
		e.Code = gw.ErrContextWindowExceeded
	case resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 401 && resp.StatusCode != 403:
		e.Code = gw.ErrCapabilityMismatch
	default:
		e.Code = gw.ErrProviderUnavailable
	}
	return e
}
