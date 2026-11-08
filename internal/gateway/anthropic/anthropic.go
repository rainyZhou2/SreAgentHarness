// Package anthropic is the Claude Messages API adapter for the Model Gateway.
// It uses the official Go SDK; no SDK type escapes this package (§20.1).
package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	sdk "github.com/anthropics/anthropic-sdk-go/v2"
	"github.com/anthropics/anthropic-sdk-go/v2/option"

	gw "go-sre-harness/internal/gateway"
)

type Config struct {
	APIKey  string
	BaseURL string // optional; tests point it at an httptest server
	// Effort maps to output_config.effort (low|medium|high|xhigh|max). Empty = API default.
	Effort string
	// Timeout bounds one HTTP request (the step deadline still applies).
	Timeout time.Duration
	// HTTPClient overrides the SDK's client (tenant models pass one that
	// enforces their network policy).
	HTTPClient *http.Client
}

type Provider struct {
	client sdk.Client
	cfg    Config
}

func New(cfg Config) *Provider {
	opts := []option.RequestOption{
		option.WithAPIKey(cfg.APIKey),
		// Retries are attempts: the gateway records and routes them itself.
		option.WithMaxRetries(0),
	}
	if cfg.BaseURL != "" {
		opts = append(opts, option.WithBaseURL(cfg.BaseURL))
	}
	if cfg.Timeout > 0 {
		opts = append(opts, option.WithRequestTimeout(cfg.Timeout))
	}
	if cfg.HTTPClient != nil {
		opts = append(opts, option.WithHTTPClient(cfg.HTTPClient))
	}
	return &Provider{client: sdk.NewClient(opts...), cfg: cfg}
}

func (p *Provider) ID() string { return "anthropic" }

func (p *Provider) params(ep gw.Endpoint, req gw.Request) (sdk.MessageNewParams, error) {
	maxTok := req.MaxOutputTokens
	if maxTok <= 0 || (ep.MaxOutput > 0 && maxTok > ep.MaxOutput) {
		maxTok = ep.MaxOutput
	}
	params := sdk.MessageNewParams{
		Model:     sdk.Model(ep.ModelID),
		MaxTokens: maxTok,
		// Adaptive thinking: the model decides how much to think. The raw
		// thinking text is never persisted; only the decision trace is (§7.3).
		Thinking: sdk.ThinkingConfigParamUnion{OfAdaptive: &sdk.ThinkingConfigAdaptiveParam{}},
	}
	effort := req.Effort
	if effort == "" {
		effort = p.cfg.Effort
	}
	if effort != "" {
		params.OutputConfig = sdk.OutputConfigParam{Effort: sdk.OutputConfigEffort(effort)}
	}
	for _, s := range req.System {
		b := sdk.TextBlockParam{Text: s.Text}
		if s.CacheBreakpoint {
			b.CacheControl = sdk.NewCacheControlEphemeralParam()
		}
		params.System = append(params.System, b)
	}
	for _, t := range req.Tools {
		var schema struct {
			Properties any      `json:"properties"`
			Required   []string `json:"required"`
		}
		_ = json.Unmarshal(t.InputSchema, &schema)
		if schema.Properties == nil {
			schema.Properties = map[string]any{}
		}
		tp := sdk.ToolParam{
			Name:        gw.WireToolName(t.Name),
			Description: sdk.String(t.Description),
			InputSchema: sdk.ToolInputSchemaParam{Properties: schema.Properties, Required: schema.Required},
			// Stream tool input as generated; the gateway validates the
			// complete arguments against the schema before the policy gate.
			EagerInputStreaming: sdk.Bool(true),
		}
		params.Tools = append(params.Tools, sdk.ToolUnionParam{OfTool: &tp})
	}
	for _, m := range req.Messages {
		var blocks []sdk.ContentBlockParamUnion
		for _, part := range m.Parts {
			switch part.Type {
			case gw.PartText:
				if strings.TrimSpace(part.Text) != "" {
					b := sdk.NewTextBlock(part.Text)
					if part.CacheBreakpoint && b.OfText != nil {
						b.OfText.CacheControl = sdk.NewCacheControlEphemeralParam()
					}
					blocks = append(blocks, b)
				}
			case gw.PartToolUse:
				var input any = map[string]any{}
				if len(part.Input) > 0 {
					_ = json.Unmarshal(part.Input, &input)
				}
				blocks = append(blocks, sdk.NewToolUseBlock(part.ToolUseID, input, gw.WireToolName(part.ToolName)))
			case gw.PartToolResult:
				blocks = append(blocks, sdk.NewToolResultBlock(part.ToolUseID, part.Text, part.IsError))
			}
		}
		if len(blocks) == 0 {
			continue
		}
		if m.Role == gw.RoleAssistant {
			params.Messages = append(params.Messages, sdk.NewAssistantMessage(blocks...))
		} else {
			params.Messages = append(params.Messages, sdk.NewUserMessage(blocks...))
		}
	}
	if len(params.Messages) == 0 {
		return params, &gw.Error{Code: gw.ErrCapabilityMismatch, Msg: "request has no messages"}
	}
	return params, nil
}

type toolBlock struct {
	id, name string
	args     strings.Builder
}

func (p *Provider) Stream(ctx context.Context, ep gw.Endpoint, req gw.Request, emit gw.Sink) error {
	params, err := p.params(ep, req)
	if err != nil {
		return err
	}
	stream := p.client.Messages.NewStreaming(ctx, params)
	defer stream.Close()

	received := false
	toolsByIdx := map[int64]*toolBlock{}
	var usage gw.Usage
	finish := ""
	for stream.Next() {
		received = true
		switch ev := stream.Current().AsAny().(type) {
		case sdk.MessageStartEvent:
			u := ev.Message.Usage
			usage = gw.Usage{InputTokens: u.InputTokens, OutputTokens: u.OutputTokens,
				CachedInputTokens: u.CacheReadInputTokens, CacheWriteTokens: u.CacheCreationInputTokens}
			emit(gw.Event{Type: gw.EvStarted, ProviderRequestID: ev.Message.ID, ResolvedModel: string(ev.Message.Model)})
		case sdk.ContentBlockStartEvent:
			if ev.ContentBlock.Type == "tool_use" {
				tb := &toolBlock{id: ev.ContentBlock.ID, name: ev.ContentBlock.Name}
				toolsByIdx[ev.Index] = tb
				emit(gw.Event{Type: gw.EvToolCallDelta, Index: int(ev.Index), ToolCallID: tb.id, ToolName: tb.name})
			}
		case sdk.ContentBlockDeltaEvent:
			switch d := ev.Delta.AsAny().(type) {
			case sdk.TextDelta:
				emit(gw.Event{Type: gw.EvTextDelta, Index: int(ev.Index), Text: d.Text})
			case sdk.InputJSONDelta:
				if tb := toolsByIdx[ev.Index]; tb != nil {
					tb.args.WriteString(d.PartialJSON)
					emit(gw.Event{Type: gw.EvToolCallDelta, Index: int(ev.Index), ToolCallID: tb.id, ArgsDelta: d.PartialJSON})
				}
			case sdk.ThinkingDelta:
				emit(gw.Event{Type: gw.EvReasoningSummary, Index: int(ev.Index), Text: d.Thinking})
			}
		case sdk.ContentBlockStopEvent:
			// A tool call is complete only at the end of its content block (§35.3).
			if tb := toolsByIdx[ev.Index]; tb != nil {
				args := tb.args.String()
				if strings.TrimSpace(args) == "" {
					args = "{}"
				}
				emit(gw.Event{Type: gw.EvToolCallComplete, Index: int(ev.Index), ToolCallID: tb.id, ToolName: tb.name,
					Args: json.RawMessage(args)})
				delete(toolsByIdx, ev.Index)
			}
		case sdk.MessageDeltaEvent:
			finish = string(ev.Delta.StopReason)
			u := ev.Usage
			usage.OutputTokens = u.OutputTokens
			if u.InputTokens > 0 {
				usage.InputTokens = u.InputTokens
			}
			if u.CacheReadInputTokens > 0 {
				usage.CachedInputTokens = u.CacheReadInputTokens
			}
			if u.CacheCreationInputTokens > 0 {
				usage.CacheWriteTokens = u.CacheCreationInputTokens
			}
			uc := usage
			emit(gw.Event{Type: gw.EvUsage, Usage: &uc})
		case sdk.MessageStopEvent:
			uc := usage
			emit(gw.Event{Type: gw.EvCompleted, FinishReason: finish, Usage: &uc})
		}
	}
	if err := stream.Err(); err != nil {
		return classify(ctx, err, received)
	}
	return nil
}

func classify(ctx context.Context, err error, received bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var apiErr *sdk.Error
	if errors.As(err, &apiErr) {
		e := &gw.Error{Status: apiErr.StatusCode, Msg: apiErr.Error(), Sent: received}
		switch {
		case apiErr.StatusCode == http.StatusTooManyRequests:
			e.Code = gw.ErrRateLimited
			if apiErr.Response != nil {
				if d, perr := time.ParseDuration(apiErr.Response.Header.Get("retry-after") + "s"); perr == nil {
					e.RetryAfter = d
				}
			}
		case apiErr.StatusCode == http.StatusBadRequest && strings.Contains(strings.ToLower(apiErr.Error()), "prompt is too long"):
			e.Code = gw.ErrContextWindowExceeded
		case apiErr.StatusCode == http.StatusBadRequest || apiErr.StatusCode == http.StatusNotFound ||
			apiErr.StatusCode == http.StatusUnprocessableEntity:
			e.Code = gw.ErrCapabilityMismatch
		case apiErr.StatusCode == http.StatusUnauthorized || apiErr.StatusCode == http.StatusForbidden:
			e.Code = gw.ErrProviderUnavailable
		case apiErr.StatusCode >= 500:
			e.Code = gw.ErrProviderUnavailable
			if received {
				e.Code = gw.ErrStreamInterrupted
			}
		default:
			e.Code = gw.ErrProviderUnavailable
		}
		return e
	}
	if received {
		return &gw.Error{Code: gw.ErrStreamInterrupted, Sent: true, Msg: err.Error()}
	}
	return &gw.Error{Code: gw.ErrProviderUnavailable, Sent: false, Msg: err.Error()}
}
