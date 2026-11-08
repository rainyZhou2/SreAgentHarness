// Package gateway is the Model Gateway (RFC §30–§36). It resolves a model
// alias to a concrete endpoint through hard filters and a soft score, applies
// admission (tenant budget, concurrency, circuit breaker), streams the provider
// response through a normalized event model, and records every request,
// attempt and usage revision in an append-only ledger.
//
// Provider SDK types never leave the provider adapters (§20.1): everything in
// this file is provider-neutral.
package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ---------- request ----------

type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

type PartType string

const (
	PartText       PartType = "text"
	PartToolUse    PartType = "tool_use"
	PartToolResult PartType = "tool_result"
)

type Part struct {
	Type      PartType        `json:"type"`
	Text      string          `json:"text,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	ToolName  string          `json:"tool_name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
	// CacheBreakpoint marks the end of a cacheable message prefix (the
	// append-only history); providers without explicit caching ignore it.
	CacheBreakpoint bool `json:"cache_breakpoint,omitempty"`
}

type Message struct {
	Role  Role   `json:"role"`
	Parts []Part `json:"parts"`
}

type SystemBlock struct {
	Text string `json:"text"`
	// CacheBreakpoint marks the end of the stable prefix (§33.1 sections A+B).
	CacheBreakpoint bool `json:"cache_breakpoint,omitempty"`
}

type ToolSchema struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
	// Source is routing metadata, not part of the provider request. Remote
	// MCP servers validate their own arguments, so the client skips the
	// local subset check for them.
	Source string `json:"-"`
}

// Request is the provider-neutral rendered request. Its canonical JSON is the
// request artifact whose hash is bound into the model_requests row.
type Request struct {
	ModelAlias      string        `json:"model_alias"`
	System          []SystemBlock `json:"system"`
	Messages        []Message     `json:"messages"`
	Tools           []ToolSchema  `json:"tools,omitempty"`
	MaxOutputTokens int64         `json:"max_output_tokens"`
	Effort          string        `json:"effort,omitempty"` // low|medium|high (provider-mapped)
	// Capabilities the request needs; used as hard filters.
	NeedTools     bool `json:"need_tools"`
	NeedStreaming bool `json:"need_streaming"`
}

// Scope identifies who is calling. The fence fields make ledger creation
// conditional on still owning the step lease (§30.4).
type Scope struct {
	TenantID          string
	RunID             string
	StepID            string
	StepAttemptID     string
	WorkerID          string
	FenceToken        int64
	ContextSnapshotID string
	Region            string
	ManifestRef       string
	Priority          int
}

// ---------- normalized stream events (§35.2) ----------

type EventType string

const (
	EvStarted          EventType = "started"
	EvTextDelta        EventType = "text_delta"
	EvReasoningSummary EventType = "reasoning_summary_delta"
	EvToolCallDelta    EventType = "tool_call_delta"
	EvToolCallComplete EventType = "tool_call_complete"
	EvUsage            EventType = "usage"
	EvCompleted        EventType = "completed"
	EvFailed           EventType = "failed"
	EvCancelled        EventType = "cancelled"
)

type Event struct {
	Type              EventType       `json:"type"`
	ModelRequestID    string          `json:"model_request_id,omitempty"`
	AttemptNo         int             `json:"attempt_no,omitempty"`
	Index             int             `json:"index,omitempty"`
	Text              string          `json:"text,omitempty"`
	ToolCallID        string          `json:"tool_call_id,omitempty"`
	ToolName          string          `json:"tool_name,omitempty"`
	ArgsDelta         string          `json:"args_delta,omitempty"`
	Args              json.RawMessage `json:"args,omitempty"`
	Usage             *Usage          `json:"usage,omitempty"`
	FinishReason      string          `json:"finish_reason,omitempty"`
	ProviderRequestID string          `json:"provider_request_id,omitempty"`
	ResolvedModel     string          `json:"resolved_model,omitempty"`
	ErrorCode         ErrorCode       `json:"error_code,omitempty"`
}

// Sink receives normalized events. Implementations must not block for long:
// the delta bus is best-effort and never a source of truth (§35.4).
type Sink func(Event)

type Usage struct {
	InputTokens       int64 `json:"input_tokens"`
	OutputTokens      int64 `json:"output_tokens"`
	CachedInputTokens int64 `json:"cached_input_tokens"`
	CacheWriteTokens  int64 `json:"cache_write_tokens"`
}

type ToolCall struct {
	ID   string          `json:"id"`
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

// Response is the aggregated, validated result of one successful attempt.
type Response struct {
	Text              string        `json:"text"`
	ToolCalls         []ToolCall    `json:"tool_calls,omitempty"`
	FinishReason      string        `json:"finish_reason"`
	Usage             Usage         `json:"usage"`
	ProviderID        string        `json:"provider_id"`
	EndpointID        string        `json:"endpoint_id"`
	ResolvedModel     string        `json:"resolved_model"`
	ProviderRequestID string        `json:"provider_request_id,omitempty"`
	ModelRequestID    string        `json:"model_request_id"`
	AttemptID         string        `json:"attempt_id"`
	TTFT              time.Duration `json:"ttft_ns"`
	CostMicros        int64         `json:"cost_micros"`

	respRef string
}

// ResponseRef is the artifact holding the aggregated response.
func (r Response) ResponseRef() string { return r.respRef }

// ---------- errors (§31.3) ----------

type ErrorCode string

const (
	ErrRejectedByPolicy      ErrorCode = "REJECTED_BY_POLICY"
	ErrCapabilityMismatch    ErrorCode = "CAPABILITY_MISMATCH"
	ErrRateLimited           ErrorCode = "RATE_LIMITED"
	ErrBudgetExceeded        ErrorCode = "BUDGET_EXCEEDED"
	ErrProviderUnavailable   ErrorCode = "PROVIDER_UNAVAILABLE"
	ErrTimeoutBeforeSend     ErrorCode = "TIMEOUT_BEFORE_SEND"
	ErrTimeoutOutcomeUnknown ErrorCode = "TIMEOUT_OUTCOME_UNKNOWN"
	ErrStreamInterrupted     ErrorCode = "STREAM_INTERRUPTED"
	ErrInvalidToolCall       ErrorCode = "INVALID_TOOL_CALL"
	ErrContextWindowExceeded ErrorCode = "CONTEXT_WINDOW_EXCEEDED"
	ErrModelChanged          ErrorCode = "MODEL_CHANGED_INCOMPATIBLY"
	ErrRefused               ErrorCode = "MODEL_REFUSED"
	ErrOutputTruncated       ErrorCode = "OUTPUT_TRUNCATED"
	ErrCancelled             ErrorCode = "CANCELLED"
	ErrFenceLost             ErrorCode = "FENCE_LOST"
)

// Error is a classified gateway error. Sent reports whether the request may
// have reached the provider (it then has a cost even if it failed).
type Error struct {
	Code       ErrorCode
	Msg        string
	Sent       bool
	RetryAfter time.Duration
	Status     int
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Msg) }

// Retryable reports whether the same request may be sent again (possibly to
// another endpoint). Model calls have no external side effects, so a failure
// after send is retryable too — it just costs tokens.
func (e *Error) Retryable() bool {
	switch e.Code {
	case ErrRateLimited, ErrProviderUnavailable, ErrTimeoutBeforeSend, ErrTimeoutOutcomeUnknown,
		ErrStreamInterrupted, ErrInvalidToolCall, ErrOutputTruncated:
		return true
	}
	return false
}

// Fallbackable reports whether the router may try another endpoint within the
// same model request. Only pre-send failures fall back silently (§31.4).
func (e *Error) Fallbackable() bool {
	return !e.Sent && (e.Code == ErrRateLimited || e.Code == ErrProviderUnavailable || e.Code == ErrTimeoutBeforeSend)
}

func errf(code ErrorCode, sent bool, format string, a ...any) *Error {
	return &Error{Code: code, Sent: sent, Msg: fmt.Sprintf(format, a...)}
}

// AsError extracts a gateway error.
func AsError(err error) (*Error, bool) {
	var ge *Error
	ok := errors.As(err, &ge)
	return ge, ok
}

// ---------- tool name mapping ----------

// WireToolName maps registry names (e.g. "mcp.prom.query") onto the
// [a-zA-Z0-9_-]{1,64} alphabet providers accept.
func WireToolName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		case r == '.':
			b.WriteString("__")
		default:
			b.WriteByte('_')
		}
	}
	s := b.String()
	if len(s) > 64 {
		s = s[:64]
	}
	return s
}

// ToolNameMap returns wire name → registry name for the request's tools.
func ToolNameMap(tools []ToolSchema) map[string]string {
	m := make(map[string]string, len(tools))
	for _, t := range tools {
		m[WireToolName(t.Name)] = t.Name
	}
	return m
}
