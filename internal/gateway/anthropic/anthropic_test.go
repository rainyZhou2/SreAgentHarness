package anthropic_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/gateway/anthropic"
)

func sse(w http.ResponseWriter, events ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	for _, e := range events {
		var m map[string]any
		_ = json.Unmarshal([]byte(e), &m)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", m["type"], e)
	}
}

func TestStreamToolUse(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		if r.Header.Get("X-Api-Key") != "test-key" && r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("api key header = %q", r.Header.Get("X-Api-Key"))
		}
		sse(w,
			`{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5","content":[],"stop_reason":null,"usage":{"input_tokens":120,"output_tokens":1,"cache_read_input_tokens":900,"cache_creation_input_tokens":30}}}`,
			`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Checking metrics."}}`,
			`{"type":"content_block_stop","index":0}`,
			`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"demo__metrics","input":{}}}`,
			`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"service\":"}}`,
			`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"checkout\"}"}}`,
			`{"type":"content_block_stop","index":1}`,
			`{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":42}}`,
			`{"type":"message_stop"}`)
	}))
	defer srv.Close()

	p := anthropic.New(anthropic.Config{APIKey: "test-key", BaseURL: srv.URL, Effort: "high"})
	ep := gw.Endpoint{ID: "e", ProviderID: "anthropic", ModelID: "claude-opus-5-5", MaxOutput: 128000}
	req := gw.Request{ModelAlias: "x", MaxOutputTokens: 4096,
		System: []gw.SystemBlock{{Text: "stable", CacheBreakpoint: true}, {Text: "dynamic"}},
		Tools:  []gw.ToolSchema{{Name: "demo.metrics", Description: "m", InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"}},"required":["service"]}`)}},
		Messages: []gw.Message{
			{Role: gw.RoleUser, Parts: []gw.Part{{Type: gw.PartText, Text: "goal"}}},
			{Role: gw.RoleAssistant, Parts: []gw.Part{{Type: gw.PartToolUse, ToolUseID: "toolu_0", ToolName: "demo.metrics", Input: json.RawMessage(`{"service":"a"}`)}}},
			{Role: gw.RoleUser, Parts: []gw.Part{{Type: gw.PartToolResult, ToolUseID: "toolu_0", Text: "{}"}}},
		}}
	agg := gw.NewAggregator(req.Tools)
	names := gw.ToolNameMap(req.Tools)
	err := p.Stream(context.Background(), ep, req, func(ev gw.Event) {
		if n, ok := names[ev.ToolName]; ok {
			ev.ToolName = n
		}
		agg.Add(ev)
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := agg.Result()
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "Checking metrics." || len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "demo.metrics" ||
		string(resp.ToolCalls[0].Args) != `{"service":"checkout"}` || resp.FinishReason != "tool_use" {
		t.Fatalf("resp = %+v", resp)
	}
	if resp.Usage != (gw.Usage{InputTokens: 120, OutputTokens: 42, CachedInputTokens: 900, CacheWriteTokens: 30}) {
		t.Fatalf("usage = %+v", resp.Usage)
	}
	if resp.ProviderRequestID != "msg_1" || resp.ResolvedModel != "claude-opus-5-5" {
		t.Fatalf("ids = %s %s", resp.ProviderRequestID, resp.ResolvedModel)
	}
	// Request shape: model, adaptive thinking, effort, cache breakpoint, eager tool streaming.
	if body["model"] != "claude-opus-5-5" || body["stream"] != true {
		t.Fatalf("body = %v", body)
	}
	if th, _ := body["thinking"].(map[string]any); th["type"] != "adaptive" {
		t.Fatalf("thinking = %v", body["thinking"])
	}
	if oc, _ := body["output_config"].(map[string]any); oc["effort"] != "high" {
		t.Fatalf("output_config = %v", body["output_config"])
	}
	sys := body["system"].([]any)
	if sys[0].(map[string]any)["cache_control"] == nil || sys[1].(map[string]any)["cache_control"] != nil {
		t.Fatalf("system = %v", sys)
	}
	tool := body["tools"].([]any)[0].(map[string]any)
	if tool["name"] != "demo__metrics" || tool["eager_input_streaming"] != true {
		t.Fatalf("tool = %v", tool)
	}
	if _, ok := body["tool_choice"]; ok {
		t.Fatalf("tool_choice must be left at the default (auto)")
	}
}

func TestErrorClassification(t *testing.T) {
	cases := []struct {
		status int
		body   string
		code   gw.ErrorCode
	}{
		{429, `{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`, gw.ErrRateLimited},
		{529, `{"type":"error","error":{"type":"overloaded_error","message":"overloaded"}}`, gw.ErrProviderUnavailable},
		{400, `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 2000000 tokens"}}`, gw.ErrContextWindowExceeded},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(c.status)
			_, _ = io.WriteString(w, c.body)
		}))
		p := anthropic.New(anthropic.Config{APIKey: "k", BaseURL: srv.URL})
		err := p.Stream(context.Background(), gw.Endpoint{ModelID: "claude-opus-5-5", MaxOutput: 1000},
			gw.Request{Messages: []gw.Message{{Role: gw.RoleUser, Parts: []gw.Part{{Type: gw.PartText, Text: "x"}}}}}, func(gw.Event) {})
		srv.Close()
		ge, ok := gw.AsError(err)
		if !ok || ge.Code != c.code || ge.Sent {
			t.Fatalf("status %d: err = %v (%+v)", c.status, err, ge)
		}
	}
}

func TestInterruptedStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sse(w, `{"type":"message_start","message":{"id":"msg_2","type":"message","role":"assistant","model":"claude-opus-5-5","content":[],"usage":{"input_tokens":5,"output_tokens":1}}}`,
			`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"demo__metrics","input":{}}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"serv"}}`)
	}))
	defer srv.Close()
	p := anthropic.New(anthropic.Config{APIKey: "k", BaseURL: srv.URL})
	agg := gw.NewAggregator(nil)
	err := p.Stream(context.Background(), gw.Endpoint{ModelID: "claude-opus-5-5", MaxOutput: 100},
		gw.Request{Messages: []gw.Message{{Role: gw.RoleUser, Parts: []gw.Part{{Type: gw.PartText, Text: "x"}}}}}, agg.Add)
	if err == nil {
		_, err = agg.Result()
	}
	ge, ok := gw.AsError(err)
	if !ok || (ge.Code != gw.ErrStreamInterrupted && ge.Code != gw.ErrInvalidToolCall) || !strings.Contains(err.Error(), "") {
		t.Fatalf("err = %v", err)
	}
}
