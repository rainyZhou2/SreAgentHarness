package openai_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/gateway/openai"
)

func TestStreamToolCalls(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		w.Header().Set("Content-Type", "text/event-stream")
		for _, c := range []string{
			`{"id":"cmpl-1","model":"qwen3","choices":[{"delta":{"role":"assistant","content":"Look"}}]}`,
			`{"id":"cmpl-1","model":"qwen3","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_a","function":{"name":"demo__metrics","arguments":"{\"serv"}}]}}]}`,
			`{"id":"cmpl-1","model":"qwen3","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ice\":\"checkout\"}"}}]}}]}`,
			`{"id":"cmpl-1","model":"qwen3","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"id":"cmpl-1","model":"qwen3","choices":[],"usage":{"prompt_tokens":100,"completion_tokens":20,"prompt_tokens_details":{"cached_tokens":64}}}`,
		} {
			fmt.Fprintf(w, "data: %s\n\n", c)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	p := openai.New(openai.Config{BaseURL: srv.URL + "/v1"})
	req := gw.Request{MaxOutputTokens: 256,
		System: []gw.SystemBlock{{Text: "sys"}},
		Tools:  []gw.ToolSchema{{Name: "demo.metrics", InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"}},"required":["service"]}`)}},
		Messages: []gw.Message{
			{Role: gw.RoleUser, Parts: []gw.Part{{Type: gw.PartText, Text: "goal"}}},
			{Role: gw.RoleAssistant, Parts: []gw.Part{{Type: gw.PartToolUse, ToolUseID: "c0", ToolName: "demo.metrics", Input: json.RawMessage(`{"service":"a"}`)}}},
			{Role: gw.RoleUser, Parts: []gw.Part{{Type: gw.PartToolResult, ToolUseID: "c0", Text: "{}"}}},
		}}
	agg := gw.NewAggregator(req.Tools)
	names := gw.ToolNameMap(req.Tools)
	if err := p.Stream(context.Background(), gw.Endpoint{ModelID: "qwen3", MaxOutput: 1000}, req, func(ev gw.Event) {
		if n, ok := names[ev.ToolName]; ok {
			ev.ToolName = n
		}
		agg.Add(ev)
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := agg.Result()
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "Look" || len(resp.ToolCalls) != 1 || string(resp.ToolCalls[0].Args) != `{"service":"checkout"}` ||
		resp.ToolCalls[0].Name != "demo.metrics" || resp.FinishReason != "tool_use" {
		t.Fatalf("resp = %+v", resp)
	}
	if resp.Usage != (gw.Usage{InputTokens: 36, OutputTokens: 20, CachedInputTokens: 64}) {
		t.Fatalf("usage = %+v", resp.Usage)
	}
	msgs := body["messages"].([]any)
	if len(msgs) != 4 || msgs[0].(map[string]any)["role"] != "system" || msgs[3].(map[string]any)["role"] != "tool" {
		t.Fatalf("messages = %v", msgs)
	}
}

func TestRateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(429)
	}))
	defer srv.Close()
	err := openai.New(openai.Config{BaseURL: srv.URL}).Stream(context.Background(), gw.Endpoint{ModelID: "m"},
		gw.Request{Messages: []gw.Message{{Role: gw.RoleUser, Parts: []gw.Part{{Type: gw.PartText, Text: "x"}}}}}, func(gw.Event) {})
	ge, ok := gw.AsError(err)
	if !ok || ge.Code != gw.ErrRateLimited || ge.RetryAfter.Seconds() != 3 || !ge.Fallbackable() {
		t.Fatalf("err = %v", err)
	}
}
