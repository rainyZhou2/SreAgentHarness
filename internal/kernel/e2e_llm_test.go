package kernel_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/gateway/anthropic"
)

func llmStrategy(policy gw.RoutePolicy, alias string, providers ...gw.Provider) func(h *harness) cognitive.Strategy {
	return func(h *harness) cognitive.Strategy {
		g := gw.New(policy, h.store, h.arts, gw.Admission{}, quiet, providers...)
		return &cognitive.LLM{Gateway: g, Artifacts: h.arts, Assembler: cognitive.DefaultAssembler(), Alias: alias}
	}
}

// The model-backed strategy runs end to end through the gateway, the fenced
// ledger and the policy gate, with a deterministic scripted model.
func TestLLMStrategyThroughGateway(t *testing.T) {
	h := start(t, opts{strategyFn: llmStrategy(gw.DefaultPolicy(false, false, ""), "sre-reasoning",
		cognitive.ScriptedModel(cognitive.RuleBased{}))})
	run := h.create(t, "diagnose high latency service=checkout")
	h.waitStatus(t, run.ID, domain.RunCompleted)

	reqs, err := h.store.ListModelRequests(context.Background(), tenant, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(reqs) != 2 {
		t.Fatalf("model requests = %d, want 2 (metrics call + conclusion)", len(reqs))
	}
	for _, r := range reqs {
		if r.Status != "SUCCEEDED" || len(r.Attempts) != 1 || r.Attempts[0].UsageState == nil ||
			*r.Attempts[0].UsageState != "REPORTED" || r.ManifestRef == "" {
			t.Fatalf("request = %+v", r)
		}
	}
	// The decision trace links back to the ledger and manifest.
	steps, _ := h.store.ListSteps(context.Background(), tenant, run.ID)
	var d domain.Decision
	data, _ := h.arts.Get(context.Background(), tenant, *steps[0].OutputRef)
	_ = json.Unmarshal(data, &d)
	if d.ModelRequestID != reqs[0].ID || d.ManifestRef != reqs[0].ManifestRef || d.Tool == nil || d.Tool.Name != "demo.metrics" {
		t.Fatalf("decision = %+v", d)
	}
	var man cognitive.PromptManifest
	mdata, _ := h.arts.Get(context.Background(), tenant, d.ManifestRef)
	_ = json.Unmarshal(mdata, &man)
	if man.ToolSchemaSetHash == "" || man.RenderedRequestHash != d.RenderedRequestHash || man.ContextSnapshotID != *steps[0].ContextSnapshotID {
		t.Fatalf("manifest = %+v", man)
	}
	events, _ := h.store.ListEvents(context.Background(), tenant, run.ID, 0, 500)
	seen := map[string]int{}
	for _, e := range events {
		seen[e.Type]++
	}
	if seen["model.requested"] != 2 || seen["model.completed"] != 2 || seen["model.attempt_started"] != 2 {
		t.Fatalf("model events = %v", seen)
	}
	cost, err := h.store.RunCost(context.Background(), tenant, run.ID)
	if err != nil || cost.Requests != 2 || cost.InputTokens == 0 {
		t.Fatalf("cost = %+v err=%v", cost, err)
	}
}

// A fake Claude endpoint drives the run: first a tool_use, then the
// submit_conclusion pseudo-tool. The adapter, aggregation, validation and
// ledger all run for real; only the HTTP server is fake.
func TestClaudeAdapterEndToEnd(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		n := calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		send := func(e string) {
			var m map[string]any
			_ = json.Unmarshal([]byte(e), &m)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", m["type"], e)
		}
		send(`{"type":"message_start","message":{"id":"msg_` + fmt.Sprint(n) + `","type":"message","role":"assistant","model":"claude-opus-5-5","content":[],"usage":{"input_tokens":800,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}`)
		var tool, args string
		if n == 1 {
			tool, args = "demo__metrics", `{\"service\":\"checkout\"}`
		} else {
			if !strings.Contains(string(body), "db_pool_wait_ms") {
				t.Errorf("second request does not contain the observation")
			}
			tool, args = "submit_conclusion", `{\"answer\":\"DB pool exhausted on checkout\",\"reason_summary\":\"pool at max\"}`
		}
		send(`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_` + fmt.Sprint(n) + `","name":"` + tool + `","input":{}}}`)
		send(`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"` + args + `"}}`)
		send(`{"type":"content_block_stop","index":0}`)
		send(`{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":60}}`)
		send(`{"type":"message_stop"}`)
	}))
	defer srv.Close()

	policy := gw.DefaultPolicy(true, false, "")
	h := start(t, opts{strategyFn: llmStrategy(policy, "sre-reasoning",
		anthropic.New(anthropic.Config{APIKey: "test", BaseURL: srv.URL}), cognitive.ScriptedModel(cognitive.RuleBased{}))})
	run := h.create(t, "diagnose high latency service=checkout")
	done := h.waitStatus(t, run.ID, domain.RunCompleted)
	data, _ := h.arts.Get(context.Background(), tenant, *done.ResultRef)
	if !strings.Contains(string(data), "DB pool exhausted") {
		t.Fatalf("result = %s", data)
	}
	cost, _ := h.store.RunCost(context.Background(), tenant, run.ID)
	// 2 × (800 in × $4 + 60 out × $20) / MTok = 2 × 4400 µUSD
	if cost.CostMicros != 8800 || cost.ByModel["claude-opus-5-5"] != 8800 {
		t.Fatalf("cost = %+v", cost)
	}
}
