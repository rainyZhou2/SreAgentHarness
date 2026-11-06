package gateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/gateway/scripted"
)

func policy() gw.RoutePolicy {
	return gw.RoutePolicy{ID: "t", Version: 3, MaxAttempts: 2, Aliases: map[string][]string{"m": {"a", "b"}},
		Endpoints: []gw.Endpoint{
			{ID: "a", ProviderID: "pa", ModelID: "model-a", Capabilities: gw.Capabilities{Tools: true, Streaming: true},
				ContextWindow: 100_000, MaxOutput: 1000, Pricing: gw.Pricing{Version: "p1", InputPerMTok: 1, OutputPerMTok: 2}},
			{ID: "b", ProviderID: "pb", ModelID: "model-b", Capabilities: gw.Capabilities{Tools: true, Streaming: true},
				ContextWindow: 100_000, MaxOutput: 1000, Priority: 1, Pricing: gw.Pricing{Version: "p1", InputPerMTok: 1, OutputPerMTok: 2}},
		}}
}

func req() gw.Request {
	return gw.Request{ModelAlias: "m", MaxOutputTokens: 500, NeedTools: true,
		Messages: []gw.Message{{Role: gw.RoleUser, Parts: []gw.Part{{Type: gw.PartText, Text: "hi"}}}},
		Tools: []gw.ToolSchema{{Name: "demo.metrics", InputSchema: json.RawMessage(
			`{"type":"object","properties":{"service":{"type":"string"}},"required":["service"]}`)}}}
}

func sc() gw.Scope { return gw.Scope{TenantID: "t1", RunID: "r", StepID: "s", FenceToken: 1} }

func prov(id string, s scripted.Script) *scripted.Provider {
	p := scripted.New(func(gw.Request) (scripted.Script, error) { return s, nil })
	p.Name = id
	return p
}

func TestGenerateAggregatesToolCall(t *testing.T) {
	led := gw.NewMemLedger()
	g := gw.New(policy(), led, gw.NewMemArtifacts(), gw.Admission{}, nil,
		prov("pa", scripted.Script{Text: "checking", ToolCalls: []gw.ToolCall{{Name: "demo.metrics", Args: json.RawMessage(`{"service":"checkout"}`)}}}))
	var deltas int
	resp, err := g.Generate(context.Background(), sc(), req(), func(ev gw.Event) {
		if ev.Type == gw.EvToolCallDelta || ev.Type == gw.EvTextDelta {
			deltas++
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "demo.metrics" || string(resp.ToolCalls[0].Args) != `{"service":"checkout"}` {
		t.Fatalf("tool calls = %+v", resp.ToolCalls)
	}
	if deltas < 2 || resp.EndpointID != "a" || resp.FinishReason != "tool_use" {
		t.Fatalf("deltas=%d endpoint=%s finish=%s", deltas, resp.EndpointID, resp.FinishReason)
	}
	r := led.Requests[resp.ModelRequestID]
	if r == nil || r.Result == nil || r.Result.Status != "SUCCEEDED" || r.Record.RoutePolicyVersion != 3 {
		t.Fatalf("ledger request = %+v", r)
	}
	// ESTIMATED revision 1 then REPORTED revision 2 superseding it.
	if len(led.Usage) != 2 || led.Usage[0].State != gw.UsageEstimated || led.Usage[1].State != gw.UsageReported ||
		led.Usage[1].SupersedesID != led.Usage[0].ID {
		t.Fatalf("usage = %+v", led.Usage)
	}
}

func TestPreSendFailureFallsBack(t *testing.T) {
	led := gw.NewMemLedger()
	g := gw.New(policy(), led, gw.NewMemArtifacts(), gw.Admission{}, nil,
		prov("pa", scripted.Script{Err: &gw.Error{Code: gw.ErrRateLimited, Sent: false}}),
		prov("pb", scripted.Script{Text: "done"}))
	resp, err := g.Generate(context.Background(), sc(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.EndpointID != "b" || len(led.Attempts) != 2 {
		t.Fatalf("endpoint=%s attempts=%d", resp.EndpointID, len(led.Attempts))
	}
}

func TestPostSendFailureDoesNotFallBack(t *testing.T) {
	led := gw.NewMemLedger()
	g := gw.New(policy(), led, gw.NewMemArtifacts(), gw.Admission{}, nil,
		prov("pa", scripted.Script{Err: &gw.Error{Code: gw.ErrStreamInterrupted, Sent: true}}),
		prov("pb", scripted.Script{Text: "done"}))
	_, err := g.Generate(context.Background(), sc(), req(), nil)
	ge, ok := gw.AsError(err)
	if !ok || ge.Code != gw.ErrStreamInterrupted || !ge.Retryable() {
		t.Fatalf("err = %v", err)
	}
	if len(led.Attempts) != 1 {
		t.Fatalf("attempts = %d, want 1 (no silent fallback after send)", len(led.Attempts))
	}
	if u := led.Usage[len(led.Usage)-1]; u.State != gw.UsageUnknown {
		t.Fatalf("usage after sent failure = %s, want UNKNOWN", u.State)
	}
}

func TestInvalidToolArgumentsRejected(t *testing.T) {
	g := gw.New(policy(), gw.NewMemLedger(), gw.NewMemArtifacts(), gw.Admission{}, nil,
		prov("pa", scripted.Script{ToolCalls: []gw.ToolCall{{Name: "demo.metrics", Args: json.RawMessage(`{"svc":1}`)}}}))
	_, err := g.Generate(context.Background(), sc(), req(), nil)
	if ge, ok := gw.AsError(err); !ok || ge.Code != gw.ErrInvalidToolCall {
		t.Fatalf("err = %v", err)
	}
	g = gw.New(policy(), gw.NewMemLedger(), gw.NewMemArtifacts(), gw.Admission{}, nil,
		prov("pa", scripted.Script{ToolCalls: []gw.ToolCall{{Name: "demo.metrics", Args: json.RawMessage(`{"service":`)}}}))
	_, err = g.Generate(context.Background(), sc(), req(), nil)
	if ge, ok := gw.AsError(err); !ok || ge.Code != gw.ErrInvalidToolCall {
		t.Fatalf("truncated JSON err = %v", err)
	}
}

func TestHardFiltersAndFence(t *testing.T) {
	p := policy()
	p.Endpoints[0].Tenants = []string{"other"}
	p.Endpoints[1].Capabilities.Tools = false
	g := gw.New(p, gw.NewMemLedger(), gw.NewMemArtifacts(), gw.Admission{}, nil, prov("pa", scripted.Script{}), prov("pb", scripted.Script{}))
	_, err := g.Generate(context.Background(), sc(), req(), nil)
	if ge, ok := gw.AsError(err); !ok || (ge.Code != gw.ErrRejectedByPolicy && ge.Code != gw.ErrCapabilityMismatch) {
		t.Fatalf("err = %v", err)
	}
	led := gw.NewMemLedger()
	led.FenceOK = func(gw.Scope) bool { return false }
	g = gw.New(policy(), led, gw.NewMemArtifacts(), gw.Admission{}, nil, prov("pa", scripted.Script{Text: "x"}))
	_, err = g.Generate(context.Background(), sc(), req(), nil)
	if ge, ok := gw.AsError(err); !ok || ge.Code != gw.ErrFenceLost {
		t.Fatalf("fence err = %v", err)
	}
}

func TestBudgetAndBreaker(t *testing.T) {
	led := gw.NewMemLedger()
	g := gw.New(policy(), led, gw.NewMemArtifacts(), gw.Admission{DailyBudgetMicros: 100}, nil, prov("pa", scripted.Script{Text: "x"}))
	_, err := g.Generate(context.Background(), sc(), req(), nil)
	if ge, ok := gw.AsError(err); !ok || ge.Code != gw.ErrBudgetExceeded {
		t.Fatalf("err = %v", err)
	}
	br := gw.NewBreakers(2, time.Minute)
	now := time.Now()
	br.Record("a", false, now)
	br.Record("a", false, now)
	if br.Allow("a", now) || !br.Allow("a", now.Add(2*time.Minute)) {
		t.Fatal("breaker should open then half-open after cooldown")
	}
	if !errors.Is(context.Canceled, context.Canceled) {
		t.Fatal()
	}
}

func TestCostMicros(t *testing.T) {
	c := gw.PriceOpus55.CostMicros(gw.Usage{InputTokens: 1_000_000, OutputTokens: 100_000, CachedInputTokens: 1_000_000})
	// $4 + $2 + $0.20
	if c != 6_200_000 {
		t.Fatalf("cost = %d", c)
	}
}

func TestOverlayRoutesEntitledTenantOnly(t *testing.T) {
	g := gw.New(policy(), gw.NewMemLedger(), gw.NewMemArtifacts(), gw.Admission{}, nil,
		prov("pa", scripted.Script{Text: "base"}), prov("pb", scripted.Script{Text: "base-b"}))
	tenantEP := gw.Endpoint{ID: "tenant:t1:claude", ProviderID: "tenant:t1:claude", ModelID: "own-model", Tenants: []string{"t1"},
		Priority: -10, Capabilities: gw.Capabilities{Tools: true, Streaming: true}, ContextWindow: 100_000, MaxOutput: 1000,
		Pricing: gw.Pricing{Version: "p1"}}
	g.SetOverlay(gw.Overlay{Endpoints: []gw.Endpoint{tenantEP}, Aliases: map[string][]string{"m": {tenantEP.ID}},
		Providers: []gw.Provider{prov("tenant:t1:claude", scripted.Script{Text: "own"}), prov("pa", scripted.Script{Text: "hijack"})}})

	resp, err := g.Generate(context.Background(), sc(), req(), func(gw.Event) {})
	if err != nil || resp.EndpointID != tenantEP.ID || resp.Text != "own" {
		t.Fatalf("t1: %+v %v", resp, err)
	}
	other := sc()
	other.TenantID, other.RunID = "t2", "r2"
	resp, err = g.Generate(context.Background(), other, req(), func(gw.Event) {})
	if err != nil || resp.EndpointID != "a" || resp.Text != "base" {
		t.Fatalf("t2 must not use t1's model, and an overlay provider must not replace a base one: %+v %v", resp, err)
	}
	// Swapping the base policy keeps the overlay; clearing the overlay restores the base.
	g.SetPolicy(policy())
	if ep, _ := g.Resolve(req(), sc()); ep.ID != tenantEP.ID {
		t.Fatalf("after SetPolicy: %s", ep.ID)
	}
	if g.Policy().Version == policy().Version {
		t.Fatal("overlay policy must carry a distinct version")
	}
	g.SetOverlay(gw.Overlay{})
	if ep, _ := g.Resolve(req(), sc()); ep.ID != "a" {
		t.Fatalf("after clearing: %s", ep.ID)
	}
}
