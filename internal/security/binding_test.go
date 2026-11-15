package security

import (
	"context"
	"sync"
	"testing"
)

func TestDecisionSlotSingleWriter(t *testing.T) {
	ctx := WithDecisionSlot(context.Background())
	request := Request{RequestID: "request-1"}
	decision := PolicyDecision{DecisionID: "decision-1", Mode: string(ModeEnforce), RequestHash: hashRequest(request)}
	if err := BindDecision(ctx, request, decision); err != nil {
		t.Fatal(err)
	}
	binding, ok := DecisionFrom(ctx)
	if !ok || binding.DecisionID != "decision-1" || binding.RequestID != "request-1" {
		t.Fatalf("binding=%+v ok=%v", binding, ok)
	}
	if err := BindDecision(ctx, request, decision); err == nil {
		t.Fatal("slot accepted a second decision")
	}
	foreign := request
	foreign.RequestID = "request-2"
	if err := BindDecision(WithDecisionSlot(context.Background()), foreign, decision); err == nil {
		t.Fatal("slot accepted mismatched request")
	}
	if err := BindDecision(context.Background(), request, decision); err == nil {
		t.Fatal("enforce decision without slot was accepted")
	}
	decision.Mode = string(ModeShadow)
	if err := BindDecision(context.Background(), request, decision); err != nil {
		t.Fatalf("shadow compatibility: %v", err)
	}
}

func TestDecisionSlotConcurrentBinding(t *testing.T) {
	ctx := WithDecisionSlot(context.Background())
	request := Request{RequestID: "request-1"}
	var wg sync.WaitGroup
	errs := make([]error, 16)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = BindDecision(ctx, request, PolicyDecision{DecisionID: "decision-1", Mode: string(ModeEnforce), RequestHash: hashRequest(request)})
		}(i)
	}
	wg.Wait()
	succeeded := 0
	for _, err := range errs {
		if err == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful bindings=%d", succeeded)
	}
}
