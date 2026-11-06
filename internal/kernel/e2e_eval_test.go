package kernel_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/profile"
)

// wrongAnswer finishes immediately with a wrong diagnosis.
type wrongAnswer struct{}

func (wrongAnswer) Name() string { return "wrong" }
func (wrongAnswer) Decide(context.Context, cognitive.ReasoningInput) (domain.Decision, error) {
	return domain.Decision{Kind: domain.DecisionFinal, Answer: "probably a DNS issue"}, nil
}

// crashing fails every reasoning step (a regression only visible online).
type crashing struct{}

func (crashing) Name() string { return "crashing" }
func (crashing) Decide(context.Context, cognitive.ReasoningInput) (domain.Decision, error) {
	return domain.Decision{}, errors.New("model output unparseable")
}

// offline: prompt_prefix_version "bad" regresses; everything else is the rule strategy.
func offlineFactory(spec profile.Spec) (cognitive.Strategy, error) {
	if spec.PromptPrefixVersion == "bad" {
		return wrongAnswer{}, nil
	}
	return cognitive.RuleBased{}, nil
}

func TestEvolutionGateApprovalCanaryAndRollback(t *testing.T) {
	var h *harness
	h = start(t, opts{workerFn: func(hh *harness, c *kernel.Config) {
		c.Strategies = &profile.Resolver{Loader: hh.store, Factory: func(spec profile.Spec) (cognitive.Strategy, error) {
			if spec.Effort == "high" { // online the candidate breaks
				return crashing{}, nil
			}
			return offlineFactory(spec)
		}}
	}})
	ctx := context.Background()
	svc := &eval.Service{Store: h.store, Artifacts: h.arts, Registry: h.reg, Factory: offlineFactory, Log: quiet}

	// 1. record a dataset from finished production runs.
	ds, err := h.store.CreateDataset(ctx, tenant, "checkout-latency", "alice")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		run := h.create(t, fmt.Sprintf("diagnose latency service=checkout #%d", i))
		h.waitStatus(t, run.ID, domain.RunCompleted)
		c, created, err := svc.RecordFromRun(ctx, tenant, ds.ID, run.ID, []string{"pool exhaustion"}, []string{"demo.restart"}, "holdout")
		if err != nil || !created || c.ServiceID != "checkout" {
			t.Fatalf("record: %+v %v %v", c, created, err)
		}
	}
	if _, _, err := svc.Evaluate(ctx, tenant, ds.ID, "sre-diagnosis", 1, "holdout", "alice"); !errors.Is(err, eval.ErrInvalid) {
		t.Fatalf("evaluating a DRAFT dataset must fail: %v", err)
	}
	if ds, err = h.store.FreezeDataset(ctx, tenant, ds.ID); err != nil || ds.ManifestHash == nil {
		t.Fatalf("freeze: %+v %v", ds, err)
	}
	if _, _, err := h.store.AddCase(ctx, tenant, ds.ID, eval.Case{Key: "late"}); !errors.Is(err, eval.ErrInvalid) {
		t.Fatalf("frozen dataset accepted a case: %v", err)
	}

	// 2. replay the base profile.
	er, m, err := svc.Evaluate(ctx, tenant, ds.ID, "sre-diagnosis", 1, "holdout", "alice")
	if err != nil || er.Status != "SUCCEEDED" || m.N != 2 || m.Accuracy != 1 || m.UnrecordedCalls != 0 {
		t.Fatalf("base eval: %+v %+v %v", er, m, err)
	}

	// 3. frozen fields cannot be evolved.
	if _, err := svc.Propose(ctx, tenant, "sre-diagnosis", 1, json.RawMessage(`{"tools":["demo.restart"]}`), "", "evolver"); !errors.Is(err, eval.ErrInvalid) {
		t.Fatalf("frozen patch: %v", err)
	}

	// 4. a regressing candidate fails the gate.
	bad, err := svc.Propose(ctx, tenant, "sre-diagnosis", 1, json.RawMessage(`{"prompt_prefix_version":"bad"}`), "shorter prompt", "evolver")
	if err != nil {
		t.Fatal(err)
	}
	bad, g, err := svc.EvaluateCandidate(ctx, tenant, bad.ID, ds.ID, "evolver")
	if err != nil || g.Pass || bad.Status != "GATE_FAILED" {
		t.Fatalf("bad candidate: %s %+v %v", bad.Status, g, err)
	}
	if _, err := svc.Approve(ctx, tenant, bad.ID, "bob"); !errors.Is(err, eval.ErrInvalid) {
		t.Fatalf("approving a gate failure: %v", err)
	}

	// 5. a non-inferior candidate passes, needs a distinct approver.
	good, err := svc.Propose(ctx, tenant, "sre-diagnosis", 1, json.RawMessage(`{"effort":"high"}`), "more effort", "evolver")
	if err != nil {
		t.Fatal(err)
	}
	good, g, err = svc.EvaluateCandidate(ctx, tenant, good.ID, ds.ID, "evolver")
	if err != nil || !g.Pass || good.Status != "PENDING_APPROVAL" || good.EvaluationID == nil {
		t.Fatalf("good candidate: %+v %+v %v", good, g, err)
	}
	if _, err := svc.Approve(ctx, tenant, good.ID, "evolver"); !errors.Is(err, domain.ErrApproverConflict) {
		t.Fatalf("self-approval: %v", err)
	}
	if good, err = svc.Approve(ctx, tenant, good.ID, "bob"); err != nil || good.Status != "APPROVED" {
		t.Fatalf("approve: %+v %v", good, err)
	}

	// 6. canary routing is sticky per routing key and splits traffic.
	ro, err := svc.StartCanary(ctx, tenant, good.ID, 50, eval.Guardrails{MinRuns: 2, MaxFailureRate: 0.2}, "bob")
	if err != nil || ro.Status != "CANARY" || ro.ToVersion != *good.CandidateVersion || ro.FromVersion != 1 {
		t.Fatalf("canary: %+v %v", ro, err)
	}
	if _, err := svc.StartCanary(ctx, tenant, good.ID, 10, eval.Guardrails{}, "bob"); err == nil {
		t.Fatal("second canary for a rolled-out candidate must fail")
	}
	canaryKeys := []string{}
	counts := map[int64]int{}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("incident-%d", i)
		v, rid := svc.ResolveProfile(ctx, tenant, "sre-diagnosis", key)
		v2, _ := svc.ResolveProfile(ctx, tenant, "sre-diagnosis", key)
		if v != v2 {
			t.Fatal("routing is not sticky")
		}
		counts[v]++
		if rid != nil {
			if *rid != ro.ID || v != ro.ToVersion {
				t.Fatalf("canary route: %d %s", v, *rid)
			}
			canaryKeys = append(canaryKeys, key)
		}
	}
	if counts[1] < 60 || counts[ro.ToVersion] < 60 {
		t.Fatalf("traffic split off: %v", counts)
	}

	// 7. canary runs crash online; the guardrail monitor rolls back.
	for _, key := range canaryKeys[:2] {
		data, _ := json.Marshal(map[string]string{"goal": "diagnose latency service=checkout " + key})
		ref, hash, _ := h.arts.Put(ctx, tenant, "goal", data)
		v, rid := svc.ResolveProfile(ctx, tenant, "sre-diagnosis", key)
		run, _, err := h.store.CreateRun(ctx, domain.CreateRunRequest{TenantID: tenant, ProfileID: "sre-diagnosis",
			ProfileVersion: v, RolloutID: rid, GoalRef: ref, GoalHash: hash, IdempotencyKey: key, InitiatorID: "alice"})
		if err != nil {
			t.Fatal(err)
		}
		h.waitStatus(t, run.ID, domain.RunFailed)
	}
	n, err := svc.MonitorTick(ctx)
	if err != nil || n != 1 {
		t.Fatalf("monitor: %d %v", n, err)
	}
	ro, _ = h.store.GetRollout(ctx, tenant, ro.ID)
	if ro.Status != "ROLLED_BACK" || ro.EndedAt == nil {
		t.Fatalf("rollout: %+v", ro)
	}
	if v, rid := svc.ResolveProfile(ctx, tenant, "sre-diagnosis", canaryKeys[0]); v != 1 || rid != nil {
		t.Fatalf("after rollback traffic must return to v1: %d %v", v, rid)
	}
	vs, _ := h.store.ListProfileVersions(ctx, tenant, "sre-diagnosis")
	for _, pv := range vs {
		if pv.Version == ro.ToVersion && pv.Status != "QUARANTINED" {
			t.Fatalf("candidate version status %s", pv.Status)
		}
	}
}

func TestEvolutionPromote(t *testing.T) {
	h := start(t, opts{})
	ctx := context.Background()
	svc := &eval.Service{Store: h.store, Artifacts: h.arts, Registry: h.reg, Factory: offlineFactory}
	ds, _ := h.store.CreateDataset(ctx, tenant, "p", "alice")
	run := h.create(t, "diagnose latency service=checkout")
	h.waitStatus(t, run.ID, domain.RunCompleted)
	if _, _, err := svc.RecordFromRun(ctx, tenant, ds.ID, run.ID, []string{"pool exhaustion"}, nil, "holdout"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.FreezeDataset(ctx, tenant, ds.ID); err != nil {
		t.Fatal(err)
	}
	c, err := svc.Propose(ctx, tenant, "sre-diagnosis", 1, json.RawMessage(`{"memory_top_k":8}`), "", "evolver")
	if err != nil {
		t.Fatal(err)
	}
	if c, _, err = svc.EvaluateCandidate(ctx, tenant, c.ID, ds.ID, "evolver"); err != nil || c.Status != "PENDING_APPROVAL" {
		t.Fatalf("%+v %v", c, err)
	}
	if _, err := svc.Approve(ctx, tenant, c.ID, "bob"); err != nil {
		t.Fatal(err)
	}
	ro, err := svc.StartCanary(ctx, tenant, c.ID, 10, eval.Guardrails{}, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if ro, err = svc.Promote(ctx, tenant, ro.ID, "bob"); err != nil || ro.Status != "PROMOTED" {
		t.Fatalf("%+v %v", ro, err)
	}
	if v, rid := svc.ResolveProfile(ctx, tenant, "sre-diagnosis", "any"); v != ro.ToVersion || rid != nil {
		t.Fatalf("promoted version not active: %d %v", v, rid)
	}
	if ro, err = svc.Rollback(ctx, tenant, ro.ID, "bob", "post-promotion regression"); err != nil || ro.Status != "ROLLED_BACK" {
		t.Fatalf("%+v %v", ro, err)
	}
	if v, _ := svc.ResolveProfile(ctx, tenant, "sre-diagnosis", "any"); v != 1 {
		t.Fatalf("rollback must restore v1, got %d", v)
	}
}
