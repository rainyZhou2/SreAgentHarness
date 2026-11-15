package projection

import (
	"encoding/json"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

func raw(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return b
}

func TestBuildOrdersBlocksAndKeepsEvidenceSemantics(t *testing.T) {
	start := time.Date(2026, 11, 15, 10, 24, 0, 0, time.UTC)
	run := domain.Run{
		ID:        "run-1",
		Version:   7,
		Status:    domain.RunRunning,
		CreatedAt: start,
		SessionID: ptr("session-1"),
		TurnID:    ptr("turn-1"),
		WorkOwner: &domain.WorkRunOwner{WorkID: "work-1", StageID: "investigate"},
	}
	steps := []domain.Step{
		{ID: "step-1", Seq: 1, Kind: domain.StepReason},
		{ID: "step-2", Seq: 2, Kind: domain.StepTool, Status: domain.StepSucceeded},
	}
	outputs := map[string]json.RawMessage{
		"step-1": raw(t, domain.Decision{Kind: domain.DecisionTool, ReasonSummary: "check the pool"}),
		"step-2": raw(t, domain.Observation{
			Tool:    "sre.service_health",
			Outcome: "ok",
			Content: raw(t, map[string]any{"bundle": map[string]any{"items": []any{
				map[string]any{
					"role": "error_rate", "status": "partial", "signal": "metrics",
					"source_id": "prom-prod", "data_ref": "obs:step2", "evidence_ref": "evidence:1",
					"summary": "error rate rose", "start": start, "end": start.Add(30 * time.Minute),
				},
			}}}),
		}),
	}
	result := &domain.Decision{
		Kind:         domain.DecisionFinal,
		Answer:       "the deploy is the likely trigger",
		EvidenceRefs: []string{"evidence:1"},
		Hypotheses: []domain.Hypothesis{
			{ID: "h1", Statement: "deploy caused it", Status: "VERIFIED"},
			{ID: "h2", Statement: "database saturation", Status: "UNVERIFIED"},
		},
		Attachments: []domain.Attachment{{Type: domain.AttachTable, Title: "errors"}},
	}
	expires := start.Add(time.Hour)

	view := Build(Input{
		Run:        run,
		Steps:      steps,
		StepOutput: outputs,
		Approvals: []domain.ApprovalRequest{{
			ID: "approval-1", ActionIntentID: "intent-1", RiskClass: domain.RiskR2,
			Target: "deployment://prod/checkout", ActionHash: "hash-1",
			RequiredApprovals: 2, Status: "PENDING", ExpiresAt: expires,
		}},
		Cost:       Cost{ModelRequests: 3, InputTokens: 100, OutputTokens: 50, CostUSD: 0.02},
		Result:     result,
		CanApprove: true,
	})

	if view.Status != "RUNNING" || view.RunVersion != 7 || view.SessionID != "session-1" || view.TurnID != "turn-1" {
		t.Fatalf("view identity: %+v", view)
	}
	if len(view.WorkIDs) != 1 || view.WorkIDs[0] != "work-1" {
		t.Fatalf("work ids: %+v", view.WorkIDs)
	}
	kinds := make([]string, 0, len(view.Blocks))
	for _, block := range view.Blocks {
		kinds = append(kinds, block.Kind)
	}
	want := []string{"status", "reasoning", "tool-group", "evidence", "hypothesis", "conclusion", "action", "attachments"}
	if len(kinds) != len(want) {
		t.Fatalf("block kinds = %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("block kinds = %v, want %v", kinds, want)
		}
		if view.Blocks[i].Ordinal != i {
			t.Fatalf("ordinal %d = %d", i, view.Blocks[i].Ordinal)
		}
	}

	evidence, ok := view.Blocks[3].Value.(EvidenceBlock)
	if !ok {
		t.Fatalf("evidence block type %T", view.Blocks[3].Value)
	}
	if evidence.Status != "partial" || !evidence.Quality.Partial || evidence.Signal != "metrics" ||
		evidence.Source != "prom-prod" || evidence.EvidenceRef != "evidence:1" {
		t.Fatalf("evidence: %+v", evidence)
	}

	conclusion, ok := view.Blocks[5].Value.(ConclusionBlock)
	if !ok {
		t.Fatalf("conclusion block type %T", view.Blocks[5].Value)
	}
	if conclusion.Verdict != "VERIFIED" || len(conclusion.Unverified) != 1 {
		t.Fatalf("conclusion: %+v", conclusion)
	}

	action, ok := view.Blocks[6].Value.(ActionBlock)
	if !ok {
		t.Fatalf("action block type %T", view.Blocks[6].Value)
	}
	if action.ApprovalID != "approval-1" || action.ActionID != "intent-1" || action.RequiredApprovals != 2 {
		t.Fatalf("action: %+v", action)
	}
	if len(action.Controls) != 2 {
		t.Fatalf("approver controls: %+v", action.Controls)
	}
}

func TestBuildIsStableAcrossCalls(t *testing.T) {
	run := domain.Run{ID: "run-1", Status: domain.RunCompleted, CreatedAt: time.Unix(0, 0).UTC()}
	input := Input{
		Run:   run,
		Steps: []domain.Step{{ID: "s1", Seq: 1, Kind: domain.StepReason}},
		StepOutput: map[string]json.RawMessage{
			"s1": raw(t, domain.Decision{Kind: domain.DecisionFinal, ReasonSummary: "done", Answer: "ok"}),
		},
	}
	first, err := json.Marshal(Build(input))
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(Build(input))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("projection is not deterministic:\n%s\n%s", first, second)
	}
}

func TestBlockJSONRoundTripPreservesFields(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	view := View{
		SchemaVersion: SchemaVersion, ProjectionVersion: ProjectionVersion, RunID: "r1", Status: "RUNNING",
		Blocks: []Block{
			{ID: "b1", Kind: "status", Value: StatusBlock{Status: "RUNNING", StartedAt: start, Cost: Cost{ModelRequests: 1}}},
			{ID: "b2", Kind: "evidence", Value: EvidenceBlock{Signal: "metrics", Source: "prom", Status: "ok"}},
		},
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var decoded View
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	reencoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(reencoded) {
		t.Fatalf("round trip lost fields:\n%s\n%s", encoded, reencoded)
	}
}

func TestBuildHidesActionControlsWithoutApprover(t *testing.T) {
	run := domain.Run{ID: "run-1", Status: domain.RunRunning}
	view := Build(Input{
		Run: run,
		Approvals: []domain.ApprovalRequest{{
			ID: "approval-1", ActionIntentID: "intent-1", RiskClass: domain.RiskR2,
			Target: "deployment://prod/checkout", ActionHash: "hash-1", Status: "PENDING",
		}},
	})
	for _, block := range view.Blocks {
		if block.Kind != "action" {
			continue
		}
		action, ok := block.Value.(ActionBlock)
		if !ok {
			t.Fatalf("action block type %T", block.Value)
		}
		if len(action.Controls) != 0 {
			t.Fatalf("viewer controls: %+v", action.Controls)
		}
		return
	}
	t.Fatal("action block missing")
}

func TestBuildDoesNotInventEvidenceFromToolStatus(t *testing.T) {
	run := domain.Run{ID: "run-1", Status: domain.RunRunning}
	outputs := map[string]json.RawMessage{
		"s1": raw(t, domain.Observation{Tool: "k8s.describe", Outcome: "ok", Content: raw(t, map[string]any{"pods": 3})}),
	}
	view := Build(Input{
		Run:        run,
		Steps:      []domain.Step{{ID: "s1", Seq: 1, Kind: domain.StepTool, Status: domain.StepSucceeded}},
		StepOutput: outputs,
	})
	for _, block := range view.Blocks {
		if block.Kind == "evidence" {
			t.Fatalf("tool success without a typed bundle must not become evidence: %+v", block)
		}
	}
}

func ptr[T any](v T) *T { return &v }
