package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/projection"
	"go-sre-harness/internal/store/pg"
)

type projectionStore struct {
	Store
	run       domain.Run
	steps     []domain.Step
	approvals []domain.ApprovalRequest
	cost      pg.CostSummary
}

func (s projectionStore) GetRun(context.Context, string, string) (domain.Run, error) {
	return s.run, nil
}

func (s projectionStore) ListSteps(context.Context, string, string) ([]domain.Step, error) {
	return s.steps, nil
}

func (s projectionStore) RunTrace(context.Context, string, string) (map[string][]domain.Thought, map[string][]domain.CapabilityRef, error) {
	return nil, nil, nil
}

func (s projectionStore) ListApprovals(context.Context, string, string) ([]domain.ApprovalRequest, error) {
	return s.approvals, nil
}

func (s projectionStore) RunCost(context.Context, string, string) (pg.CostSummary, error) {
	return s.cost, nil
}

func TestGetRunProjection(t *testing.T) {
	ctx := context.Background()
	arts := artifact.NewMemory()
	start := time.Date(2026, 11, 15, 10, 24, 0, 0, time.UTC)

	result, _ := json.Marshal(domain.Decision{
		Kind:   domain.DecisionFinal,
		Answer: "the deploy is the likely trigger",
		Hypotheses: []domain.Hypothesis{
			{ID: "h1", Statement: "deploy caused it", Status: "VERIFIED"},
		},
	})
	resultRef, _, err := arts.Put(ctx, "tenant-1", "result", result)
	if err != nil {
		t.Fatal(err)
	}
	output, _ := json.Marshal(domain.Observation{
		Tool:    "sre.service_health",
		Outcome: "ok",
		Content: json.RawMessage(`{"bundle":{"items":[{"role":"error_rate","status":"ok","signal":"metrics","source_id":"prom-prod","data_ref":"obs:step2","evidence_ref":"evidence:1","summary":"error rate rose","start":"2026-11-15T10:00:00Z","end":"2026-11-15T10:24:00Z"}]}}`),
	})
	outputRef, _, err := arts.Put(ctx, "tenant-1", "step-output", output)
	if err != nil {
		t.Fatal(err)
	}
	run := domain.Run{
		ID: "run-1", TenantID: "tenant-1", Status: domain.RunCompleted, CreatedAt: start,
		ResultRef: &resultRef,
	}
	store := projectionStore{
		run: run,
		steps: []domain.Step{
			{ID: "step-1", Seq: 1, Kind: domain.StepReason, Status: domain.StepSucceeded},
			{ID: "step-2", Seq: 2, Kind: domain.StepTool, Status: domain.StepSucceeded, OutputRef: &outputRef},
		},
		approvals: []domain.ApprovalRequest{{
			ID: "approval-1", ActionIntentID: "intent-1", RiskClass: domain.RiskR2,
			Target: "deployment://prod/checkout", ActionHash: "hash-1",
			RequiredApprovals: 2, Status: "PENDING", ExpiresAt: start.Add(time.Hour),
		}},
		cost: pg.CostSummary{Requests: 3, InputTokens: 100, OutputTokens: 50, CostUSD: 0.02},
	}
	s := &Server{Store: store, Artifacts: arts, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	req := httptest.NewRequest(http.MethodGet, "/api/runs/run-1/projection", nil)
	req.SetPathValue("run_id", "run-1")
	req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{
		Tenant: "tenant-1", Subject: "alice", Roles: []auth.Role{auth.Viewer}, Method: "token",
	}))
	rec := httptest.NewRecorder()
	s.getRunProjection(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var view projection.View
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.RunID != "run-1" || view.Status != "COMPLETED" {
		t.Fatalf("view = %+v", view)
	}
	kinds := make([]string, 0, len(view.Blocks))
	for _, block := range view.Blocks {
		kinds = append(kinds, block.Kind)
	}
	want := []string{"status", "tool-group", "evidence", "hypothesis", "conclusion", "action"}
	if len(kinds) != len(want) {
		t.Fatalf("kinds = %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("kinds = %v, want %v", kinds, want)
		}
	}
	if view.Blocks[2].Kind != "evidence" || view.Blocks[2].ID == "" {
		t.Fatalf("evidence block = %+v", view.Blocks[2])
	}
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("missing ETag")
	}
	req = httptest.NewRequest(http.MethodGet, "/api/runs/run-1/projection", nil)
	req.SetPathValue("run_id", "run-1")
	req.Header.Set("If-None-Match", etag)
	req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{
		Tenant: "tenant-1", Subject: "alice", Roles: []auth.Role{auth.Viewer}, Method: "token",
	}))
	rec = httptest.NewRecorder()
	s.getRunProjection(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("conditional status = %d", rec.Code)
	}
}
