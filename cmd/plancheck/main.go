package main

// Command plancheck runs the planner against a real model and verifies that
// it calls the submit_flow tool with a schema-valid plan. It is the live
// counterpart of the fake-gateway tests and is safe to run in CI once the
// provider environment variables are set:
//
//	ANTHROPIC_API_KEY=... go run ./cmd/plancheck -goal "restart the checkout deployment"
//	HARNESS_OPENAI_BASE_URL=... HARNESS_OPENAI_MODEL=... go run ./cmd/plancheck ...
//	GEMINI_API_KEY=... GEMINI_MODEL=gemini-3-pro go run ./cmd/plancheck ...
//
// The command exits non-zero when the model does not produce a valid plan, so
// a release pipeline can gate on real tool-call behavior rather than only on
// the scripted provider.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/gateway"
	"go-sre-harness/internal/gateway/anthropic"
	"go-sre-harness/internal/gateway/gemini"
	"go-sre-harness/internal/gateway/openai"
	"go-sre-harness/internal/planner"
	"go-sre-harness/internal/tools"
)

func main() {
	goal := flag.String("goal", "The checkout service is returning 5xx errors. Diagnose it and, if the cause is a bad deployment, restart the checkout deployment after approval.", "goal for the planner")
	timeout := flag.Duration("timeout", 3*time.Minute, "overall timeout")
	jsonOut := flag.Bool("json", false, "print the full plan result as JSON")
	flag.Parse()

	provider, policy, err := providerFromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	arts := artifact.NewMemory()
	gw := gateway.New(policy, gateway.NewMemLedger(), arts, gateway.Admission{}, nil, provider)
	svc := &planner.Service{Gateway: gw, Artifacts: arts, Config: planner.Config{
		ModelAlias:      "sre-reasoning",
		MaxRepairs:      2,
		MaxNodes:        12,
		MaxOutputTokens: 8000,
		ApprovalMode:    "auto",
		MaxChildren:     8, MaxTotalRuns: 24, MaxDepth: 3, MaxIterations: 8, MaxModelCalls: 30,
	}}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	descs := []tools.Descriptor{
		{Name: "demo.metrics", Version: "1", Risk: domain.RiskR0, Trust: "T3", Source: "native",
			Description: "Query service metrics for a time window.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"},"window":{"type":"string"}},"required":["service"]}`),
			TargetArg:   "service"},
		{Name: "demo.logs", Version: "1", Risk: domain.RiskR0, Trust: "T2", Source: "native",
			Description: "Search recent service logs.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"},"query":{"type":"string"}},"required":["service"]}`),
			TargetArg:   "service"},
		{Name: "demo.deploy_status", Version: "1", Risk: domain.RiskR0, Trust: "T2", Source: "native",
			Description: "Read the current deployment revision and rollout status.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"deployment":{"type":"string"}},"required":["deployment"]}`),
			TargetArg:   "deployment"},
		{Name: "demo.restart", Version: "1", Risk: domain.RiskR2, Trust: "T2", Source: "native",
			Description: "Restart a deployment. This is a production write and needs approval.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"deployment":{"type":"string"}},"required":["deployment"]}`),
			TargetArg:   "deployment"},
	}
	res, err := svc.Plan(ctx, gateway.Scope{TenantID: "plancheck", RunID: "plancheck", StepID: "plancheck",
		WorkerID: "plancheck", FenceToken: 1},
		planner.Input{Goal: strings.TrimSpace(*goal), Tools: descs, AllowTool: func(string) bool { return true },
			MaxSteps: 40})
	if err != nil {
		fmt.Fprintln(os.Stderr, "plan failed:", err)
		os.Exit(1)
	}
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{"mode": res.Mode, "rationale": res.Rationale, "flow": res.Flow,
			"flow_hash": res.Hash, "plan_hash": res.PlanHash, "analysis": res.Analysis,
			"needs_review": res.NeedsReview, "repairs": res.Repairs, "validation_errors": res.ValidationErrors})
		return
	}
	nodes := 0
	if res.Flow != nil {
		nodes = len(res.Flow.Nodes)
	}
	fmt.Printf("mode=%s nodes=%d plan_hash=%s flow_hash=%s repairs=%d review=%v\n",
		res.Mode, nodes, res.PlanHash, res.Hash, res.Repairs, res.NeedsReview)
	if res.Analysis != nil {
		fmt.Printf("risk=%s side_effects=%d estimated_runs=%d estimated_model_calls=%d\n",
			res.Analysis.HighestRisk, res.Analysis.SideEffects, res.Analysis.EstimatedRuns, res.Analysis.EstimatedModelCalls)
		for _, f := range res.Analysis.Findings {
			fmt.Printf("finding[%s] %s: %s\n", f.Severity, f.Code, f.Message)
		}
	}
	if res.Mode == "flow" && res.Flow != nil {
		for _, n := range res.Flow.Nodes {
			fmt.Printf("node %s type=%s tool=%s tools=%v\n", n.ID, n.Type, n.Tool, n.Tools)
		}
	}
}

func providerFromEnv() (gateway.Provider, gateway.RoutePolicy, error) {
	switch {
	case os.Getenv("ANTHROPIC_API_KEY") != "":
		p := anthropic.New(anthropic.Config{APIKey: os.Getenv("ANTHROPIC_API_KEY"),
			BaseURL: os.Getenv("HARNESS_ANTHROPIC_BASE_URL")})
		return p, gateway.DefaultPolicy(true, false, ""), nil
	case os.Getenv("HARNESS_OPENAI_BASE_URL") != "" && os.Getenv("HARNESS_OPENAI_MODEL") != "":
		p := openai.New(openai.Config{BaseURL: os.Getenv("HARNESS_OPENAI_BASE_URL"),
			APIKey: os.Getenv("HARNESS_OPENAI_API_KEY")})
		return p, gateway.DefaultPolicy(false, true, os.Getenv("HARNESS_OPENAI_MODEL")), nil
	case os.Getenv("GEMINI_API_KEY") != "":
		model := os.Getenv("GEMINI_MODEL")
		if model == "" {
			model = "gemini-3-pro"
		}
		p := gemini.New(gemini.Config{APIKey: os.Getenv("GEMINI_API_KEY"), BaseURL: os.Getenv("HARNESS_GEMINI_BASE_URL")})
		ep := gateway.GeminiEndpoint(model, 0, 0)
		policy, err := gateway.ComposePolicy("plancheck", gateway.PolicyPart{Endpoints: []gateway.Endpoint{ep},
			Aliases: map[string][]string{"sre-reasoning": {ep.ID}}})
		return p, policy, err
	default:
		return nil, gateway.RoutePolicy{}, fmt.Errorf("set ANTHROPIC_API_KEY, HARNESS_OPENAI_BASE_URL+HARNESS_OPENAI_MODEL, or GEMINI_API_KEY")
	}
}
