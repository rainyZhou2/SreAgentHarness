package planner

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/tools"
)

func TestRouterClassifierCatchesParaphrases(t *testing.T) {
	r := DefaultRouter()
	// None of these match the regex lists; the classifier decides.
	cases := map[string]string{
		"bounce the payments pods please":    "flow",
		"please revert yesterday's change":   "flow",
		"帮忙把订单服务回退":                          "flow",
		"请帮我降级推荐服务":                          "flow",
		"what's going on with orders":        "react",
		"网关现在什么情况":                           "react",
		"latency of the search service":      "react",
		"how many errors did billing return": "react",
	}
	for goal, want := range cases {
		got, why := r.Route(Input{Goal: goal}, "")
		if got != want {
			t.Errorf("Route(%q) = %s (%s), want %s", goal, got, why, want)
		}
	}
	// Explicit signals still win over the classifier.
	if m, _ := r.Route(Input{Goal: "bounce the payments pods please", ModeHint: "react"}, ""); m != "react" {
		t.Fatal("hint must win")
	}
	if m, _ := r.Route(Input{Goal: "why is checkout slow"}, ""); m != "react" {
		t.Fatal("react pattern must win")
	}
	// A classifier with no confident answer falls through to ReAct.
	r.Classifier = NewNgramClassifier(nil)
	if m, why := r.Route(Input{Goal: "bounce the payments pods please"}, ""); m != "react" || why != "no signal" {
		t.Fatalf("empty classifier: %s %s", m, why)
	}
}

func TestOverrideOnlyTightens(t *testing.T) {
	base := Config{ModelAlias: "big", ApprovalMode: "auto", MaxNodes: 20, MaxModelCalls: 40, MaxEstimatedTokens: 1000,
		MaxRepairs: 2, Fallbacks: []FallbackRule{{ProfileID: "default-rb", Version: 1}}}
	two, five := 1, 5
	eff := base.effective(&Override{ModelLadder: []string{"small", "big"}, ApprovalMode: "never", MaxNodes: 50,
		MaxModelCalls: 10, MaxEstimatedTokens: 5000, MaxRepairs: &five,
		Fallbacks: []FallbackRule{{Services: []string{"db"}, ProfileID: "db-rb", Version: 2}}})
	if eff.ApprovalMode != "auto" || eff.MaxNodes != 20 || eff.MaxModelCalls != 10 || eff.MaxEstimatedTokens != 1000 || eff.MaxRepairs != 2 {
		t.Fatalf("override loosened the config: %+v", eff)
	}
	if eff.aliasFor(0) != "small" || eff.aliasFor(1) != "big" || eff.aliasFor(7) != "big" {
		t.Fatal("ladder")
	}
	if id, v, _ := eff.fallbackFor("db is slow", []string{"db"}); id != "db-rb" || v != 2 {
		t.Fatalf("service rule: %s@%d", id, v)
	}
	if id, _, _ := eff.fallbackFor("checkout", []string{"checkout"}); id != "default-rb" {
		t.Fatalf("rule order: %s", id)
	}
	eff = base.effective(&Override{ApprovalMode: "always", MaxRepairs: &two})
	if eff.ApprovalMode != "always" || eff.MaxRepairs != 1 {
		t.Fatalf("stricter values must apply: %+v", eff)
	}
	if StricterApproval("never", "auto") != "auto" || StricterApproval("always", "never") != "always" {
		t.Fatal("StricterApproval")
	}
	if err := (FallbackRule{ProfileID: "x", Version: 1, GoalPattern: "("}).Validate(); err == nil {
		t.Fatal("bad pattern accepted")
	}
	if rules, err := ParseFallbacks(`[{"goal_pattern":"rollback","profile_id":"rb","version":3}]`); err != nil || len(rules) != 1 {
		t.Fatalf("parse: %v %v", rules, err)
	}
}

func TestModelLadderEscalatesOnRepair(t *testing.T) {
	bad := `{"mode":"flow","rationale":"x","flow":{"entry":"q","nodes":[
		{"id":"q","type":"tool","tool":"made.up","next":[{"to":"done"}]},{"id":"done","type":"end"}]}}`
	gen := &scriptedGen{replies: []any{submit(bad), submit(validFlow)}}
	res, err := newService(gen, Config{MaxRepairs: 2, ModelLadder: []string{"small", "large"}}).Plan(context.Background(),
		gw.Scope{TenantID: "t"}, baseInput())
	if err != nil {
		t.Fatal(err)
	}
	if gen.reqs[0].ModelAlias != "small" || gen.reqs[1].ModelAlias != "large" || res.ModelAlias != "large" {
		t.Fatalf("aliases %s,%s result %s", gen.reqs[0].ModelAlias, gen.reqs[1].ModelAlias, res.ModelAlias)
	}
}

func TestPlanCacheReusesAndRevalidates(t *testing.T) {
	cache := NewMemoryCache(0, 0)
	gen := &scriptedGen{replies: []any{submit(validFlow)}}
	s := newService(gen, Config{Cache: cache})
	first, err := s.Plan(context.Background(), gw.Scope{TenantID: "t"}, baseInput())
	if err != nil || first.Cached {
		t.Fatalf("first: %+v %v", first, err)
	}
	second, err := s.Plan(context.Background(), gw.Scope{TenantID: "t"}, baseInput())
	if err != nil || !second.Cached || second.PlanHash != first.PlanHash || len(gen.reqs) != 1 {
		t.Fatalf("second must be a cache hit without a model call: %+v %v calls=%d", second, err, len(gen.reqs))
	}
	// Another tenant never sees the entry.
	gen.replies = []any{submit(validFlow)}
	if r, _ := s.Plan(context.Background(), gw.Scope{TenantID: "other"}, baseInput()); r.Cached {
		t.Fatal("cache crossed tenants")
	}
	// A different tool set is a different key.
	in := baseInput()
	in.Tools = catalog[:3]
	gen.replies = []any{submit(validFlow)}
	if r, _ := s.Plan(context.Background(), gw.Scope{TenantID: "t"}, in); r.Cached {
		t.Fatal("tool change must miss")
	}
	// Replans and flagged inputs are never cached.
	in = baseInput()
	in.Replan = &Replan{Generation: 1, Failure: "x"}
	gen.replies = []any{submit(validFlow)}
	if r, _ := s.Plan(context.Background(), gw.Scope{TenantID: "t"}, in); r.Cached {
		t.Fatal("replan used the cache")
	}
}

func TestAutoPinAndDryRun(t *testing.T) {
	plan := `{"mode":"flow","rationale":"look around","flow":{"entry":"a","nodes":[
		{"id":"a","type":"agent","role":"investigator","max_steps":3,"next":[{"to":"done"}]},
		{"id":"done","type":"end"}]}}`
	gen := &scriptedGen{replies: []any{submit(plan)}}
	res, err := newService(gen, Config{}).Plan(context.Background(), gw.Scope{TenantID: "t"}, baseInput())
	if err != nil {
		t.Fatal(err)
	}
	tools := res.Flow.Nodes[0].Tools
	if strings.Join(tools, ",") != "logs.search,prom.query" {
		t.Fatalf("agent not pinned to read-only tools: %v", tools)
	}
	if res.NeedsReview || res.Analysis.HighestRisk != domain.RiskR0 {
		t.Fatalf("pinned read-only plan should not need review: %+v", res.Analysis)
	}
	if _, ok := findingCodes(*res.Analysis)["agent_tools_pinned"]; !ok {
		t.Fatal("missing pin finding")
	}
	sim := res.Analysis.Simulation
	if sim == nil || !sim.Completed || strings.Join(sim.Path, ",") != "a" {
		t.Fatalf("dry run: %+v", sim)
	}
	if res.Review == nil || len(res.Review.Steps) == 0 || !strings.Contains(res.Review.Summary, "read-only") {
		t.Fatalf("review: %+v", res.Review)
	}

	// A graph whose default path exceeds its own step limit is rejected by
	// the dry run and repaired.
	long := `{"mode":"flow","rationale":"three probes","flow":{"entry":"a","limits":{"max_steps":2},"nodes":[
		{"id":"a","type":"tool","tool":"prom.query","next":[{"to":"b"}]},
		{"id":"b","type":"tool","tool":"prom.query","next":[{"to":"c"}]},
		{"id":"c","type":"tool","tool":"prom.query","next":[{"to":"done"}]},
		{"id":"done","type":"end"}]}}`
	gen = &scriptedGen{replies: []any{submit(long), submit(validFlow)}}
	res, err = newService(gen, Config{MaxRepairs: 1}).Plan(context.Background(), gw.Scope{TenantID: "t"}, baseInput())
	if err != nil || res.Repairs != 1 || !strings.Contains(strings.Join(res.ValidationErrors, ";"), "dry run") {
		t.Fatalf("dry-run rejection: %+v %v", res, err)
	}
	// The R2 plan's dry run records the approval before the restart.
	if sim := res.Analysis.Simulation; sim == nil || len(sim.Unapproved) != 0 || strings.Join(sim.Approvals, ",") != "ok" {
		t.Fatalf("approval order: %+v", sim)
	}
}

func TestTokenEstimateUsesTypicalOutput(t *testing.T) {
	fs := compileSpec(t, `{"entry":"a","nodes":[
		{"id":"a","type":"agent","role":"x","tools":["prom.query"],"max_steps":10,"next":[{"to":"done"}]},
		{"id":"done","type":"end"}]}`)
	a := Analyze(fs, AnalysisOptions{Tools: catalog, Budget: Budget{MaxOutputTokens: 8000, TypicalOutputTokens: 1000,
		MaxEstimatedTokens: 50000}})
	if a.EstimatedOutputTokens != 10000 || a.WorstCaseOutputTokens != 80000 || len(a.Errors()) != 0 {
		t.Fatalf("estimates: %+v", a)
	}
	a = Analyze(fs, AnalysisOptions{Tools: catalog, Budget: Budget{MaxOutputTokens: 8000, MaxEstimatedTokens: 50000}})
	if len(a.Errors()) != 1 {
		t.Fatalf("worst case must be checked without a typical value: %v", a.Errors())
	}
}

func TestFallbackRulePerService(t *testing.T) {
	dbRunbook := json.RawMessage(`{"flow":{"entry":"q","limits":{"max_steps":5},"nodes":[
		{"id":"q","type":"tool","tool":"logs.search","next":[{"to":"done"}]},{"id":"done","type":"end"}]}}`)
	def := json.RawMessage(`{"flow":{"entry":"q","limits":{"max_steps":5},"nodes":[
		{"id":"q","type":"tool","tool":"prom.query","next":[{"to":"done"}]},{"id":"done","type":"end"}]}}`)
	cfg := Config{FallbackProfileID: "default", FallbackVersion: 1,
		Fallbacks: []FallbackRule{{Name: "db", Services: []string{"db"}, ProfileID: "db-runbook", Version: 1}}}
	in := baseInput()
	in.ModeHint, in.Services = "flow", []string{"db"}
	s := newService(&scriptedGen{replies: []any{gw.Response{}}}, cfg)
	s.Profiles = profiles{"default": def, "db-runbook": dbRunbook}
	res, err := s.Plan(context.Background(), gw.Scope{TenantID: "t"}, in)
	if err != nil || !res.Fallback || res.Flow.Nodes[0].Tool != "logs.search" || res.FallbackSource != "rule db" {
		t.Fatalf("service rule: %+v %v", res, err)
	}
}

func TestExemplarsHistoryAndReplanDiff(t *testing.T) {
	in := baseInput()
	in.Exemplars = []Exemplar{{MemoryID: "m1", Kind: "plan", Title: "Plan exemplar for checkout", Content: `Flow: {"entry":"q"}`},
		{MemoryID: "m2", Kind: "pattern", Title: "Reasoning pattern", Content: "check deploys first"}}
	for i := 0; i < 12; i++ {
		in.History = append(in.History, Message{Role: "user", Text: "unrelated chatter about lunch plans number " + string(rune('a'+i))})
	}
	in.History[2] = Message{Role: "user", Text: "checkout was restarted last week and it was unhealthy afterwards"}
	in.Replan = &Replan{Generation: 1, Failure: "q failed", PreviousFlow: json.RawMessage(`{"entry":"q","nodes":[
		{"id":"q","type":"tool","tool":"prom.query","next":[{"to":"done"}]},{"id":"done","type":"end"}]}`)}
	gen := &scriptedGen{replies: []any{submit(validFlow)}}
	res, err := newService(gen, Config{}).Plan(context.Background(), gw.Scope{TenantID: "t"}, in)
	if err != nil {
		t.Fatal(err)
	}
	prompt := lastPrompt(gen.reqs[0])
	if !strings.Contains(prompt, "[exemplars]") || !strings.Contains(prompt, "- memory=m1 kind=plan") ||
		!strings.Contains(prompt, "source=memory kind=pattern") {
		t.Fatalf("exemplars not rendered:\n%s", prompt)
	}
	if strings.Join(res.Exemplars, ",") != "m1,m2" {
		t.Fatalf("exemplar ids: %v", res.Exemplars)
	}
	if !strings.Contains(prompt, "checkout was restarted last week") || !strings.Contains(prompt, "earlier messages omitted") {
		t.Fatalf("relevant old message not selected:\n%s", prompt)
	}
	if res.Review == nil || res.Review.DiffBase != "previous_generation" || len(res.Review.Diff) == 0 {
		t.Fatalf("replan diff: %+v", res.Review)
	}
	joined := strings.Join(res.Review.Diff, "\n")
	if !strings.Contains(joined, "+ ok (approval)") || !strings.Contains(joined, "+ restart (tool k8s.rollout_restart)") {
		t.Fatalf("diff content:\n%s", joined)
	}
}

func TestTenantApprovalOverride(t *testing.T) {
	read := `{"mode":"flow","rationale":"read","flow":{"entry":"q","nodes":[
		{"id":"q","type":"tool","tool":"prom.query","next":[{"to":"done"}]},{"id":"done","type":"end"}]}}`
	in := baseInput()
	in.Override = &Override{ApprovalMode: "always"}
	gen := &scriptedGen{replies: []any{submit(read)}}
	res, err := newService(gen, Config{ApprovalMode: "auto"}).Plan(context.Background(), gw.Scope{TenantID: "t"}, in)
	if err != nil || !res.NeedsReview || res.ApprovalMode != "always" {
		t.Fatalf("tenant always: %+v %v", res, err)
	}
	_ = tools.Descriptor{}
}
