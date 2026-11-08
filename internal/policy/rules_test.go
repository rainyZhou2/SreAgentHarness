package policy

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

type fakeCounter struct {
	n      int
	err    error
	calls  int
	target string
	since  time.Time
}

func (f *fakeCounter) CountActions(_ context.Context, _, _, target string, since time.Time) (int, error) {
	f.calls++
	f.target, f.since = target, since
	return f.n, f.err
}

func mustEngine(t *testing.T, raw string, c ActionCounter) RuleEngine {
	t.Helper()
	var cfg RuleConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	e, err := NewRuleEngine(cfg, c)
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}
	return e
}

func desc(name string, risk domain.RiskClass) *tools.Descriptor {
	return &tools.Descriptor{Name: name, Risk: risk, TargetArg: "service", Source: "native"}
}

func args(m map[string]any) json.RawMessage {
	b, _ := json.Marshal(m)
	return b
}

func TestRuleEngineFallsBackToBaseEngine(t *testing.T) {
	e := mustEngine(t, `{"default_environment":"staging","allow_r1_without_approval":true}`, nil)
	d := e.EvaluateRequest(context.Background(), Request{Tool: desc("svc.scale", domain.RiskR1), Args: args(map[string]any{"service": "api"})})
	if d.Verdict != Allow || d.Environment != "staging" {
		t.Fatalf("got %+v, want allow in staging", d)
	}
	d = e.EvaluateRequest(context.Background(), Request{Tool: nil})
	if d.Verdict != Deny {
		t.Fatalf("unregistered tool: got %v", d.Verdict)
	}
}

func TestRuleEngineEnvironmentResolution(t *testing.T) {
	e := mustEngine(t, `{
		"default_environment":"production","allow_r1_without_approval":true,
		"environments":[
			{"name":"staging","targets":["*-staging","staging/*"]},
			{"name":"lab","labels":{"team":"sre"}}
		]}`, nil)
	ctx := context.Background()
	cases := []struct {
		target string
		labels map[string]string
		env    string
		want   Verdict
	}{
		{"api-staging", nil, "staging", Allow},
		{"staging/eu/api", nil, "staging", Allow}, // * crosses '/'
		{"api", map[string]string{"team": "sre"}, "lab", Allow},
		{"api", nil, "production", RequireApproval},
	}
	for _, c := range cases {
		d := desc("svc.restart", domain.RiskR1)
		d.Labels = c.labels
		got := e.EvaluateRequest(ctx, Request{Tool: d, Args: args(map[string]any{"service": c.target})})
		if got.Environment != c.env || got.Verdict != c.want {
			t.Errorf("target %q: got env=%q verdict=%v, want %q/%v", c.target, got.Environment, got.Verdict, c.env, c.want)
		}
		if env := e.EnvironmentFor(*d, args(map[string]any{"service": c.target})); env != c.env {
			t.Errorf("EnvironmentFor(%q) = %q, want %q", c.target, env, c.env)
		}
	}
}

func TestRuleEngineMostRestrictiveRuleWins(t *testing.T) {
	e := mustEngine(t, `{"rules":[
		{"name":"allow-reads","match":{"risks":["R0"]},"effect":"allow"},
		{"name":"two-approvers","match":{"tools":["db.*"]},"effect":"require_approval","approvals":2},
		{"name":"one-approver","match":{"tools":["db.*"]},"effect":"require_approval"},
		{"name":"no-drop","match":{"tools":["db.drop*"]},"effect":"deny","reason":"never"}
	]}`, nil)
	ctx := context.Background()
	d := e.EvaluateRequest(ctx, Request{Tool: desc("db.failover", domain.RiskR1), Args: args(map[string]any{"service": "orders"})})
	if d.Verdict != RequireApproval || d.RequiredApprovals != 2 {
		t.Fatalf("got %+v, want 2 approvals", d)
	}
	if len(d.MatchedRules) != 2 {
		t.Fatalf("matched rules %v", d.MatchedRules)
	}
	d = e.EvaluateRequest(ctx, Request{Tool: desc("db.drop_table", domain.RiskR1), Args: args(map[string]any{"service": "orders"})})
	if d.Verdict != Deny || d.Reason != `policy rule "no-drop": never` {
		t.Fatalf("got %+v", d)
	}
}

func TestRuleEngineArgsAndBlastRadius(t *testing.T) {
	e := mustEngine(t, `{"rules":[
		{"name":"prod-ns","match":{"args":{"namespace":"prod-*"}},"effect":"require_approval"},
		{"name":"wide","match":{"blast_radius":{"arg":"hosts","over":3}},"effect":"deny"}
	]}`, nil)
	ctx := context.Background()
	tool := desc("k8s.rollout", domain.RiskR0)
	d := e.EvaluateRequest(ctx, Request{Tool: tool, Args: args(map[string]any{"service": "a", "namespace": "prod-eu"})})
	if d.Verdict != RequireApproval {
		t.Fatalf("namespace rule: %+v", d)
	}
	d = e.EvaluateRequest(ctx, Request{Tool: tool, Args: args(map[string]any{"service": "a", "namespace": "dev"})})
	if d.Verdict != Allow {
		t.Fatalf("no match should keep base verdict: %+v", d)
	}
	for _, hosts := range []any{[]string{"a", "b", "c", "d"}, "a,b,c,d", map[string]any{"a": 1, "b": 1, "c": 1, "d": 1}} {
		d = e.EvaluateRequest(ctx, Request{Tool: tool, Args: args(map[string]any{"service": "a", "hosts": hosts})})
		if d.Verdict != Deny {
			t.Errorf("hosts=%v: %+v", hosts, d)
		}
	}
	d = e.EvaluateRequest(ctx, Request{Tool: tool, Args: args(map[string]any{"service": "a", "hosts": []string{"a", "b", "c"}})})
	if d.Verdict != Allow {
		t.Fatalf("at the limit should not match: %+v", d)
	}
}

func TestRuleEngineWindows(t *testing.T) {
	e := mustEngine(t, `{"rules":[
		{"name":"freeze","match":{"windows":[{"days":["fri"],"start":"18:00","end":"02:00","timezone":"UTC"}]},"effect":"deny"}
	]}`, nil)
	ctx := context.Background()
	tool := desc("svc.deploy", domain.RiskR0)
	friEvening := time.Date(2026, 11, 6, 20, 0, 0, 0, time.UTC) // Friday
	friNoon := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		at   time.Time
		want Verdict
	}{{friEvening, Deny}, {friNoon, Allow}} {
		d := e.EvaluateRequest(ctx, Request{Tool: tool, Args: args(map[string]any{"service": "a"}), Now: c.at})
		if d.Verdict != c.want {
			t.Errorf("at %s: got %v, want %v", c.at, d.Verdict, c.want)
		}
	}
}

func TestWindowContains(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	w := Window{From: &from, To: &to}
	if err := w.compile(); err != nil {
		t.Fatal(err)
	}
	if !w.contains(from.Add(time.Hour)) || w.contains(to) || w.contains(from.Add(-time.Second)) {
		t.Fatal("absolute window bounds")
	}
	bad := []Window{{Start: "25:00", End: "01:00"}, {Days: []string{"someday"}}, {Timezone: "Nowhere/City"}, {Start: "10:00"}}
	for i := range bad {
		if err := bad[i].compile(); err == nil {
			t.Errorf("window %+v should not compile", bad[i])
		}
	}
}

func TestRuleEngineRate(t *testing.T) {
	ctx := context.Background()
	raw := `{"rules":[{"name":"burst","match":{"tools":["svc.restart"],"rate":{"window":"10m","over":3,"per":"target"}},"effect":"deny"}]}`
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	c := &fakeCounter{n: 3}
	e := mustEngine(t, raw, c)
	req := Request{TenantID: "t1", Tool: desc("svc.restart", domain.RiskR0), Args: args(map[string]any{"service": "api"}), Now: now}
	if d := e.EvaluateRequest(ctx, req); d.Verdict != Deny {
		t.Fatalf("at the threshold: %+v", d)
	}
	if c.target != "api" || !c.since.Equal(now.Add(-10*time.Minute)) {
		t.Fatalf("counter called with target=%q since=%s", c.target, c.since)
	}
	c.n = 2
	if d := e.EvaluateRequest(ctx, req); d.Verdict != Allow {
		t.Fatalf("below the threshold: %+v", d)
	}
	// Fail closed when history is unavailable.
	c.err = errors.New("db down")
	if d := e.EvaluateRequest(ctx, req); d.Verdict != Deny {
		t.Fatalf("counter error must apply the rule: %+v", d)
	}
	noCounter := mustEngine(t, raw, nil)
	if d := noCounter.EvaluateRequest(ctx, req); d.Verdict != Deny {
		t.Fatalf("missing counter must apply the rule: %+v", d)
	}
}

func TestRuleEngineConfigValidation(t *testing.T) {
	bad := []string{
		`{"rules":[{"name":"","effect":"deny"}]}`,
		`{"rules":[{"name":"a","effect":"deny"},{"name":"a","effect":"deny"}]}`,
		`{"rules":[{"name":"a","effect":"maybe"}]}`,
		`{"rules":[{"name":"a","effect":"deny","approvals":2}]}`,
		`{"rules":[{"name":"a","effect":"require_approval","approvals":9}]}`,
		`{"rules":[{"name":"a","effect":"deny","match":{"risks":["R9"]}}]}`,
		`{"rules":[{"name":"a","effect":"deny","match":{"blast_radius":{"arg":"","over":1}}}]}`,
		`{"rules":[{"name":"a","effect":"deny","match":{"rate":{"window":"1m","over":0}}}]}`,
		`{"rules":[{"name":"a","effect":"deny","match":{"rate":{"window":"1m","over":1,"per":"tenant"}}}]}`,
		`{"environments":[{"name":"x"}]}`,
	}
	for _, raw := range bad {
		var cfg RuleConfig
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			continue // a decode error is also a rejection
		}
		if _, err := NewRuleEngine(cfg, nil); err == nil {
			t.Errorf("config %s should be rejected", raw)
		}
	}
}

func TestGuardedFloorAppliesToRules(t *testing.T) {
	e := mustEngine(t, `{"rules":[{"name":"yolo","match":{"tools":["*"]},"effect":"allow"}]}`, nil)
	g := Guarded(e)
	ctx := context.Background()
	d := EvaluateRequest(ctx, g, Request{Tool: desc("svc.delete", domain.RiskR3), Args: args(map[string]any{"service": "a"})})
	if d.Verdict != Deny {
		t.Fatalf("R3 must stay denied: %+v", d)
	}
	d = EvaluateRequest(ctx, g, Request{Tool: desc("svc.failover", domain.RiskR2), Args: args(map[string]any{"service": "a"})})
	if d.Verdict != RequireApproval || d.RequiredApprovals < 1 {
		t.Fatalf("R2 must need approval: %+v", d)
	}
	d = EvaluateRequest(ctx, g, Request{Tool: desc("svc.failover", domain.RiskR1), Args: args(map[string]any{})})
	if d.Verdict != Deny {
		t.Fatalf("missing target must be denied: %+v", d)
	}
}

func TestGlob(t *testing.T) {
	cases := []struct {
		p, s string
		want bool
	}{
		{"*", "", true},
		{"a*", "abc", true},
		{"*c", "abc", true},
		{"a*c", "a/b/c", true},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
		{"prod-*", "staging-1", false},
		{"exact", "exact", true},
	}
	for _, c := range cases {
		if got := glob(c.p, c.s); got != c.want {
			t.Errorf("glob(%q,%q)=%v", c.p, c.s, got)
		}
	}
}
