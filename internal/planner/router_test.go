package planner

import (
	"testing"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

func TestRouterOrder(t *testing.T) {
	r := DefaultRouter()
	toolset := []tools.Descriptor{{Name: "k8s.cordon_node", Risk: domain.RiskR1}, {Name: "logs.search", Risk: domain.RiskR0}}
	cases := []struct {
		in        Input
		requested string
		want      string
	}{
		{Input{Goal: "why is checkout slow", ModeHint: "flow"}, "", "flow"},
		{Input{Goal: "restart checkout"}, "react", "react"},
		{Input{Goal: "Why did checkout restart?"}, "", "react"},
		{Input{Goal: "为什么支付超时"}, "", "react"},
		{Input{Goal: "cordon node ip-10-0-0-1", Tools: toolset}, "", "flow"},
		{Input{Goal: "search logs for checkout", Tools: toolset}, "", "react"},
		{Input{Goal: "rollback checkout to v41"}, "", "flow"},
		{Input{Goal: "回滚支付服务"}, "", "flow"},
		{Input{Goal: "checkout latency"}, "", "react"},
	}
	for _, c := range cases {
		got, why := r.Route(c.in, c.requested)
		if got != c.want {
			t.Errorf("Route(%q, %q) = %s (%s), want %s", c.in.Goal, c.requested, got, why, c.want)
		}
	}
}

func TestParsePatterns(t *testing.T) {
	if got := ParsePatterns("", DefaultFlowPatterns); len(got) != len(DefaultFlowPatterns) {
		t.Fatal("empty means defaults")
	}
	if got := ParsePatterns("none", DefaultFlowPatterns); got != nil {
		t.Fatal("none disables")
	}
	got := ParsePatterns(` \bfix\b || ^drain `, nil)
	if len(got) != 2 || got[0] != `\bfix\b` {
		t.Fatalf("got %q", got)
	}
	r, err := NewRouter(nil, got)
	if err != nil {
		t.Fatal(err)
	}
	if m, _ := r.Route(Input{Goal: "please FIX the queue"}, ""); m != "flow" {
		t.Fatal("custom pattern did not route")
	}
	if _, err := NewRouter([]string{"("}, nil); err == nil {
		t.Fatal("invalid pattern accepted")
	}
	if r, _ := NewRouter([]string{"none"}, []string{"none"}); len(r.React)+len(r.Flow) != 0 {
		t.Fatal("none entry must disable")
	}
}
