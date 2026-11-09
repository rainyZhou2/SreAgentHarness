package capability

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

type fakeExec struct {
	name string
	risk domain.RiskClass
}

func (f fakeExec) Descriptor() action.Descriptor {
	return action.Descriptor{Name: f.name, Version: "1", ResourceKinds: []string{"deployment"}, Risk: f.risk,
		IdempotencyMode: action.IdemQueryBeforeRetry, Timeout: time.Minute}
}
func (fakeExec) Preflight(context.Context, action.Plan) (action.PreflightResult, error) {
	return action.PreflightResult{Allowed: true}, nil
}
func (fakeExec) Execute(context.Context, action.Plan, string) (json.RawMessage, error) {
	return nil, nil
}
func (fakeExec) Inspect(context.Context, action.Plan) (action.InspectResult, error) {
	return action.InspectResult{}, nil
}

// testCap registers what its fields say.
type testCap struct {
	Base
	id        string
	tools     []tools.Tool
	actions   []action.Executor
	collect   func(context.Context, time.Time) (Observation, error)
	templates []work.Template
}

func (c *testCap) ID() string      { return c.id }
func (c *testCap) Version() string { return "1" }
func (c *testCap) RegisterTools(r ToolRegistry) error {
	for _, t := range c.tools {
		if err := r.AddTool(t); err != nil {
			return err
		}
	}
	return nil
}
func (c *testCap) RegisterActions(r ActionRegistry) error {
	for _, a := range c.actions {
		if err := r.AddAction(a); err != nil {
			return err
		}
	}
	return nil
}
func (c *testCap) RegisterWorld(r WorldRegistry) error {
	if c.collect == nil {
		return nil
	}
	return r.AddCollector("c", time.Minute, CollectorFunc(c.collect))
}
func (c *testCap) WorkTemplates() []work.Template { return c.templates }

func tool(name string, risk domain.RiskClass) tools.Tool {
	return &tools.Func{Desc: tools.Descriptor{Name: name, Version: "1", Risk: risk, Source: "native"},
		Fn: func(context.Context, string, json.RawMessage) (json.RawMessage, error) { return nil, nil }}
}

func TestRegistrationLimits(t *testing.T) {
	opts := Options{Tenant: "t1"}
	cases := map[string][]Capability{
		"tool with side effects": {&testCap{id: "a", tools: []tools.Tool{tool("x.write", domain.RiskR1)}}},
		"R3 action":              {&testCap{id: "a", actions: []action.Executor{fakeExec{"x.delete", domain.RiskR3}}}},
		"duplicate action": {&testCap{id: "a", actions: []action.Executor{fakeExec{"x.scale", domain.RiskR1}}},
			&testCap{id: "b", actions: []action.Executor{fakeExec{"x.scale", domain.RiskR1}}}},
		"duplicate capability": {&testCap{id: "a"}, &testCap{id: "a"}},
		"invalid template":     {&testCap{id: "a", templates: []work.Template{{ID: "Bad Id"}}}},
	}
	for name, caps := range cases {
		if _, err := NewSet(opts, caps...); !errors.Is(err, ErrInvalid) && !errors.Is(err, work.ErrInvalid) {
			if err == nil || !strings.Contains(err.Error(), "template") {
				t.Errorf("%s: %v", name, err)
			}
		}
	}
	if _, err := NewSet(Options{}, &testCap{id: "a"}); err == nil {
		t.Error("set without tenant")
	}
	// An operator floor raising an action to R3 refuses it too.
	if _, err := NewSet(Options{Tenant: "t1", RiskFloor: map[string]domain.RiskClass{"x.scale": domain.RiskR3}},
		&testCap{id: "a", actions: []action.Executor{fakeExec{"x.scale", domain.RiskR1}}}); err == nil {
		t.Error("R3 floor accepted")
	}
	s, err := NewSet(Options{Tenant: "t1", RiskFloor: map[string]domain.RiskClass{"x.scale": domain.RiskR2, "x.cordon": domain.RiskR0}},
		&testCap{id: "a", actions: []action.Executor{fakeExec{"x.scale", domain.RiskR1}, fakeExec{"x.cordon", domain.RiskR2}}})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]domain.RiskClass{}
	for _, a := range s.Actions() {
		got[a.Descriptor().Name] = a.Descriptor().Risk
	}
	if got["x.scale"] != domain.RiskR2 || got["x.cordon"] != domain.RiskR2 {
		t.Fatalf("floors: %v (raise only)", got)
	}
}

func TestApplyCapsTrustAndRetractsRelations(t *testing.T) {
	round := 0
	c := &testCap{id: "inv", collect: func(context.Context, time.Time) (Observation, error) {
		round++
		o := Observation{Facts: []world.Fact{{SubjectRef: "service://a", Predicate: "owner", Value: "x", TrustTier: world.TrustAuthoritative, Source: "spoofed"}},
			Events: []eventing.Envelope{{ID: "e", Type: "x.y", SubjectRef: "service://a", TenantID: "other"}}}
		o.Relations = []world.Relation{{FromRef: "service://a", Type: "depends_on", ToRef: "service://b"}}
		if round == 1 {
			o.Relations = append(o.Relations, world.Relation{FromRef: "service://a", Type: "depends_on", ToRef: "service://c"})
		}
		return o, nil
	}}
	for _, auth := range []bool{false, true} {
		round = 0
		s, err := NewSet(Options{Tenant: "t1", Authoritative: map[string]bool{"inv": auth}}, c)
		if err != nil {
			t.Fatal(err)
		}
		var facts []world.Fact
		var retracted []string
		var events []eventing.Envelope
		env := plugin.WorkEnv{
			AssertFact:     func(_ context.Context, f world.Fact) error { facts = append(facts, f); return nil },
			AssertRelation: func(context.Context, world.Relation) error { return nil },
			RetractRelation: func(_ context.Context, tenant, from, typ, to, source string, _ time.Time) error {
				retracted = append(retracted, tenant+"|"+to+"|"+source)
				return nil
			},
			Ingest: func(_ context.Context, e eventing.Envelope) error { events = append(events, e); return nil },
		}
		for i := 0; i < 2; i++ {
			if err := s.CollectOnce(context.Background(), env, "inv", "c"); err != nil {
				t.Fatal(err)
			}
		}
		want := world.TrustFirstParty
		if auth {
			want = world.TrustAuthoritative
		}
		if f := facts[0]; f.TrustTier != want || f.Source != "inv" || f.TenantID != "t1" {
			t.Fatalf("auth=%v fact %+v", auth, f)
		}
		if len(retracted) != 1 || retracted[0] != "t1|service://c|inv" {
			t.Fatalf("retracted %v", retracted)
		}
		if events[0].TenantID != "t1" || events[0].Source != "inv" {
			t.Fatalf("event %+v", events[0])
		}
	}
}
