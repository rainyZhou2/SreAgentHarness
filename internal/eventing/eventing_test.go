package eventing

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

func valid() Envelope {
	return Envelope{ID: "alert-001", TenantID: "t1", Source: "alertmanager", Type: "Alert.Firing",
		SubjectRef: "service://prod//search-api", DedupKey: "fp-1", Labels: map[string]string{"severity": "critical"},
		Payload: json.RawMessage(`{"a":1}`)}
}

func TestNormalize(t *testing.T) {
	e := valid()
	if err := e.Normalize(now); err != nil {
		t.Fatal(err)
	}
	if e.Type != "alert.firing" || e.SubjectRef != "service://prod/search-api" || e.Confidence != 1 ||
		!e.ObservedAt.Equal(now) || !e.OccurredAt.Equal(now) || e.PayloadHash == "" || e.SchemaVersion != 1 {
		t.Fatalf("normalized: %+v", e)
	}
	mut := []func(*Envelope){
		func(e *Envelope) { e.ID = "" },
		func(e *Envelope) { e.TenantID = "" },
		func(e *Envelope) { e.Source = "bad source" },
		func(e *Envelope) { e.Type = "Alert Firing" },
		func(e *Envelope) { e.SubjectRef = "search-api" },
		func(e *Envelope) { e.OccurredAt = now.Add(time.Hour) },
		func(e *Envelope) { e.OccurredAt = now.Add(-60 * 24 * time.Hour) },
		func(e *Envelope) { e.Confidence = 1.5 },
		func(e *Envelope) { e.Payload = json.RawMessage(`{not json`) },
		func(e *Envelope) { e.Labels = map[string]string{"bad key": "x"} },
		func(e *Envelope) { e.SchemaVersion = 9 },
		func(e *Envelope) { x := now.Add(-time.Hour); e.OccurredAt = now; e.ExpiresAt = &x },
	}
	for i, m := range mut {
		e := valid()
		m(&e)
		if err := e.Normalize(now); !errors.Is(err, ErrInvalid) {
			t.Errorf("case %d accepted: %v", i, err)
		}
	}
}

func TestPredicate(t *testing.T) {
	e := valid()
	_ = e.Normalize(now)
	p := Predicate{EventTypes: []string{"alert.*"}, SubjectRefs: []string{"service://prod/*"},
		Where: &Cond{All: []Cond{
			{Field: "labels.severity", Op: "in", Values: []string{"critical", "page"}},
			{Not: &Cond{Field: "labels.silenced", Op: "exists"}},
			{Field: "confidence", Op: "gte", Value: "0.5"},
		}}}
	if err := p.Normalize(); err != nil {
		t.Fatal(err)
	}
	if !p.Match(&e) {
		t.Fatal("expected match")
	}
	e.Labels["silenced"] = "1"
	if p.Match(&e) {
		t.Fatal("silenced must not match")
	}
	delete(e.Labels, "silenced")
	e.Type = "change.deployed"
	if p.Match(&e) {
		t.Fatal("type mismatch")
	}
	bad := []Predicate{
		{EventTypes: []string{"Bad Type"}},
		{SubjectRefs: []string{"nope"}},
		{Where: &Cond{Field: "payload.x", Op: "eq"}},
		{Where: &Cond{Field: "type", Op: "regex"}},
		{Where: &Cond{Field: "type", Op: "eq", All: []Cond{{Field: "type", Op: "exists"}}}},
		{Where: &Cond{Field: "confidence", Op: "gt", Value: "x"}},
	}
	for i, b := range bad {
		if err := b.Normalize(); !errors.Is(err, ErrPredicate) {
			t.Errorf("bad predicate %d accepted: %v", i, err)
		}
	}
	// Depth bound.
	deep := &Cond{Field: "type", Op: "exists"}
	for i := 0; i < 20; i++ {
		deep = &Cond{Not: deep}
	}
	if err := (&Predicate{Where: deep}).Normalize(); err == nil {
		t.Fatal("deep predicate accepted")
	}
}

func TestRouterAndGroupKey(t *testing.T) {
	rt, err := NewRouter([]Rule{
		{Name: "resolved", Match: Predicate{EventTypes: []string{"alert.resolved"}}, Action: ActionAttach, Key: "label:service"},
		{Name: "alerts", Match: Predicate{EventTypes: []string{"alert.*", "change.*"}}, Key: "label:service",
			Title: "Incident ${label.service} (${type})", WorkKind: "incident"},
	})
	if err != nil {
		t.Fatal(err)
	}
	a := valid()
	a.Labels["service"] = "search-api"
	_ = a.Normalize(now)
	c := Envelope{ID: "chg-9", TenantID: "t1", Source: "argo", Type: "change.deployed", SubjectRef: "k8s://c/prod/Deployment/search",
		Labels: map[string]string{"service": "search-api"}}
	_ = c.Normalize(now)
	ra, _ := rt.Route(&a)
	rc, _ := rt.Route(&c)
	if ra.Name != "alerts" || rc.Name != "alerts" {
		t.Fatalf("routing %s %s", ra.Name, rc.Name)
	}
	if ra.GroupKey(&a) != rc.GroupKey(&c) {
		t.Fatal("alert and change for one service must correlate") // T05
	}
	if got := ra.RenderTitle(&a); got != "Incident search-api (alert.firing)" {
		t.Fatalf("title %q", got)
	}
	other := a
	other.Labels = map[string]string{"service": "checkout"}
	if ra.GroupKey(&other) == ra.GroupKey(&a) {
		t.Fatal("different services must not correlate")
	}
	r := Envelope{ID: "x", TenantID: "t1", Source: "am", Type: "alert.resolved", SubjectRef: "service://prod/a"}
	_ = r.Normalize(now)
	if rr, _ := rt.Route(&r); rr.Action != ActionAttach {
		t.Fatal("resolved must attach only")
	}
	if _, err := NewRouter([]Rule{{Name: "x", Key: "payload.foo"}}); err == nil {
		t.Fatal("bad key accepted")
	}
}
