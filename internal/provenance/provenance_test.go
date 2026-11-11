package provenance

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/world"
)

var t0 = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

type memHealth struct {
	hs  []CollectorHealth
	err error
}

func (m memHealth) RecordCollection(context.Context, Report) error { return nil }
func (m memHealth) CollectorHealth(context.Context, string) ([]CollectorHealth, error) {
	return m.hs, m.err
}

func fact(pred, val, source string, at time.Time) world.Fact {
	return world.Fact{ID: pred + "@" + source, SubjectRef: "service://prod/search", Predicate: pred, Value: val, Source: source, ObservedAt: at}
}

func TestAuthoritySettlesOrConflicts(t *testing.T) {
	a := DefaultAuthority()
	d := a.Resolve("owner", []world.Fact{fact("owner", "team-search", "cmdb", t0), fact("owner", "team-infra", "kubernetes", t0.Add(time.Hour))})
	if d.Conflict || d.Value != "team-search" || d.Source != "cmdb" {
		t.Fatalf("the CMDB owns ownership, however recent the other source: %+v", d)
	}
	d = a.Resolve("owner", []world.Fact{fact("owner", "team-a", "wiki", t0), fact("owner", "team-b", "spreadsheet", t0)})
	if !d.Conflict || d.Value != "" {
		t.Fatalf("no authority among the sources: must be a conflict, not a pick: %+v", d)
	}
	d = a.Resolve("image", []world.Fact{fact("image", "v2", "gitops", t0), fact("image", "v2", "kubernetes:prod-east", t0)})
	if d.Conflict || d.Value != "v2" {
		t.Fatalf("agreeing sources: %+v", d)
	}
}

// F10 and §10.4: a conflict without authority, and a collector that stopped
// reporting, both refuse an action; a healthy source passes.
func TestQualityProblems(t *testing.T) {
	ok, old := t0.Add(-time.Minute), t0.Add(-time.Hour)
	snap := &world.Snapshot{Facts: []world.Fact{
		fact("generation", "7", "kubernetes", t0), fact("owner", "a", "wiki", t0), fact("owner", "b", "sheet", t0),
		fact("revision", "r42", "gitops", t0), fact("tier", "1", "cmdb", t0)}}
	q := Quality{Clock: func() time.Time { return t0 }, Health: memHealth{hs: []CollectorHealth{
		{Collector: "kubernetes/topology", LastSuccessAt: &ok, MaxAge: 5 * time.Minute},
		{Collector: "gitops/commits", LastSuccessAt: &old, MaxAge: 5 * time.Minute},
		{Collector: "cmdb/services", LastSuccessAt: &ok, MaxAge: 5 * time.Minute, ErrorCount: 4}}}}
	probs := strings.Join(q.Problems(context.Background(), "t", "service://prod/search",
		[]string{"generation", "owner", "revision", "tier"}, snap), "\n")
	for _, want := range []string{"conflict: sources disagree on owner", "collector stale: the gitops collector behind required fact revision",
		"collector failing: the cmdb collector behind required fact tier"} {
		if !strings.Contains(probs, want) {
			t.Errorf("missing %q in:\n%s", want, probs)
		}
	}
	if strings.Contains(probs, "generation") {
		t.Errorf("a healthy, agreed fact must pass:\n%s", probs)
	}
	q.Health = memHealth{err: errors.New("db down")}
	if p := q.Problems(context.Background(), "t", "service://prod/search", []string{"generation"}, snap); len(p) != 1 {
		t.Fatalf("unreadable collector health must fail closed: %v", p)
	}
}
