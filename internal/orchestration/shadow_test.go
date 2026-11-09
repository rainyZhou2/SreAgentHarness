package orchestration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/trigger"
	"go-sre-harness/internal/work"
)

// RFC P0-3: v1 triggers keep running; their events are mirrored into the
// inbox in shadow mode, and the report shows where v2 routing would differ.
func TestShadowMirrorAndReport(t *testing.T) {
	ctx := context.Background()
	s, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	clock := &fakeClock{t: time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)}
	s.Clock = clock.Now
	router, err := eventing.NewRouter([]eventing.Rule{{Name: "alerts", Match: eventing.Predicate{EventTypes: []string{"trigger.alerts"}},
		Key: "dedup_key", Window: cpkit.Duration(30 * time.Minute)}})
	if err != nil {
		t.Fatal(err)
	}
	cat, _ := NewCatalog(nil)
	in := &Ingestor{Store: s, Router: router, Catalog: cat, Shadow: true, Clock: clock.Now, Log: quiet}
	m := TriggerMirror{Ingestor: in}
	send := func(fp, v1 string, item map[string]any) {
		t.Helper()
		if err := m.MirrorTriggerEvent(ctx, trigger.MirrorEvent{Trigger: "alerts", TenantID: "t1", Fingerprint: fp,
			Goal: "Investigate", Services: []string{"checkout"}, Labels: map[string]string{"severity": "critical"},
			Item: item, ReceivedAt: clock.Now(), V1Action: v1, V1RunID: map[bool]string{true: cpkit.NewID()}[v1 == trigger.ActionCreated]}); err != nil {
			t.Fatal(err)
		}
	}
	send("fpA", trigger.ActionCreated, map[string]any{"n": 1}) // agree: create
	send("fpA", trigger.ActionCreated, map[string]any{"n": 1}) // webhook redelivery: one inbox record
	clock.Add(5 * time.Minute)
	send("fpA", trigger.ActionDuplicate, map[string]any{"n": 2}) // agree: attach
	send("fpB", trigger.ActionCreated, map[string]any{"n": 3})   // agree: create
	clock.Add(10 * time.Minute)
	// v1's dedup window expired (shorter) but v2's 30m window still holds:
	// v1 starts a second run, v2 would attach to the open Work.
	send("fpA", trigger.ActionCreated, map[string]any{"n": 4})

	if ws, _ := s.ListWorks(ctx, work.Filter{TenantID: "t1"}); len(ws) != 0 {
		t.Fatalf("shadow mode must not create Works: %d", len(ws))
	}
	recs, _ := s.ListEvents(ctx, cpsql.EventFilter{TenantID: "t1", SourcePrefix: TriggerSourcePrefix})
	if len(recs) != 4 {
		t.Fatalf("inbox records: %d", len(recs))
	}
	if r := recs[0]; r.Envelope.SubjectRef != "service://checkout" || r.Envelope.Labels["trigger"] != "alerts" ||
		r.Envelope.Source != "trigger:alerts" {
		t.Fatalf("envelope: %+v", r.Envelope)
	}
	rep, err := in.ShadowReport(ctx, "t1", clock.Now().Add(-time.Hour), clock.Now(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Events != 4 || rep.Agreements != 3 || len(rep.Disagreements) != 1 {
		t.Fatalf("report: %+v", rep)
	}
	d := rep.Disagreements[0]
	if d.V1 != trigger.ActionCreated || d.V2Would != "attach" || d.Rule != "alerts" {
		t.Fatalf("disagreement: %+v", d)
	}
	if rep.Matrix["created→create"] != 2 || rep.Matrix["deduplicated→attach"] != 1 || rep.ByTrigger["alerts"] != [2]int{4, 3} {
		t.Fatalf("matrix: %+v by=%v", rep.Matrix, rep.ByTrigger)
	}
	// Other sources are not part of the report.
	if _, _, err := in.Ingest(ctx, eventing.Envelope{ID: "x1", Source: "alertmanager", Type: "alert.firing", TenantID: "t1",
		SubjectRef: "service://checkout", OccurredAt: clock.Now()}); err != nil {
		t.Fatal(err)
	}
	if rep, _ = in.ShadowReport(ctx, "t1", clock.Now().Add(-time.Hour), clock.Now(), 100); rep.Events != 4 {
		t.Fatalf("report includes non-trigger events: %d", rep.Events)
	}
}
