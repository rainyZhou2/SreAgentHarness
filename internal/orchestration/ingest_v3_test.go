package orchestration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/work"
)

func alertRule() eventing.Rule {
	return eventing.Rule{Name: "alerts", Match: eventing.Predicate{EventTypes: []string{"alert.firing"}}, Key: "subject",
		WorkKind: "incident", Template: "mitigate", Intent: "restore"}
}

func (h *harness) event(typ, subject string, at time.Time, labels map[string]string) eventing.Record {
	h.t.Helper()
	rec, _, err := h.ing.Ingest(h.ctx, eventing.Envelope{ID: cpkit.NewID(), TenantID: "t1", Source: "alertmanager", Type: typ,
		SubjectRef: subject, OccurredAt: at, Labels: labels})
	if err != nil {
		h.t.Fatal(err)
	}
	return rec
}

// §9.3/§9.5: every routing leaves a decision (rule, version, basis); a late
// event attaches but never opens a Work; a source cannot send types the
// operator did not give it; a deploy lands in the change ledger.
func TestIngestDecisionsLateEventsAndChanges(t *testing.T) {
	h := newHarness(t, domain.RiskR1, alertRule(), eventing.Rule{Name: "deploys", Group: "alerts",
		Match: eventing.Predicate{EventTypes: []string{"change.deployed"}}, Key: "subject", Action: "attach"})
	h.ing.Sources = map[string]eventing.SourcePolicy{"alertmanager": {EventTypes: []string{"alert.*", "change.*"},
		MaxLateness: cpkit.Duration(time.Hour)}}
	h.ing.Changes = h.store
	now := h.clock.Now()
	const svc = "service://prod/search-api"

	late := h.event("alert.firing", svc, now.Add(-3*time.Hour), nil)
	if late.WorkID != "" {
		t.Fatalf("a late event must not open a Work: %+v", late)
	}
	d, ok, err := h.store.GetDecision(h.ctx, "t1", late.InboxID)
	if err != nil || !ok || !d.Late || d.RuleName != "alerts" || d.RuleVersion == "" {
		t.Fatalf("late decision: %+v %v", d, err)
	}

	denied := h.event("ticket.created", svc, now, nil)
	if denied.Decision != "type_not_allowed" || denied.WorkID != "" {
		t.Fatalf("type outside the source policy: %+v", denied)
	}

	deploy := h.event("change.deployed", svc, now.Add(-5*time.Minute), map[string]string{"revision": "rev-42", "actor": "ci"})
	_ = deploy
	changes, err := h.store.ChangesFor(h.ctx, "t1", []string{svc}, now.Add(-time.Hour), now, 10)
	if err != nil || len(changes) != 1 || changes[0].Category != "release" || changes[0].AfterRef != "rev-42" || changes[0].Actor != "ci" {
		t.Fatalf("change ledger: %+v %v", changes, err)
	}

	fresh := h.event("alert.firing", svc, now, nil)
	if fresh.WorkID == "" || fresh.Decision != "created" {
		t.Fatalf("fresh alert: %+v", fresh)
	}
	d, _, _ = h.store.GetDecision(h.ctx, "t1", fresh.InboxID)
	if d.Basis != "deterministic" || d.WorkID != fresh.WorkID || len(d.Candidates) != 1 || !d.Candidates[0].Chosen || d.GroupKey == "" {
		t.Fatalf("decision: %+v", d)
	}
	// The late event of the same group now attaches to the open Work.
	late2 := h.event("alert.firing", svc, now.Add(-2*time.Hour), nil)
	if late2.WorkID != fresh.WorkID || late2.Decision != "attached" {
		t.Fatalf("a late event joins the open Work: %+v", late2)
	}
}

// §9.5: an event on a dependency of an open incident is noted as a lead on
// that Work. It does not wake or change the Work.
func TestTopologyAssistedCorrelationIsOnlyALead(t *testing.T) {
	h := newHarness(t, domain.RiskR1, alertRule())
	const svc, db = "service://prod/search-api", "service://prod/search-db"
	h.ing.Neighbors = func(_ context.Context, _, subject string) ([]string, error) {
		if subject == db {
			return []string{svc}, nil
		}
		return nil, nil
	}
	inc := h.event("alert.firing", svc, h.clock.Now(), nil)
	h.tick(2)
	before, _ := h.store.GetWork(h.ctx, "t1", inc.WorkID)
	rel := h.event("db.failover", db, h.clock.Now(), nil)
	if rel.Decision != "related" || rel.WorkID != inc.WorkID {
		t.Fatalf("related event: %+v", rel)
	}
	d, _, _ := h.store.GetDecision(h.ctx, "t1", rel.InboxID)
	if d.Basis != "topology" || len(d.Candidates) != 1 || d.Candidates[0].Relation != "dependency" {
		t.Fatalf("decision: %+v", d)
	}
	after, _ := h.store.GetWork(h.ctx, "t1", inc.WorkID)
	if after.Status != before.Status || after.Stage != before.Stage || !sameTime(after.WakeAt, before.WakeAt) {
		t.Fatalf("a topology lead must not wake or move the Work")
	}
	found := false
	for _, e := range h.events(inc.WorkID) {
		found = found || e == "work.related_event"
	}
	if !found {
		t.Fatal("the lead must be visible on the Work timeline")
	}
	_ = work.StatusActive
}

func TestUpcastV2AndSourceSignature(t *testing.T) {
	raw := []byte(`{"schema_version":2,"event_id":"am-1","source_id":"alertmanager-prod","type":"alert.firing",
		"subject_ref":"service://prod/search-api","occurred_at":"2026-11-10T13:00:00Z","observed_at":"2026-11-10T13:00:03Z",
		"correlation_key":"search-api:latency","labels":{"severity":"critical"}}`)
	e, err := eventing.Upcast(raw)
	if err != nil || e.ID != "am-1" || e.Source != "alertmanager-prod" || e.DedupKey != "search-api:latency" || e.TenantID != "" {
		t.Fatalf("upcast: %+v %v", e, err)
	}
	if _, err := eventing.Upcast([]byte(`{"schema_version":2,"event_id":"x","tenant_id":"evil"}`)); err == nil {
		t.Fatal("a v2 body cannot name a tenant")
	}
	if _, err := eventing.Upcast([]byte(`{"schema_version":3}`)); err == nil {
		t.Fatal("unknown schema version accepted")
	}
	t.Setenv("AM_KEY", "s3cret")
	p := eventing.SourcePolicy{SecretEnv: "AM_KEY", Tenants: []string{"acme"}}
	good := http.Header{}
	good.Set(eventing.SignatureHeader, eventing.Sign([]byte("s3cret"), raw))
	if err := p.Verify("acme", good, raw); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		tenant string
		h      http.Header
		body   []byte
	}{"unsigned": {"acme", http.Header{}, raw}, "tampered": {"acme", good, append([]byte(" "), raw...)}, "other tenant": {"other", good, raw}} {
		if err := p.Verify(c.tenant, c.h, c.body); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	t.Setenv("AM_KEY", "")
	if err := p.Verify("acme", good, raw); err == nil {
		t.Fatal("a missing signing key must not disable the check")
	}
}
