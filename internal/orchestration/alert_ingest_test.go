package orchestration

import (
	"testing"

	"go-sre-harness/internal/alerting"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/work"
)

func symptomAlertRule(action string) eventing.Rule {
	return eventing.Rule{Name: "alert-symptoms-" + action,
		Match: eventing.Predicate{EventTypes: []string{"alert.firing", "alert.resolved"}}, Key: eventing.KeySymptom,
		Action: action, WorkKind: "incident", Template: "mitigate", Intent: "restore"}
}

func ingestAlert(t *testing.T, h *harness, fingerprint, eventType, subject, symptom string, extra map[string]string) eventing.Record {
	t.Helper()
	labels := map[string]string{"alertname": "HighLatency", "symptom": symptom, "impact_scope": "paid-search", "severity": "critical"}
	for k, v := range extra {
		labels[k] = v
	}
	rec, _, err := h.ing.Ingest(h.ctx, eventing.Envelope{ID: cpkit.NewID(), TenantID: "t1", Source: "alertmanager",
		Type: eventType, SubjectRef: subject, DedupKey: fingerprint, OccurredAt: h.clock.Now(), Labels: labels})
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestAlertLayeringGroupsBySymptomWithoutOverMerge(t *testing.T) {
	h := newHarness(t, domain.RiskR1, symptomAlertRule(eventing.ActionCreate))
	const subject = "service://prod/search"
	first := ingestAlert(t, h, "fp-1", alerting.EventFiring, subject, "latency", map[string]string{"silenced": "true"})
	if first.WorkID == "" || first.Decision != "created" {
		t.Fatalf("first alert: %+v", first)
	}
	second := ingestAlert(t, h, "fp-2", alerting.EventFiring, subject, "latency", nil)
	if second.WorkID != first.WorkID || second.Decision != "attached" {
		t.Fatalf("same Symptom must share an Incident: first=%+v second=%+v", first, second)
	}
	otherKind := ingestAlert(t, h, "fp-3", alerting.EventFiring, subject, "availability", nil)
	if otherKind.WorkID == "" || otherKind.WorkID == first.WorkID {
		t.Fatalf("different Symptom kinds over-merged: %+v", otherKind)
	}
	otherSubject := ingestAlert(t, h, "fp-4", alerting.EventFiring, "service://prod/other", "latency", nil)
	if otherSubject.WorkID == "" || otherSubject.WorkID == first.WorkID {
		t.Fatalf("different subjects over-merged: %+v", otherSubject)
	}

	projection, err := h.store.AlertProjection(h.ctx, "t1", first.InboxID)
	if err != nil {
		t.Fatal(err)
	}
	if !projection.Instance.NotificationSuppressed || projection.Episode == nil || projection.Symptom.ActiveEpisodes != 2 {
		t.Fatalf("alert projection: %+v", projection)
	}
	decision, ok, err := h.store.GetDecision(h.ctx, "t1", first.InboxID)
	if err != nil || !ok || decision.Alert == nil || decision.Alert.SymptomID != projection.Symptom.ID ||
		!decision.Alert.NotificationSuppressed {
		t.Fatalf("routing decision alert reference: %+v %v", decision, err)
	}
}

func TestAlertLayeringResolvedAttachDoesNotCreateIncident(t *testing.T) {
	h := newHarness(t, domain.RiskR1, symptomAlertRule(eventing.ActionAttach))
	rec := ingestAlert(t, h, "fp-resolved", alerting.EventResolved, "service://prod/search", "latency", nil)
	if rec.WorkID != "" || rec.Decision != "no_open_work" {
		t.Fatalf("resolved attach created a Work: %+v", rec)
	}
	projection, err := h.store.AlertProjection(h.ctx, "t1", rec.InboxID)
	if err != nil || projection.Instance.Resolution != alerting.ResolutionAmbiguous {
		t.Fatalf("resolved fact was not preserved: %+v %v", projection, err)
	}
}

func TestAlertLayeringRetriesCorrelationWithoutReprojecting(t *testing.T) {
	rule := symptomAlertRule(eventing.ActionCreate)
	rule.Template = "missing-template"
	h := newHarness(t, domain.RiskR1, rule)
	rec := ingestAlert(t, h, "fp-retry", alerting.EventFiring, "service://prod/search", "latency", nil)
	if rec.State != eventing.StateFailed {
		t.Fatalf("correlation failure must not return success: %+v", rec)
	}
	before, err := h.store.AlertProjection(h.ctx, "t1", rec.InboxID)
	if err != nil {
		t.Fatal(err)
	}
	cat, err := NewCatalog([]work.Template{mitigateTemplate()})
	if err != nil {
		t.Fatal(err)
	}
	h.ing.Catalog = cat
	h.ing.Router, err = eventing.NewRouter([]eventing.Rule{symptomAlertRule(eventing.ActionCreate)})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.ing.Process(h.ctx, rec); err != nil {
		t.Fatal(err)
	}
	stored, err := h.store.GetEvent(h.ctx, "t1", rec.InboxID)
	if err != nil || stored.State != eventing.StateProcessed || stored.WorkID == "" {
		t.Fatalf("retry result: %+v %v", stored, err)
	}
	after, err := h.store.AlertProjection(h.ctx, "t1", rec.InboxID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Instance.ID != before.Instance.ID || after.Episode.ID != before.Episode.ID ||
		after.Episode.Version != before.Episode.Version || after.Symptom.Version != before.Symptom.Version || after.Symptom.ActiveEpisodes != 1 {
		t.Fatalf("retry repeated alert transition: before=%+v after=%+v", before, after)
	}
}
