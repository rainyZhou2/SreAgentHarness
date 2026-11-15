package alerting

import (
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
)

func TestPolicyNormalize(t *testing.T) {
	p := Policy{}
	if err := p.Normalize(); err != nil {
		t.Fatal(err)
	}
	if p.ReopenWindow.D() != time.Hour || p.FlappingWindow.D() != 24*time.Hour || p.FlappingThreshold != 3 {
		t.Fatalf("unexpected defaults: %+v", p)
	}
	p = Policy{FlappingThreshold: 1}
	if err := p.Normalize(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("threshold 1: %v", err)
	}
	p = Policy{ReopenWindow: cpkit.Duration(-time.Second)}
	if err := p.Normalize(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative reopen window: %v", err)
	}
}

func TestNewInputBuildsStableAlertAndSymptomIdentity(t *testing.T) {
	at := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	env := eventing.Envelope{ID: "a1", TenantID: "acme", Source: "alertmanager", Type: "alert.firing",
		SubjectRef: "service://prod/search", OccurredAt: at, ObservedAt: at,
		Labels: map[string]string{"alertname": "HighLatency", "symptom": "latency", "impact_scope": "paid-search",
			"severity": "critical", "instance": "search-7", "silenced": "true", "inhibited": "true"}}
	in, err := NewInput(env, "inbox-1", Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if in.Fingerprint == "" || in.SymptomKey == "" || in.AlertName != "HighLatency" || in.SymptomKind != "latency" {
		t.Fatalf("identity: %+v", in)
	}
	if !in.NotificationSuppressed || len(in.SuppressionReasons) != 2 || in.Severity != "critical" {
		t.Fatalf("notification metadata: %+v", in)
	}

	env.Labels["severity"] = "warning"
	env.Labels["silenced"] = "false"
	env.Labels["inhibited"] = "false"
	again, err := NewInput(env, "inbox-2", Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if again.Fingerprint != in.Fingerprint || again.SymptomKey != in.SymptomKey {
		t.Fatalf("notification metadata changed identity: %+v %+v", in, again)
	}
	if again.NotificationSuppressed {
		t.Fatal("suppression flags must not be sticky across notifications")
	}
}

func TestNewInputPrefersSourceFingerprint(t *testing.T) {
	at := time.Now().UTC()
	env := eventing.Envelope{TenantID: "acme", Type: "alert.firing", SubjectRef: "service://prod/search",
		DedupKey: "fp-1", OccurredAt: at, Labels: map[string]string{"alertname": "HighLatency"}}
	in, err := NewInput(env, "inbox-1", Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if in.Fingerprint != "fp-1" {
		t.Fatalf("fingerprint = %q", in.Fingerprint)
	}
	env.DedupKey = ""
	env.Labels["fingerprint"] = "fp-2"
	in, err = NewInput(env, "inbox-1", Policy{})
	if err != nil || in.Fingerprint != "fp-2" {
		t.Fatalf("label fingerprint: %+v %v", in, err)
	}
}

func TestResolvedWithoutFingerprintUsesCandidateMatching(t *testing.T) {
	at := time.Now().UTC()
	env := eventing.Envelope{TenantID: "acme", Type: "alert.resolved", SubjectRef: "service://prod/search",
		OccurredAt: at, Labels: map[string]string{"alertname": "HighLatency"}}
	in, err := NewInput(env, "inbox-1", Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if in.Fingerprint != "" {
		t.Fatalf("resolved without a source fingerprint must not invent one: %q", in.Fingerprint)
	}
}

func TestNewInputRejectsInvalidAlert(t *testing.T) {
	at := time.Now().UTC()
	for name, env := range map[string]eventing.Envelope{
		"type":     {TenantID: "acme", Type: "change.deployed", SubjectRef: "service://prod/search", OccurredAt: at},
		"identity": {TenantID: "acme", Type: "alert.firing", OccurredAt: at},
		"tenant":   {Type: "alert.firing", SubjectRef: "service://prod/search", OccurredAt: at, Labels: map[string]string{"alertname": "HighLatency"}},
		"flag":     {TenantID: "acme", Type: "alert.firing", SubjectRef: "service://prod/search", OccurredAt: at, Labels: map[string]string{"alertname": "HighLatency", "silenced": "perhaps"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewInput(env, "inbox-1", Policy{}); !errors.Is(err, ErrInvalid) {
				t.Fatalf("got %v", err)
			}
		})
	}
	if SupportedEvent(eventing.Envelope{Type: "change.deployed"}) {
		t.Fatal("non-alert event must not enter alert projection")
	}
}
