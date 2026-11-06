package eventsinks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventsink"
)

func TestWebhookSlackFormatAndErrorClasses(t *testing.T) {
	status := http.StatusOK
	var body map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		if r.Header.Get("X-Harness-Event-Id") != "e1" {
			t.Errorf("missing de-duplication header")
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	w, err := NewWebhook(WebhookConfig{URL: srv.URL, Format: "slack"})
	if err != nil {
		t.Fatal(err)
	}
	e := domain.Event{ID: "e1", TenantID: "t", RunID: "r1", Type: "approval.requested",
		Payload: json.RawMessage(`{"tool":"k8s.rollout_restart","target":"checkout","risk_class":"R2"}`)}
	if err := w.Deliver(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body["text"], "approval.requested") || !strings.Contains(body["text"], "tool=k8s.rollout_restart") {
		t.Fatalf("slack text = %q", body["text"])
	}
	status = http.StatusBadRequest
	if err := w.Deliver(context.Background(), e); !eventsink.IsPermanent(err) {
		t.Fatalf("400 must be permanent, got %v", err)
	}
	status = http.StatusServiceUnavailable
	if err := w.Deliver(context.Background(), e); err == nil || eventsink.IsPermanent(err) {
		t.Fatalf("503 must be retryable, got %v", err)
	}
	if _, err := NewWebhook(WebhookConfig{URL: "ftp://x"}); err == nil {
		t.Fatal("non-http url accepted")
	}
	if !(eventsink.Options{Types: []string{"approval.*"}}).Matches(e) || (eventsink.Options{Types: []string{"run.*"}}).Matches(e) {
		t.Fatal("type filter")
	}
}
