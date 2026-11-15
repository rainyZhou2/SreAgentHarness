package cpsql

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/alerting"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/work"
)

func alertInput(t *testing.T, tenantID, inboxID, fingerprint, eventType string, at time.Time) alerting.Input {
	t.Helper()
	env := eventing.Envelope{ID: inboxID, TenantID: tenantID, Source: "alertmanager", Type: eventType,
		SubjectRef: "service://prod/search", DedupKey: fingerprint, OccurredAt: at, ObservedAt: at,
		Labels: map[string]string{"alertname": "HighLatency", "symptom": "latency", "impact_scope": "paid-search", "severity": "critical"}}
	in, err := alerting.NewInput(env, inboxID, alerting.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	return in
}

func TestAlertProjectionIsIdempotentAndTenantScoped(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		in := alertInput(t, tn, cpkit.NewID(), "fp-1", alerting.EventFiring, t0)
		got, err := s.ProjectAlert(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if got.Episode == nil || got.Episode.State != alerting.StateFiring || got.Symptom.ActiveEpisodes != 1 {
			t.Fatalf("projection: %+v", got)
		}
		again, err := s.ProjectAlert(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if again.Instance.ID != got.Instance.ID || again.Episode.Version != got.Episode.Version || again.Symptom.Version != got.Symptom.Version {
			t.Fatalf("idempotent replay changed projection: %+v %+v", got, again)
		}
		if _, err := s.AlertEpisode(ctx, "other-"+tn, got.Episode.ID); !errors.Is(err, work.ErrNotFound) {
			t.Fatalf("cross-tenant episode read: %v", err)
		}
		stored, err := s.AlertProjection(ctx, tn, in.InboxID)
		if err != nil || stored.Instance.ID != got.Instance.ID || stored.Symptom.Key != got.Symptom.Key {
			t.Fatalf("read projection: %+v %v", stored, err)
		}
	})
}

func TestAlertProjectionConcurrentFiringConverges(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		const n = 8
		results := make([]alerting.Projection, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				in := alertInput(t, tn, cpkit.NewID(), "fp-concurrent", alerting.EventFiring, t0)
				results[i], errs[i] = s.ProjectAlert(ctx, in)
			}(i)
		}
		wg.Wait()
		episodeID := ""
		for i := range results {
			if errs[i] != nil {
				t.Fatalf("projection %d: %v", i, errs[i])
			}
			if episodeID == "" {
				episodeID = results[i].Episode.ID
			}
			if results[i].Episode.ID != episodeID || results[i].Symptom.ActiveEpisodes != 1 {
				t.Fatalf("projection %d did not converge: %+v", i, results[i])
			}
		}
	})
}

func TestAlertProjectionResolveReopenAndNewEpisode(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		firing := alertInput(t, tn, cpkit.NewID(), "fp-reopen", alerting.EventFiring, t0)
		opened, err := s.ProjectAlert(ctx, firing)
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := s.ProjectAlert(ctx, alertInput(t, tn, cpkit.NewID(), "fp-reopen", alerting.EventResolved, t0.Add(5*time.Minute)))
		if err != nil || resolved.Episode.State != alerting.StateResolved || resolved.Symptom.ActiveEpisodes != 0 {
			t.Fatalf("resolve: %+v %v", resolved, err)
		}
		reopened, err := s.ProjectAlert(ctx, alertInput(t, tn, cpkit.NewID(), "fp-reopen", alerting.EventFiring, t0.Add(10*time.Minute)))
		if err != nil || reopened.Episode.ID != opened.Episode.ID || reopened.Episode.ReopenCount != 1 || reopened.Symptom.ActiveEpisodes != 1 {
			t.Fatalf("reopen: %+v %v", reopened, err)
		}
		if _, err := s.ProjectAlert(ctx, alertInput(t, tn, cpkit.NewID(), "fp-reopen", alerting.EventResolved, t0.Add(15*time.Minute))); err != nil {
			t.Fatal(err)
		}
		newEpisode, err := s.ProjectAlert(ctx, alertInput(t, tn, cpkit.NewID(), "fp-reopen", alerting.EventFiring, t0.Add(76*time.Minute)))
		if err != nil || newEpisode.Episode.ID == opened.Episode.ID || newEpisode.Episode.ReopenCount != 0 || newEpisode.Symptom.ActiveEpisodes != 1 {
			t.Fatalf("new episode: %+v %v", newEpisode, err)
		}
	})
}

func TestAlertProjectionAmbiguousResolutionDoesNotCloseEpisode(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		for i := 0; i < 2; i++ {
			if _, err := s.ProjectAlert(ctx, alertInput(t, tn, cpkit.NewID(), fmt.Sprintf("fp-%d", i), alerting.EventFiring, t0)); err != nil {
				t.Fatal(err)
			}
		}
		in := alertInput(t, tn, cpkit.NewID(), "", alerting.EventResolved, t0.Add(time.Minute))
		got, err := s.ProjectAlert(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if got.Instance.Resolution != alerting.ResolutionAmbiguous || got.Episode != nil || got.Symptom.ActiveEpisodes != 2 {
			t.Fatalf("ambiguous resolution: %+v", got)
		}
	})
}

func TestAlertProjectionOutOfOrderCannotRegressState(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		opened, err := s.ProjectAlert(ctx, alertInput(t, tn, cpkit.NewID(), "fp-late", alerting.EventFiring, t0.Add(10*time.Minute)))
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.ProjectAlert(ctx, alertInput(t, tn, cpkit.NewID(), "fp-late", alerting.EventResolved, t0.Add(5*time.Minute)))
		if err != nil {
			t.Fatal(err)
		}
		if got.Instance.Resolution != alerting.ResolutionOutOfOrder || got.Episode.ID != opened.Episode.ID ||
			got.Episode.State != alerting.StateFiring || got.Symptom.ActiveEpisodes != 1 {
			t.Fatalf("out-of-order resolution: %+v", got)
		}
	})
}

func TestAlertProjectionFlappingWindowBoundsReopenHistory(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		newInput := func(eventType string, at time.Time) alerting.Input {
			in := alertInput(t, tn, cpkit.NewID(), "fp-flapping-window", eventType, at)
			in.Policy.ReopenWindow = cpkit.Duration(24 * time.Hour)
			in.Policy.FlappingWindow = cpkit.Duration(time.Hour)
			in.Policy.FlappingThreshold = 2
			return in
		}
		if _, err := s.ProjectAlert(ctx, newInput(alerting.EventFiring, t0)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ProjectAlert(ctx, newInput(alerting.EventResolved, t0.Add(time.Minute))); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ProjectAlert(ctx, newInput(alerting.EventFiring, t0.Add(2*time.Minute))); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ProjectAlert(ctx, newInput(alerting.EventResolved, t0.Add(3*time.Minute))); err != nil {
			t.Fatal(err)
		}
		got, err := s.ProjectAlert(ctx, newInput(alerting.EventFiring, t0.Add(2*time.Hour)))
		if err != nil {
			t.Fatal(err)
		}
		if got.Episode.Flapping {
			t.Fatalf("reopens separated by more than the flapping window were combined: %+v", got.Episode)
		}
	})
}

func TestAlertProjectionRollsBackAllRows(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB().ExecContext(ctx, `CREATE TRIGGER reject_alert_instance BEFORE INSERT ON alert_instances
		BEGIN SELECT RAISE(ABORT, 'injected instance failure'); END`); err != nil {
		t.Fatal(err)
	}
	in := alertInput(t, tenant(), cpkit.NewID(), "fp-rollback", alerting.EventFiring, t0)
	if _, err := s.ProjectAlert(ctx, in); err == nil {
		t.Fatal("injected Instance failure must fail the projection")
	}
	for _, table := range []string{"alert_instances", "alert_episodes", "alert_symptoms"} {
		var n int
		if err := s.DB().QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE tenant_id=?", in.TenantID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("%s contains %d rows after rollback", table, n)
		}
	}
}

func TestAlertProjectionRejectsOpenFingerprintSymptomMove(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		opened, err := s.ProjectAlert(ctx, alertInput(t, tn, cpkit.NewID(), "fp-stable-symptom", alerting.EventFiring, t0))
		if err != nil {
			t.Fatal(err)
		}
		env := eventing.Envelope{ID: "moved", TenantID: tn, Source: "alertmanager", Type: alerting.EventFiring,
			SubjectRef: "service://prod/search", DedupKey: "fp-stable-symptom", OccurredAt: t0.Add(time.Minute), ObservedAt: t0.Add(time.Minute),
			Labels: map[string]string{"alertname": "HighLatency", "symptom": "availability", "impact_scope": "paid-search"}}
		moved, err := alerting.NewInput(env, cpkit.NewID(), alerting.Policy{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ProjectAlert(ctx, moved); !errors.Is(err, alerting.ErrInvalid) {
			t.Fatalf("open fingerprint moved Symptom without an error: %v", err)
		}
		symptom, err := s.AlertSymptom(ctx, tn, opened.Symptom.ID)
		if err != nil || symptom.ActiveEpisodes != 1 {
			t.Fatalf("original Symptom changed after rejected move: %+v %v", symptom, err)
		}
		var instances int
		if err := s.DB().QueryRowContext(ctx, s.q(`SELECT count(*) FROM alert_instances WHERE tenant_id=?`), tn).Scan(&instances); err != nil {
			t.Fatal(err)
		}
		if instances != 1 {
			t.Fatalf("rejected move persisted an Instance; count=%d", instances)
		}
	})
}
