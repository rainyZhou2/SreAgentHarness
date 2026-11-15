package orchestration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/store/cpsql"
)

type sourceReconcilerFunc func(context.Context, eventing.SourceReconcileRequest) (eventing.SourceReconcilePage, error)

func (f sourceReconcilerFunc) Reconcile(ctx context.Context, req eventing.SourceReconcileRequest) (eventing.SourceReconcilePage, error) {
	return f(ctx, req)
}

func recoveryEvent(id string) eventing.Envelope {
	return eventing.Envelope{ID: id, Type: "alert.firing", SubjectRef: "service://prod/api", OccurredAt: time.Date(2026, 11, 8, 9, 0, 0, 0, time.UTC)}
}

func newSourceRecovery(h *harness, reconcilers map[string]eventing.SourceReconciler) *SourceRecovery {
	return &SourceRecovery{Store: h.store, Ingestor: h.ing, Reconcilers: reconcilers,
		Targets: []SourceTarget{{TenantID: "t1", Source: "alerts"}}, Owner: "owner-a", Clock: h.clock.Now,
		Lease: time.Minute, Retry: time.Minute, Log: quiet}
}

func TestSourceRecoveryCommitsOnlyAfterDurableIngest(t *testing.T) {
	h := newHarness(t, domain.RiskR1, alertRule())
	var cursors []string
	reconciler := sourceReconcilerFunc(func(_ context.Context, req eventing.SourceReconcileRequest) (eventing.SourceReconcilePage, error) {
		cursors = append(cursors, req.Cursor)
		if req.TenantID != "t1" || req.Source != "alerts" || req.Limit != 25 {
			t.Fatalf("request=%+v", req)
		}
		return eventing.SourceReconcilePage{Events: []eventing.Envelope{recoveryEvent("event-1")}, NextCursor: "cursor-" + req.Cursor + "1"}, nil
	})
	r := newSourceRecovery(h, map[string]eventing.SourceReconciler{"alerts": reconciler})
	r.Batch = 25
	if err := r.Tick(h.ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Tick(h.ctx); err != nil {
		t.Fatal(err)
	}
	if len(cursors) != 2 || cursors[0] != "" || cursors[1] != "cursor-1" {
		t.Fatalf("cursors=%v", cursors)
	}
	events, err := h.store.ListEvents(h.ctx, cpsql.EventFilter{TenantID: "t1"})
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%v err=%v", events, err)
	}
	state, ok, err := h.store.SourceCheckpoint(h.ctx, "t1", "alerts")
	if err != nil || !ok || state.Cursor != "cursor-cursor-11" || state.Owner != "" || state.LastError != "" {
		t.Fatalf("state=%+v ok=%v err=%v", state, ok, err)
	}
}

func TestSourceRecoveryPartialPageDoesNotAdvance(t *testing.T) {
	h := newHarness(t, domain.RiskR1, alertRule())
	fail := true
	reconciler := sourceReconcilerFunc(func(context.Context, eventing.SourceReconcileRequest) (eventing.SourceReconcilePage, error) {
		second := recoveryEvent("event-2")
		if fail {
			second.ID = ""
		}
		return eventing.SourceReconcilePage{Events: []eventing.Envelope{recoveryEvent("event-1"), second}, NextCursor: "cursor-2"}, nil
	})
	r := newSourceRecovery(h, map[string]eventing.SourceReconciler{"alerts": reconciler})
	if err := r.Tick(h.ctx); err == nil {
		t.Fatal("partial page must fail")
	}
	state, ok, err := h.store.SourceCheckpoint(h.ctx, "t1", "alerts")
	if err != nil || !ok || state.Cursor != "" || state.LastError == "" || state.NextRetryAt == nil {
		t.Fatalf("state=%+v ok=%v err=%v", state, ok, err)
	}
	fail = false
	h.clock.Add(2 * time.Minute)
	if err := r.Tick(h.ctx); err != nil {
		t.Fatal(err)
	}
	events, err := h.store.ListEvents(h.ctx, cpsql.EventFilter{TenantID: "t1"})
	if err != nil || len(events) != 2 {
		t.Fatalf("events=%v err=%v", events, err)
	}
	state, _, _ = h.store.SourceCheckpoint(h.ctx, "t1", "alerts")
	if state.Cursor != "cursor-2" || state.LastError != "" {
		t.Fatalf("recovered state=%+v", state)
	}
}

func TestSourceRecoveryEmptyPageRequiresHighWatermark(t *testing.T) {
	h := newHarness(t, domain.RiskR1, alertRule())
	page := eventing.SourceReconcilePage{}
	r := newSourceRecovery(h, map[string]eventing.SourceReconciler{"alerts": sourceReconcilerFunc(func(context.Context, eventing.SourceReconcileRequest) (eventing.SourceReconcilePage, error) {
		return page, nil
	})})
	if err := r.Tick(h.ctx); err != nil {
		t.Fatal(err)
	}
	state, _, _ := h.store.SourceCheckpoint(h.ctx, "t1", "alerts")
	if state.Cursor != "" {
		t.Fatalf("empty page advanced to %q", state.Cursor)
	}
	page = eventing.SourceReconcilePage{NextCursor: "high-water-10", HighWatermark: true}
	if err := r.Tick(h.ctx); err != nil {
		t.Fatal(err)
	}
	state, _, _ = h.store.SourceCheckpoint(h.ctx, "t1", "alerts")
	if state.Cursor != "high-water-10" {
		t.Fatalf("high watermark state=%+v", state)
	}
}

func TestSourceRecoverySkipsUnregisteredAndCorruptSources(t *testing.T) {
	h := newHarness(t, domain.RiskR1, alertRule())
	called := false
	r := newSourceRecovery(h, map[string]eventing.SourceReconciler{"other": sourceReconcilerFunc(func(context.Context, eventing.SourceReconcileRequest) (eventing.SourceReconcilePage, error) {
		called = true
		return eventing.SourceReconcilePage{}, nil
	})})
	if err := r.Tick(h.ctx); err != nil || called {
		t.Fatalf("unregistered called=%v err=%v", called, err)
	}
	if _, ok, err := h.store.SourceCheckpoint(h.ctx, "t1", "alerts"); err != nil || ok {
		t.Fatalf("unregistered checkpoint ok=%v err=%v", ok, err)
	}
	if _, err := h.store.DB().ExecContext(h.ctx, `INSERT INTO source_reconcile_checkpoints
		(tenant_id, source, cursor, fencing_token, updated_at) VALUES ('t1', 'alerts', ?, 0, 1)`, strings.Repeat("x", 4097)); err != nil {
		t.Fatal(err)
	}
	r.Reconcilers["alerts"] = sourceReconcilerFunc(func(context.Context, eventing.SourceReconcileRequest) (eventing.SourceReconcilePage, error) {
		called = true
		return eventing.SourceReconcilePage{}, nil
	})
	if err := r.Tick(h.ctx); !errors.Is(err, eventing.ErrInvalidCheckpoint) {
		t.Fatalf("corrupt cursor err=%v", err)
	}
	if called {
		t.Fatal("reconciler called with a corrupt cursor")
	}
}
