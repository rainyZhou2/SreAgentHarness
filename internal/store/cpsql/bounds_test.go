package cpsql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/work"
)

func boundedWork(t *testing.T, s *Store, tn string, l func(*work.Limits)) work.Work {
	t.Helper()
	n := newWork(tn, cpkit.NewID())
	l(&n.Template.Limits)
	w, _, err := s.CreateWork(context.Background(), n)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// RFC §20.3 max_watches: a Work cannot hold more active watches than its
// template allows.
func TestWatchLimit(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := boundedWork(t, s, tn, func(l *work.Limits) { l.MaxWatches = 1 })
		watch := func() continuous.Watch {
			wt, err := continuous.Build(continuous.Spec{Mode: continuous.OnEither, Timeout: cpkit.Duration(time.Minute),
				Predicate: &eventing.Predicate{EventTypes: []string{"deploy.finished"}}}, tn, w.ID, w.Generation,
				[]string{"service://prod/checkout"}, t0)
			if err != nil {
				t.Fatal(err)
			}
			return wt
		}
		w, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID,
			ExpectedVersion: w.Version, Now: t0, Status: st(work.StatusActive), Watches: []continuous.Watch{watch()}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID,
			ExpectedVersion: w.Version, Now: t0, Watches: []continuous.Watch{watch()}})
		if !errors.Is(err, work.ErrLimit) || work.LimitName(err) != "max_watches" {
			t.Fatalf("second watch: %v", err)
		}
		// Replacing the watch (cancel + add in one change) stays within it.
		if _, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID,
			ExpectedVersion: w.Version, Now: t0, CancelAllWatches: true, Watches: []continuous.Watch{watch()}}); err != nil {
			t.Fatalf("replace: %v", err)
		}
	})
}

// RFC §20.3 max_evidence / max_evidence_bytes.
func TestEvidenceLimits(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		add := func(w work.Work, summary string) error {
			_, _, err := s.AddEvidence(ctx, evidence.Record{TenantID: tn, WorkID: w.ID, SourceType: evidence.SourceHuman,
				SourceRef: "api:op", Summary: summary, TrustTier: "T3"})
			return err
		}
		w := boundedWork(t, s, tn, func(l *work.Limits) { l.MaxEvidence = 2 })
		for i := 0; i < 2; i++ {
			if err := add(w, fmt.Sprintf("note %d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := add(w, "note 0"); err != nil {
			t.Fatalf("a duplicate is returned, not counted: %v", err)
		}
		if err := add(w, "note 2"); work.LimitName(err) != "max_evidence" {
			t.Fatalf("third record: %v", err)
		}

		w = boundedWork(t, s, tn, func(l *work.Limits) { l.MaxEvidenceBytes = 1500 })
		if err := add(w, strings.Repeat("a", 600)); err != nil {
			t.Fatal(err)
		}
		if err := add(w, strings.Repeat("b", 900)); work.LimitName(err) != "max_evidence_bytes" {
			t.Fatalf("over the byte budget: %v", err)
		}
		if list, _ := s.ListEvidence(ctx, tn, w.ID, 10); len(list) != 1 {
			t.Fatalf("evidence kept: %d", len(list))
		}
	})
}

// RFC §20.3 max_event_rate: an alert storm on one group neither floods the
// Work's timeline nor keeps waking it; the rate recovers a minute later.
func TestCorrelateThrottlesStorms(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		n := newWork(tn, cpkit.NewID())
		n.Template.Limits.MaxEventRate = 3
		corr := func(i int, now time.Time) (work.Work, string) {
			t.Helper()
			w, d, err := s.Correlate(ctx, CorrelateRequest{TenantID: tn, GroupKey: "storm", Rule: "r", Window: time.Hour,
				Now: now, Create: &n, EventID: fmt.Sprintf("e%d", i), Payload: map[string]any{"i": i}})
			if err != nil {
				t.Fatal(err)
			}
			return w, d
		}
		w, d := corr(0, t0)
		if d != CorrCreated {
			t.Fatalf("first: %s", d)
		}
		var decisions []string
		for i := 1; i <= 8; i++ {
			_, d := corr(i, t0.Add(time.Duration(i)*time.Second))
			decisions = append(decisions, d)
		}
		want := []string{CorrAttached, CorrAttached, CorrAttached}
		for i, d := range decisions {
			exp := CorrThrottled
			if i < len(want) {
				exp = want[i]
			}
			if d != exp {
				t.Fatalf("decisions: %v", decisions)
			}
		}
		count := func() (attached, throttled int) {
			evs, err := s.ListWorkEvents(ctx, tn, w.ID, 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range evs {
				switch e.Type {
				case "work.event_attached":
					attached++
				case "work.events_throttled":
					throttled++
				}
			}
			return
		}
		if a, th := count(); a != 3 || th != 1 {
			t.Fatalf("timeline: attached=%d throttled=%d", a, th)
		}
		got, _ := s.GetWork(ctx, tn, w.ID)
		if got.WakeAt == nil || got.WakeAt.After(t0.Add(3*time.Second)) {
			t.Fatalf("throttled events must not move the wake: %v", got.WakeAt)
		}
		// A minute later the Work hears events again.
		if _, d := corr(100, t0.Add(2*time.Minute)); d != CorrAttached {
			t.Fatalf("after the window: %s", d)
		}
		if a, th := count(); a != 4 || th != 1 {
			t.Fatalf("timeline after recovery: attached=%d throttled=%d", a, th)
		}
	})
}

func TestTemplateLimitDefaults(t *testing.T) {
	tpl := work.Template{ID: "t", Stages: []work.Stage{{ID: "c", Type: work.StageConfirm}}}
	if err := tpl.Normalize(); err != nil {
		t.Fatal(err)
	}
	l := tpl.Limits
	if l.MaxWatches != work.DefaultMaxWatches || l.MaxEvidence != work.DefaultMaxEvidence ||
		l.MaxEvidenceBytes != work.DefaultMaxEvidenceBytes || l.MaxEventRate != work.DefaultMaxEventRate || l.MaxCostUSD != 0 {
		t.Fatalf("defaults: %+v", l)
	}
	// A Work pinned before these limits existed is still bounded.
	if (work.Limits{}).Watches() != work.DefaultMaxWatches || (work.Limits{}).EventRate() != work.DefaultMaxEventRate {
		t.Fatal("legacy limits are unbounded")
	}
	for _, bad := range []work.Limits{{MaxCostUSD: -1}, {MaxWatches: 65}, {MaxEvidenceBytes: 65 << 20}, {MaxEventRate: 10001}} {
		tpl := work.Template{ID: "t", Stages: []work.Stage{{ID: "c", Type: work.StageConfirm}}, Limits: bad}
		if err := tpl.Normalize(); !errors.Is(err, work.ErrInvalid) {
			t.Fatalf("%+v accepted: %v", bad, err)
		}
	}
}
