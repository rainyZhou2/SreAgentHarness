package cpsql

import (
	"context"
	"testing"
	"time"

	"go-sre-harness/internal/work"
)

func TestWorkEventMetadataRoundTrip(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		n := newWork(tn, "metadata")
		n.SourceEvent = "inbox-1"
		n.CorrelationID = "trace-1"
		n.CorrelationKey = "group-1"
		created, ok, err := s.CreateWork(ctx, n)
		if err != nil || !ok {
			t.Fatalf("create: %v created=%v", err, ok)
		}
		events, err := s.ListWorkEvents(ctx, tn, created.ID, 0, 10)
		if err != nil || len(events) != 1 {
			t.Fatalf("events=%+v err=%v", events, err)
		}
		got := events[0]
		if got.CausationID != "inbox-1" || got.CorrelationID != "trace-1" || got.CorrelationKey != "group-1" {
			t.Fatalf("event metadata=%+v", got)
		}
		if got.ReceivedAt.IsZero() || got.ReceivedAt.Before(got.OccurredAt) || got.ReceivedAt.After(got.OccurredAt.Add(time.Second)) {
			t.Fatalf("occurred_at=%s received_at=%s", got.OccurredAt, got.ReceivedAt)
		}
		if _, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: created.ID, ExpectedVersion: created.Version,
			ActorType: "system", ActorID: "metadata", Now: got.OccurredAt.Add(time.Second), Status: st(work.StatusActive)}); err != nil {
			t.Fatal(err)
		}
		events, err = s.ListWorkEvents(ctx, tn, created.ID, 1, 10)
		if err != nil || len(events) != 1 || events[0].ReceivedAt.IsZero() {
			t.Fatalf("appended event=%+v err=%v", events, err)
		}
	})
}
