package protocol

import (
	"testing"
	"time"

	"go-sre-harness/internal/work"
)

func TestFromWorkPreservesEventMetadata(t *testing.T) {
	occurred := time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC)
	received := occurred.Add(250 * time.Millisecond)
	got := FromWork(work.Event{ID: "event-1", TenantID: "tenant-1", WorkID: "work-1", Seq: 7, Type: "work.event_attached",
		CausationID: "inbox-1", CorrelationID: "trace-1", CorrelationKey: "group-1", OccurredAt: occurred, ReceivedAt: received})
	if got.CausationID != "inbox-1" || got.ParentEventID != "inbox-1" || got.CorrelationID != "trace-1" ||
		got.CorrelationKey != "group-1" || !got.ReceivedAt.Equal(received) {
		t.Fatalf("event metadata was not preserved: %+v", got)
	}
}
