package eventlog

// Package eventlog defines the commit-ordered run event log shared by the
// event sinks, the event relay and the SSE gateway.

import (
	"context"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
)

// Cursor is a position in the global event log. Only events of transactions
// older than every running transaction are read, so a cursor never skips a
// late commit.
type Cursor struct {
	TxID      int64 `json:"tx_id"`
	GlobalSeq int64 `json:"global_seq"`
}

// Delivery is an event with its log position.
type Delivery struct {
	Cursor Cursor
	Event  domain.Event
}

// Store is the cursor and log access used by event consumers.
type Store interface {
	// ClaimEventSink creates the cursor if missing and takes or renews the
	// lease; ok is false while another owner holds it.
	ClaimEventSink(ctx context.Context, sinkID, owner string, ttl time.Duration, earliest bool) (c Cursor, ok bool, err error)
	ReadEventLog(ctx context.Context, after Cursor, limit int) ([]Delivery, error)
	// AdvanceEventSink moves the cursor and renews the lease; it fails with
	// domain.ErrLeaseLost when the lease was taken over.
	AdvanceEventSink(ctx context.Context, sinkID, owner string, c Cursor, delivered, skipped int, ttl time.Duration) error
}

// Subject returns the bus subject for one run.
func Subject(tenantID, runID string) string {
	return "harness.events." + token(tenantID) + "." + token(runID)
}

// Prefix returns the bus subject prefix for one tenant.
func Prefix(tenantID string) string {
	return "harness.events." + token(tenantID) + ".>"
}

func token(s string) string {
	return strings.NewReplacer(".", "_", "*", "_", ">", "_", " ", "_", "/", "_").Replace(s)
}
