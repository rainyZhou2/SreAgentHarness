// Package eventing is the unified event entry of the control plane (RFC §6):
// every alert, change, schedule tick or telemetry signal becomes one
// validated Envelope before it can touch a Work. Envelopes never carry
// authorization: tenant and source are decided by the server from the
// authenticated caller or the operator-owned adapter, never from the payload.
package eventing

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
)

// SchemaVersion is the current envelope schema.
const SchemaVersion = 1

// Envelope is a normalized event.
type Envelope struct {
	// ID is the source's own id for the event; (tenant, source, id) is
	// unique, so redeliveries are deduplicated. It is not a correlation key.
	ID            string `json:"event_id"`
	SchemaVersion int    `json:"schema_version"`
	TenantID      string `json:"tenant_id"`
	Source        string `json:"source"`
	Type          string `json:"type"`
	SubjectRef    string `json:"subject_ref"`
	CorrelationID string `json:"correlation_id,omitempty"`
	// DedupKey groups events about "the same problem" (e.g. an alert
	// fingerprint); independent state changes keep distinct IDs.
	DedupKey   string            `json:"dedup_key,omitempty"`
	OccurredAt time.Time         `json:"occurred_at"`
	ObservedAt time.Time         `json:"observed_at"`
	ExpiresAt  *time.Time        `json:"expires_at,omitempty"`
	Confidence float64           `json:"confidence"`
	Labels     map[string]string `json:"labels,omitempty"`
	// Payload is the raw event body (untrusted data). Large payloads are
	// offloaded by the inbox and replaced by PayloadRef.
	Payload     json.RawMessage `json:"payload,omitempty"`
	PayloadRef  string          `json:"payload_ref,omitempty"`
	PayloadHash string          `json:"payload_hash,omitempty"`
	TraceID     string          `json:"trace_id,omitempty"`
}

// Limits.
const (
	MaxIDLen        = 200
	MaxLabels       = 32
	MaxLabelKey     = 64
	MaxLabelValue   = 256
	MaxPayloadBytes = 1 << 20
	MaxClockSkew    = 10 * time.Minute
	MaxEventAge     = 30 * 24 * time.Hour
)

// ErrInvalid wraps envelope validation failures.
var ErrInvalid = errors.New("invalid event")

var (
	typeRe   = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z0-9_]+){0,7}$`)
	sourceRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,127}$`)
	labelRe  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.-]{0,63}$`)
)

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
}

// Normalize validates the envelope and fills defaults in place. now is the
// server receive time: ObservedAt defaults to it, and timestamps far in the
// future are rejected so a source cannot reorder history.
func (e *Envelope) Normalize(now time.Time) error {
	now = now.UTC()
	e.ID = strings.TrimSpace(e.ID)
	if e.ID == "" || len(e.ID) > MaxIDLen {
		return invalid("event_id is required (max %d chars)", MaxIDLen)
	}
	if e.SchemaVersion == 0 {
		e.SchemaVersion = SchemaVersion
	}
	if e.SchemaVersion != SchemaVersion {
		return invalid("unsupported schema_version %d", e.SchemaVersion)
	}
	if e.TenantID == "" {
		return invalid("tenant is required")
	}
	if !sourceRe.MatchString(e.Source) {
		return invalid("source %q is invalid", e.Source)
	}
	e.Type = strings.ToLower(strings.TrimSpace(e.Type))
	if !typeRe.MatchString(e.Type) {
		return invalid("type %q is invalid (want dotted lower-case, e.g. alert.firing)", e.Type)
	}
	ref, err := cpkit.NormalizeRef(e.SubjectRef)
	if err != nil {
		return invalid("subject_ref: %v", err)
	}
	e.SubjectRef = ref
	if len(e.CorrelationID) > MaxIDLen || len(e.DedupKey) > MaxIDLen || len(e.TraceID) > 128 {
		return invalid("correlation_id/dedup_key/trace_id too long")
	}
	if e.ObservedAt.IsZero() {
		e.ObservedAt = now
	}
	if e.OccurredAt.IsZero() {
		e.OccurredAt = e.ObservedAt
	}
	e.OccurredAt, e.ObservedAt = e.OccurredAt.UTC(), e.ObservedAt.UTC()
	if e.OccurredAt.After(now.Add(MaxClockSkew)) || e.ObservedAt.After(now.Add(MaxClockSkew)) {
		return invalid("timestamps are in the future")
	}
	if e.OccurredAt.Before(now.Add(-MaxEventAge)) {
		return invalid("occurred_at is older than %s", MaxEventAge)
	}
	if e.ExpiresAt != nil {
		x := e.ExpiresAt.UTC()
		if !x.After(e.OccurredAt) {
			return invalid("expires_at must be after occurred_at")
		}
		e.ExpiresAt = &x
	}
	switch {
	case e.Confidence == 0:
		e.Confidence = 1
	case e.Confidence < 0 || e.Confidence > 1:
		return invalid("confidence must be in [0,1]")
	}
	if len(e.Labels) > MaxLabels {
		return invalid("more than %d labels", MaxLabels)
	}
	for k, v := range e.Labels {
		if !labelRe.MatchString(k) || len(v) > MaxLabelValue {
			return invalid("label %q is invalid", k)
		}
	}
	if len(e.Payload) > 0 {
		if len(e.Payload) > MaxPayloadBytes {
			return invalid("payload exceeds %d bytes", MaxPayloadBytes)
		}
		if !json.Valid(e.Payload) {
			return invalid("payload must be JSON")
		}
		e.PayloadHash = cpkit.HashBytes(e.Payload)
	}
	return nil
}

// Expired reports whether the event is past its ExpiresAt.
func (e *Envelope) Expired(now time.Time) bool {
	return e.ExpiresAt != nil && !e.ExpiresAt.After(now)
}

// Label returns a label value.
func (e *Envelope) Label(k string) string { return e.Labels[k] }

// SortedLabelKeys is a deterministic label iteration order.
func (e *Envelope) SortedLabelKeys() []string {
	keys := make([]string, 0, len(e.Labels))
	for k := range e.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Processing states of an inbox record.
const (
	StateNew       = "NEW"
	StateProcessed = "PROCESSED"
	StateIgnored   = "IGNORED"
	StateFailed    = "FAILED"
	StateDead      = "DEAD" // repeated failures: kept for repair, never dropped
)

// Record is a persisted inbox entry.
type Record struct {
	InboxID    string    `json:"inbox_id"`
	Envelope   Envelope  `json:"envelope"`
	State      string    `json:"state"`
	Attempts   int       `json:"attempts"`
	LastError  string    `json:"last_error,omitempty"`
	WorkID     string    `json:"work_id,omitempty"`
	Decision   string    `json:"decision,omitempty"` // created | attached | ignored | matched | shadow
	ReceivedAt time.Time `json:"received_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}
