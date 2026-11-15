package eventing

import (
	"encoding/json"
	"time"

	"go-sre-harness/internal/cpkit"
)

// WireSchemaVersion is the current external event contract. The inbox keeps
// storing the normalized Envelope, so accepting this wire format requires no
// rewrite of stored events.
const WireSchemaVersion = 2

// WireEnvelope is the current external event envelope.
type WireEnvelope struct {
	SchemaVersion  int               `json:"schema_version"`
	EventID        string            `json:"event_id"`
	SourceID       string            `json:"source_id"`
	Type           string            `json:"type"`
	SubjectRef     string            `json:"subject_ref"`
	OccurredAt     time.Time         `json:"occurred_at"`
	ObservedAt     time.Time         `json:"observed_at"`
	ExpiresAt      *time.Time        `json:"expires_at,omitempty"`
	TraceID        string            `json:"trace_id,omitempty"`
	CorrelationKey string            `json:"correlation_key,omitempty"`
	PayloadRef     string            `json:"payload_ref,omitempty"`
	PayloadDigest  string            `json:"payload_digest,omitempty"`
	Payload        json.RawMessage   `json:"payload,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Confidence     float64           `json:"confidence,omitempty"`
}

func upcastWire(raw []byte) (Envelope, error) {
	var v WireEnvelope
	if err := decodeStrict(raw, &v); err != nil {
		return Envelope{}, err
	}
	if v.PayloadDigest != "" && len(v.Payload) > 0 && cpkit.HashBytes(v.Payload) != v.PayloadDigest {
		return Envelope{}, invalid("payload does not match payload_digest")
	}
	return Envelope{ID: v.EventID, SchemaVersion: StoredSchemaVersion, Source: v.SourceID, Type: v.Type, SubjectRef: v.SubjectRef,
		OccurredAt: v.OccurredAt, ObservedAt: v.ObservedAt, ExpiresAt: v.ExpiresAt, TraceID: v.TraceID,
		DedupKey: v.CorrelationKey, CorrelationID: v.CorrelationKey, PayloadRef: v.PayloadRef, PayloadHash: v.PayloadDigest,
		Payload: v.Payload, Labels: v.Labels, Confidence: v.Confidence}, nil
}
