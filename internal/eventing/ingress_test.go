package eventing

import (
	"errors"
	"testing"
)

func TestUpcastCurrentWireEnvelope(t *testing.T) {
	raw := []byte(`{"schema_version":2,"event_id":"e1","source_id":"src","type":"alert.firing","subject_ref":"service://prod/api","occurred_at":"2026-01-01T00:00:00Z","observed_at":"2026-01-01T00:00:00Z","correlation_key":"alert-key","payload_digest":"digest"}`)
	got, err := Upcast(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "e1" || got.Source != "src" || got.SchemaVersion != StoredSchemaVersion {
		t.Fatalf("unexpected normalized identity: %+v", got)
	}
	if got.DedupKey != "alert-key" || got.CorrelationID != "alert-key" || got.PayloadHash != "digest" {
		t.Fatalf("unexpected normalized correlation: %+v", got)
	}
}

func TestUpcastLegacyEnvelope(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "implicit schema", raw: `{"event_id":"e0","source":"src","type":"alert.firing","subject_ref":"service://prod/api"}`},
		{name: "stored schema", raw: `{"schema_version":1,"event_id":"e1","source":"src","type":"alert.firing","subject_ref":"service://prod/api"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Upcast([]byte(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if got.SchemaVersion != StoredSchemaVersion {
				t.Fatalf("schema = %d, want %d", got.SchemaVersion, StoredSchemaVersion)
			}
		})
	}
}

func TestUpcastRejectsInvalidCurrentInput(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "unknown field", raw: `{"schema_version":2,"event_id":"e1","source_id":"src","unknown":true}`},
		{name: "digest mismatch", raw: `{"schema_version":2,"event_id":"e1","source_id":"src","payload":{"x":1},"payload_digest":"wrong"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Upcast([]byte(tc.raw)); !errors.Is(err, ErrInvalid) {
				t.Fatalf("error = %v, want ErrInvalid", err)
			}
		})
	}
}
