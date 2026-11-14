package protocol

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/work"
)

func TestWritersDeclareVersions(t *testing.T) {
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	for _, ev := range []Event{FromWork(work.Event{ID: "event", Seq: 1, TenantID: "t", WorkID: "w", Type: "custom.future.type", OccurredAt: now}), Delta("t", "w", "run", "text", now)} {
		if ev.SchemaVersion != SchemaVersion || ev.ContractVersion != ContractVersion || ev.MinimumReaderVersion != MinimumReaderVersion {
			t.Fatalf("missing version: %+v", ev)
		}
		if err := ev.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOnlyExactLegacyVersionShapesAreAccepted(t *testing.T) {
	for _, body := range []string{
		`{"schema_version":1,"work":{"id":"w"}}`,
		`{"schema_version":0,"work":{"id":"w"}}`,
		`{"schema_version":null,"work":{"id":"w"}}`,
		`{"schema_version":1,"contract_version":0,"minimum_reader_version":0,"work":{"id":"w"}}`,
	} {
		var v View
		err := json.Unmarshal([]byte(body), &v)
		if err == nil {
			err = v.Validate()
		}
		if err == nil {
			t.Errorf("invalid legacy snapshot accepted: %s", body)
		}
	}
	var old View
	if err := json.Unmarshal([]byte(`{"work":{"id":"w"}}`), &old); err != nil {
		t.Fatal(err)
	}
	if err := old.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCompatibilityErrorIsTyped(t *testing.T) {
	var pe *Error
	if err := ValidateVersions(2, 1, 1); !errors.As(err, &pe) || pe.Code != IncompatibleVersion {
		t.Fatalf("untyped incompatible version: %v", err)
	}
	for _, v := range [][3]int{{1, 1, 1}, {1, 0, 0}} {
		if err := ValidateVersions(v[0], v[1], v[2]); err != nil {
			t.Fatal(err)
		}
	}
}

// Existing Go callers can keep constructing v1 named literals; absent new
// metadata must stay absent on the wire, rather than becoming explicit zero.
func TestLegacyCompositeLiteralRoundTrip(t *testing.T) {
	old := Event{SchemaVersion: 1, EventID: "event", Sequence: 1, Type: "work.created", TenantID: "t", WorkID: "w", OccurredAt: time.Now().UTC()}
	b, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Event
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("legacy event literal broke: %s: %v", b, err)
	}
	oldView := View{Work: work.Work{ID: "w"}}
	b, err = json.Marshal(oldView)
	if err != nil {
		t.Fatal(err)
	}
	var view View
	if err := json.Unmarshal(b, &view); err != nil {
		t.Fatalf("legacy view literal broke: %s: %v", b, err)
	}
}
