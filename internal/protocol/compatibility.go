package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Versions are deliberately bounded to the v1 contract. New optional fields and
// event types do not change these versions; incompatible shapes or semantics
// require an explicitly supported reader and a new versioned endpoint.
const (
	ContractVersion            = 1
	MinimumReaderVersion       = 1
	ReaderVersion              = 1
	HeaderSchemaVersion        = "X-Harness-Event-Schema"
	HeaderContractVersion      = "X-Harness-Contract-Version"
	HeaderMinimumReaderVersion = "X-Harness-Minimum-Reader-Version"
	IncompatibleVersion        = "incompatible_version"
	InvalidFrame               = "invalid_frame"
)

// Error is a permanent protocol failure. Retrying the same cursor cannot make
// incompatible or invalid data safe to consume. Clients must not advance it.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return "agent protocol: " + e.Code + ": " + e.Message }

// ValidateVersions accepts the complete current tuple and the exact legacy v1
// event/page tuple (schema=1, the other fields absent). A low minimum reader
// never grants permission to interpret an unknown schema or contract.
func ValidateVersions(schema, contract, minimumReader int) error {
	if schema == 1 && contract == 0 && minimumReader == 0 {
		return nil
	}
	return validateCurrentVersions(schema, contract, minimumReader)
}

func validateCurrentVersions(schema, contract, minimumReader int) error {
	if schema != SchemaVersion || contract != ContractVersion || minimumReader != MinimumReaderVersion || minimumReader > ReaderVersion {
		return &Error{Code: IncompatibleVersion, Message: fmt.Sprintf("unsupported schema=%d contract=%d minimum_reader=%d (reader=%d)", schema, contract, minimumReader, ReaderVersion)}
	}
	return nil
}

func (e Event) Validate() error {
	if err := ValidateVersions(e.SchemaVersion, e.ContractVersion, e.MinimumReaderVersion); err != nil {
		return err
	}
	if e.Type == "" || e.TenantID == "" || e.OccurredAt.IsZero() {
		return &Error{Code: InvalidFrame, Message: "event needs a type, tenant and timestamp"}
	}
	if e.Ephemeral {
		if e.Sequence != 0 {
			return &Error{Code: InvalidFrame, Message: "ephemeral event must not carry a durable sequence"}
		}
	} else if e.Sequence <= 0 || e.EventID == "" || e.WorkID == "" {
		return &Error{Code: InvalidFrame, Message: "durable event needs a positive sequence, event ID and Work ID"}
	}
	return nil
}

// EventPage retains compatibility metadata even when the page is empty.
type EventPage struct {
	SchemaVersion        int     `json:"schema_version"`
	ContractVersion      int     `json:"contract_version,omitempty"`
	MinimumReaderVersion int     `json:"minimum_reader_version,omitempty"`
	Items                []Event `json:"items"`
	NextAfterSeq         int64   `json:"next_after_seq"`
}

func (p EventPage) Validate() error {
	if err := ValidateVersions(p.SchemaVersion, p.ContractVersion, p.MinimumReaderVersion); err != nil {
		return err
	}
	if p.NextAfterSeq < 0 {
		return &Error{Code: InvalidFrame, Message: "page cursor cannot be negative"}
	}
	for _, e := range p.Items {
		if err := e.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (v View) Validate() error {
	// The existing v1 snapshot had no version metadata at all.
	if v.SchemaVersion != 0 || v.ContractVersion != 0 || v.MinimumReaderVersion != 0 {
		if err := validateCurrentVersions(v.SchemaVersion, v.ContractVersion, v.MinimumReaderVersion); err != nil {
			return err
		}
	}
	if v.Work.ID == "" || v.LastSeq < 0 {
		return &Error{Code: InvalidFrame, Message: "snapshot needs a Work ID and a nonnegative cursor"}
	}
	return nil
}

// Check presence before decoding integer fields: explicit zero or null is not
// the absence of metadata used by legacy v1 writers.
func validateVersionJSON(data []byte, legacySchema, legacyUnversioned bool) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return &Error{Code: InvalidFrame, Message: "contract response must be an object"}
	}
	names := []string{"schema_version", "contract_version", "minimum_reader_version"}
	values := [3]int{}
	count := 0
	for i, name := range names {
		raw, ok := fields[name]
		if !ok {
			continue
		}
		count++
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return &Error{Code: InvalidFrame, Message: "version metadata must not be null"}
		}
		if err := json.Unmarshal(raw, &values[i]); err != nil {
			return &Error{Code: InvalidFrame, Message: "version metadata must be integers"}
		}
	}
	if count == 0 && legacyUnversioned {
		return nil
	}
	if count == 1 && legacySchema && values[0] == 1 {
		return nil
	}
	if count != 3 {
		return &Error{Code: IncompatibleVersion, Message: "incomplete version metadata"}
	}
	return validateCurrentVersions(values[0], values[1], values[2])
}

func (e *Event) UnmarshalJSON(data []byte) error {
	if err := validateVersionJSON(data, true, false); err != nil {
		return err
	}
	type wire Event
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*e = Event(decoded)
	return nil
}
func (p *EventPage) UnmarshalJSON(data []byte) error {
	if err := validateVersionJSON(data, true, false); err != nil {
		return err
	}
	type wire EventPage
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*p = EventPage(decoded)
	return nil
}
func (v *View) UnmarshalJSON(data []byte) error {
	if err := validateVersionJSON(data, false, true); err != nil {
		return err
	}
	type wire View
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*v = View(decoded)
	return nil
}
