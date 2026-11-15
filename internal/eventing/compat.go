package eventing

import (
	"bytes"
	"encoding/json"
)

// Upcast decodes a legacy or current wire envelope into the normalized
// Envelope. The tenant is never read from the body: the caller sets it from
// the authenticated principal. Unknown fields are rejected in every format.
func Upcast(raw []byte) (Envelope, error) {
	var probe struct {
		V int `json:"schema_version"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return Envelope{}, invalid("body is not a JSON object")
	}
	switch probe.V {
	case 0, StoredSchemaVersion:
		var e Envelope
		if err := decodeStrict(raw, &e); err != nil {
			return Envelope{}, err
		}
		e.SchemaVersion = StoredSchemaVersion
		return e, nil
	case WireSchemaVersion:
		return upcastWire(raw)
	default:
		return Envelope{}, invalid("unsupported schema_version %d", probe.V)
	}
}

func decodeStrict(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return invalid("%v", err)
	}
	return nil
}
