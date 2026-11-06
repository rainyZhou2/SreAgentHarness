package profile

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

// Definition is one immutable profile version supplied by a profile source.
type Definition struct {
	// Tenant the profile belongs to; empty means the source's default tenants.
	Tenant  string          `json:"tenant,omitempty"`
	ID      string          `json:"id"`
	Version int64           `json:"version"`
	Spec    json.RawMessage `json:"spec"`
	// Origin describes where the definition came from (file path etc.).
	Origin string `json:"-"`
	// Seed marks a default that only fills a gap: if the version already
	// exists with other content, the stored version wins (with a warning)
	// instead of failing startup.
	Seed bool `json:"-"`
}

// Source supplies profile versions at startup (the profile_source plugin
// kind). Versions are registered once and never overwritten: changing a
// published version is an error, publish a new version instead.
type Source interface {
	Profiles(ctx context.Context) ([]Definition, error)
}

// SourceFunc adapts a function.
type SourceFunc func(ctx context.Context) ([]Definition, error)

func (f SourceFunc) Profiles(ctx context.Context) ([]Definition, error) { return f(ctx) }

// Validate checks the definition and strictly decodes its spec. It returns
// the compacted spec to store.
func (d Definition) Validate() (Spec, json.RawMessage, error) {
	if d.ID == "" {
		return Spec{}, nil, fmt.Errorf("profile %s: id is required", d.Origin)
	}
	if d.Version <= 0 {
		return Spec{}, nil, fmt.Errorf("profile %s (%s): version must be > 0", d.ID, d.Origin)
	}
	var s Spec
	dec := json.NewDecoder(bytes.NewReader(d.Spec))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return Spec{}, nil, fmt.Errorf("profile %s@%d (%s): spec: %w", d.ID, d.Version, d.Origin, err)
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, d.Spec); err != nil {
		return Spec{}, nil, err
	}
	return s, buf.Bytes(), nil
}
