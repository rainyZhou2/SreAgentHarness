package eventing

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
)

// SchemaVersionV2 is the production event contract (design §9.2). The inbox
// keeps storing the normalized Envelope (schema 1); v2 is a wire format that
// Upcast maps onto it, so both versions are accepted side by side and
// nothing stored has to be rewritten.
const SchemaVersionV2 = 2

// EnvelopeV2 is the v2 wire envelope.
type EnvelopeV2 struct {
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

// Upcast decodes a v1 or v2 envelope into the normalized Envelope. The
// tenant is never read from the body: the caller sets it from the
// authenticated principal. Unknown fields are rejected in both versions.
func Upcast(raw []byte) (Envelope, error) {
	var probe struct {
		V int `json:"schema_version"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return Envelope{}, invalid("body is not a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	switch probe.V {
	case 0, SchemaVersion:
		var e Envelope
		if err := dec.Decode(&e); err != nil {
			return Envelope{}, invalid("%v", err)
		}
		return e, nil
	case SchemaVersionV2:
		var v EnvelopeV2
		if err := dec.Decode(&v); err != nil {
			return Envelope{}, invalid("%v", err)
		}
		if v.PayloadDigest != "" && len(v.Payload) > 0 && cpkit.HashBytes(v.Payload) != v.PayloadDigest {
			return Envelope{}, invalid("payload does not match payload_digest")
		}
		return Envelope{ID: v.EventID, SchemaVersion: SchemaVersion, Source: v.SourceID, Type: v.Type, SubjectRef: v.SubjectRef,
			OccurredAt: v.OccurredAt, ObservedAt: v.ObservedAt, ExpiresAt: v.ExpiresAt, TraceID: v.TraceID,
			DedupKey: v.CorrelationKey, CorrelationID: v.CorrelationKey, PayloadRef: v.PayloadRef, PayloadHash: v.PayloadDigest,
			Payload: v.Payload, Labels: v.Labels, Confidence: v.Confidence}, nil
	}
	return Envelope{}, invalid("unsupported schema_version %d", probe.V)
}

// SourcePolicy is what the operator declared about an event source (design
// §9.3). The source id, its tenant and the event types it may send are
// operator configuration, not claims of a request body.
type SourcePolicy struct {
	// EventTypes the source may send; a trailing ".*" matches a prefix.
	// Empty allows every type.
	EventTypes []string `json:"event_types,omitempty"`
	// SecretEnv names the environment variable holding the HMAC-SHA256 key
	// that signs request bodies (header X-Harness-Signature: sha256=<hex>).
	// Empty: the source is authenticated by its API credential only.
	SecretEnv string `json:"secret_env,omitempty"`
	// MaxLateness is how old an event may be and still create a Work. An
	// older event is kept and attached to the open Work of its group, but
	// never opens a new one and never restarts a closed generation.
	MaxLateness cpkit.Duration `json:"max_lateness,omitempty"`
	// Tenants restricts which tenants may use the source (empty: any).
	Tenants  []string `json:"tenants,omitempty"`
	Disabled bool     `json:"disabled,omitempty"`
}

// DefaultMaxLateness applies to sources without their own.
const DefaultMaxLateness = 6 * time.Hour

// ErrUnattested: the request does not prove it comes from the source.
var ErrUnattested = errors.New("event source attestation failed")

// SignatureHeader carries the body signature of a signed source.
const SignatureHeader = "X-Harness-Signature"

// Sign returns the signature header value of body under key.
func Sign(key, body []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

// Verify checks the request against the policy: the source is enabled for
// the tenant and, when it signs its requests, the body signature matches.
func (p SourcePolicy) Verify(tenantID string, h http.Header, body []byte) error {
	if p.Disabled {
		return fmt.Errorf("%w: the source is disabled", ErrUnattested)
	}
	if len(p.Tenants) > 0 {
		ok := false
		for _, t := range p.Tenants {
			ok = ok || t == tenantID
		}
		if !ok {
			return fmt.Errorf("%w: the source is not registered for this tenant", ErrUnattested)
		}
	}
	if p.SecretEnv == "" {
		return nil
	}
	key := os.Getenv(p.SecretEnv)
	if key == "" {
		// A configured but missing key must not turn into "no signature
		// needed".
		return fmt.Errorf("%w: the signing key of the source is not available", ErrUnattested)
	}
	got := strings.TrimSpace(h.Get(SignatureHeader))
	if got == "" || !hmac.Equal([]byte(got), []byte(Sign([]byte(key), body))) {
		return fmt.Errorf("%w: missing or wrong body signature", ErrUnattested)
	}
	return nil
}

// AllowsType reports whether the source may send the event type.
func (p SourcePolicy) AllowsType(t string) bool {
	return len(p.EventTypes) == 0 || matchType(p.EventTypes, t)
}

// Late reports whether an event is too old to open a Work.
func (p SourcePolicy) Late(e *Envelope, now time.Time) bool {
	max := p.MaxLateness.D()
	if max <= 0 {
		max = DefaultMaxLateness
	}
	return now.Sub(e.OccurredAt) > max
}

// Version identifies a rule's content: a recorded decision names the rule
// version that made it, so a changed rule can be compared and rolled back.
func (r Rule) Version() string { return cpkit.MustHash(r)[7:19] }

// Decision records why an event went where it went (design §9.5).
type Decision struct {
	TenantID    string `json:"tenant_id"`
	InboxID     string `json:"inbox_id"`
	RuleName    string `json:"rule_name,omitempty"`
	RuleVersion string `json:"rule_version,omitempty"`
	// Decision is the routing result (created, attached, no_rule, ...).
	Decision string `json:"decision"`
	WorkID   string `json:"work_id,omitempty"`
	// Basis: deterministic (a rule keyed the event to the Work), topology
	// (a dependency of an open Work's subject — a lead for investigation,
	// never grounds for a write), or none.
	Basis string `json:"basis"`
	// GroupKey is the correlation group the event fell into.
	GroupKey string `json:"group_key,omitempty"`
	// Candidates are the Works considered, with why each was or was not
	// chosen.
	Candidates []Candidate `json:"candidates,omitempty"`
	Reason     string      `json:"reason,omitempty"`
	Late       bool        `json:"late,omitempty"`
	Shadow     bool        `json:"shadow,omitempty"`
	At         time.Time   `json:"at"`
}

// Candidate is one Work considered for an event.
type Candidate struct {
	WorkID   string  `json:"work_id"`
	Relation string  `json:"relation"` // same_group | dependency
	Score    float64 `json:"score"`
	Chosen   bool    `json:"chosen"`
	Reason   string  `json:"reason,omitempty"`
}
