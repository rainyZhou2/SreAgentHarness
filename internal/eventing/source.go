package eventing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
)

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

// Verify checks the request against the policy: the source is enabled for the
// tenant and, when it signs its requests, the body signature matches.
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
		// A configured but missing key must not turn into "no signature needed".
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
