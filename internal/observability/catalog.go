// Package observability is the controlled read path to telemetry (design
// §5): a tenant-scoped Source Catalog, read-only Providers, and a Query
// Broker that authorizes, bounds, redacts and audits every query and turns
// its result into typed evidence.
//
// The data plane stays where it is: Prometheus, Loki, Tempo and the change
// systems remain the stores of record. The broker never copies telemetry
// into the control-plane database; a result is an artifact with a digest.
//
// Hard rules, enforced here and not left to adapters:
//
//   - the tenant comes from the server-side request context, never from a
//     tool argument, and a source is only visible inside its tenant;
//   - an endpoint and a credential are operator-owned references; no API
//     and no model output can make the broker call an arbitrary URL;
//   - a subject must be inside the source's allowed scope and, for a Work,
//     inside the Work's subjects — a tenant label in a query is no boundary;
//   - a provider is read-only; changing monitoring configuration is an
//     action, never a query;
//   - EMPTY and UNAVAILABLE are not health: only data that exists, is fresh
//     and complete can support a "healthy" statement.
package observability

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
)

// Signal is what a source carries.
type Signal string

const (
	SignalMetrics Signal = "metrics"
	SignalLogs    Signal = "logs"
	SignalTraces  Signal = "traces"
	SignalEvents  Signal = "events"
	SignalChange  Signal = "changes"
)

// ValidSignal reports whether s is known. "change" is accepted as an alias
// of "changes".
func ValidSignal(s Signal) bool {
	switch s {
	case SignalMetrics, SignalLogs, SignalTraces, SignalEvents, SignalChange:
		return true
	}
	return false
}

// Source states.
const (
	StateActive   = "ACTIVE"
	StateDegraded = "DEGRADED"
	StateDisabled = "DISABLED"
)

// QueryCapabilities bound what one source may be asked.
type QueryCapabilities struct {
	MaxRange      cpkit.Duration `json:"max_range,omitempty"`
	MaxSeries     int            `json:"max_series,omitempty"`
	MaxDataBytes  int64          `json:"max_data_bytes,omitempty"`
	MaxConcurrent int            `json:"max_concurrent,omitempty"`
	// AllowRawQuery lets expert callers send their own expression. Without
	// it only the source's named query templates run.
	AllowRawQuery bool `json:"allow_raw_query,omitempty"`
	// ReadOnly is always true for a catalog source; it is part of the record
	// so an attestation can show it.
	ReadOnly bool `json:"read_only"`
	// MaxExpression bounds a raw expression (default 2000 bytes).
	MaxExpression int `json:"max_expression,omitempty"`
	// Timeout bounds one backend call (default 15s).
	Timeout cpkit.Duration `json:"timeout,omitempty"`
}

// Defaults of the capabilities.
const (
	DefaultMaxRange      = 6 * time.Hour
	DefaultMaxSeries     = 50
	DefaultMaxDataBytes  = 4 << 20
	DefaultMaxConcurrent = 4
	DefaultMaxExpression = 2000
	DefaultTimeout       = 15 * time.Second
	MaxQueries           = 64
)

// Source is one telemetry backend as the operator registered it.
type Source struct {
	ID          string `json:"id"`
	TenantID    string `json:"tenant_id"`
	Kind        Signal `json:"kind"`    // metrics|logs|traces|events|changes
	Backend     string `json:"backend"` // prometheus|victoriametrics|mimir|loki|tempo|jaeger|ledger
	Environment string `json:"environment"`
	Region      string `json:"region,omitempty"`
	Cluster     string `json:"cluster,omitempty"`
	// EndpointRef names an operator-owned endpoint record. The URL behind it
	// is configuration of the server; it is never part of this record and
	// never shown to a model.
	EndpointRef string `json:"endpoint_ref"`
	// CredentialID names a secret-manager entry (resolved by the credential
	// broker at call time). Never a secret.
	CredentialID string `json:"credential_id,omitempty"`
	// AllowedScope lists the resource refs this source may be asked about
	// ("service://prod", "deployment://prod/*"). Empty denies everything: a
	// source without a scope is not usable.
	AllowedScope []string          `json:"allowed_scope"`
	QueryCaps    QueryCapabilities `json:"query_caps"`
	// Queries are the operator-managed query templates by name. ${namespace},
	// ${name}, ${service}, ${target} are filled from the subject ref and
	// escaped for the backend.
	Queries map[string]string `json:"queries,omitempty"`
	// DataClass is the sensitivity of what the source returns (default
	// internal). A result above the caller's class is withheld.
	DataClass string `json:"data_class,omitempty"`
	// Freshness is the max age of the newest sample before a result counts
	// as stale (default 5m for metrics, unset for the others).
	Freshness cpkit.Duration `json:"freshness,omitempty"`
	// Authority ranks the source for conflicts (higher wins; 0 = unranked).
	Authority int       `json:"authority,omitempty"`
	State     string    `json:"state"` // ACTIVE|DEGRADED|DISABLED
	Version   int64     `json:"version"`
	Health    *Health   `json:"health,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Health is the last probe of a source.
type Health struct {
	OK          bool      `json:"ok"`
	CheckedAt   time.Time `json:"checked_at"`
	LastSuccess time.Time `json:"last_success,omitempty"`
	// Failures counts consecutive failed probes.
	Failures int    `json:"failures,omitempty"`
	Error    string `json:"error,omitempty"`
	// Authorized is false when the backend refused the credential (401/403):
	// the source is reachable but this deployment may not read it.
	Authorized bool  `json:"authorized"`
	LatencyMS  int64 `json:"latency_ms,omitempty"`
}

// Errors.
var (
	ErrInvalid = errors.New("invalid observability request")
	// ErrNotFound: no such source in this tenant. A source of another tenant
	// is indistinguishable from a missing one.
	ErrNotFound        = errors.New("observability source not found")
	ErrVersionConflict = errors.New("observability source version conflict")
)

func invalidf(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
}

var (
	idRe      = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)
	queryIDRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

// backends a kind may use; providers register the implementations.
var backendKinds = map[string][]Signal{
	"prometheus":      {SignalMetrics},
	"victoriametrics": {SignalMetrics},
	"mimir":           {SignalMetrics},
	"loki":            {SignalLogs},
	"tempo":           {SignalTraces},
	"jaeger":          {SignalTraces},
	"ledger":          {SignalChange, SignalEvents},
}

// Normalize validates a source and fills defaults.
func (s *Source) Normalize() error {
	if !idRe.MatchString(s.ID) {
		return invalidf("source id %q (lowercase, [a-z0-9_.-], max 64)", s.ID)
	}
	if s.TenantID == "" {
		return invalidf("tenant is required")
	}
	if s.Kind == "change" {
		s.Kind = SignalChange
	}
	if !ValidSignal(s.Kind) {
		return invalidf("unknown kind %q", s.Kind)
	}
	kinds, ok := backendKinds[s.Backend]
	if !ok {
		return invalidf("unknown backend %q", s.Backend)
	}
	match := false
	for _, k := range kinds {
		match = match || k == s.Kind
	}
	if !match {
		return invalidf("backend %s does not serve %s", s.Backend, s.Kind)
	}
	if s.Environment == "" || len(s.Environment) > 64 {
		return invalidf("environment is required")
	}
	if !idRe.MatchString(s.EndpointRef) {
		return invalidf("endpoint_ref %q must name an operator endpoint record, not a URL", s.EndpointRef)
	}
	if s.CredentialID != "" && !idRe.MatchString(s.CredentialID) {
		return invalidf("credential_id %q must name a secret-manager entry", s.CredentialID)
	}
	if len(s.AllowedScope) > 64 {
		return invalidf("too many scope entries")
	}
	scope := make([]string, 0, len(s.AllowedScope))
	for _, p := range s.AllowedScope {
		wild := strings.HasSuffix(p, "/*")
		n, err := cpkit.NormalizeRef(strings.TrimSuffix(p, "/*"))
		if err != nil {
			return invalidf("allowed_scope: %v", err)
		}
		if wild {
			n += "/*"
		}
		scope = append(scope, n)
	}
	sort.Strings(scope)
	s.AllowedScope = scope
	if len(s.Queries) > MaxQueries {
		return invalidf("more than %d query templates", MaxQueries)
	}
	for name, q := range s.Queries {
		if !queryIDRe.MatchString(name) || strings.TrimSpace(q) == "" || len(q) > 4000 {
			return invalidf("query template %q is invalid", name)
		}
	}
	c := &s.QueryCaps
	if c.MaxRange < 0 || c.MaxSeries < 0 || c.MaxDataBytes < 0 || c.MaxConcurrent < 0 || c.MaxExpression < 0 || c.Timeout < 0 {
		return invalidf("query_caps must not be negative")
	}
	if c.MaxRange == 0 {
		c.MaxRange = cpkit.Duration(DefaultMaxRange)
	}
	if c.MaxSeries == 0 {
		c.MaxSeries = DefaultMaxSeries
	}
	if c.MaxDataBytes == 0 {
		c.MaxDataBytes = DefaultMaxDataBytes
	}
	if c.MaxConcurrent == 0 {
		c.MaxConcurrent = DefaultMaxConcurrent
	}
	if c.MaxExpression == 0 {
		c.MaxExpression = DefaultMaxExpression
	}
	if c.Timeout == 0 {
		c.Timeout = cpkit.Duration(DefaultTimeout)
	}
	if c.Timeout.D() > 2*time.Minute || c.MaxRange.D() > 31*24*time.Hour {
		return invalidf("query_caps exceed the framework bounds (timeout 2m, range 31d)")
	}
	c.ReadOnly = true // a catalog source is never a write path
	switch s.DataClass {
	case "":
		s.DataClass = "internal"
	case "public", "internal", "confidential", "restricted":
	default:
		return invalidf("unknown data_class %q", s.DataClass)
	}
	if s.Freshness < 0 {
		return invalidf("freshness must not be negative")
	}
	if s.Freshness == 0 && s.Kind == SignalMetrics {
		s.Freshness = cpkit.Duration(5 * time.Minute)
	}
	switch s.State {
	case "":
		s.State = StateDisabled
	case StateActive, StateDegraded, StateDisabled:
	default:
		return invalidf("unknown state %q", s.State)
	}
	return nil
}

// Allows reports whether the source may be asked about subject.
func (s *Source) Allows(subject string) bool {
	for _, p := range s.AllowedScope {
		if cpkit.RefMatches(p, subject) {
			return true
		}
	}
	return false
}

// Usable reports whether queries may be sent (ACTIVE or DEGRADED).
func (s *Source) Usable() bool { return s.State == StateActive || s.State == StateDegraded }

// Revision is one recorded version of a source.
type Revision struct {
	Version   int64     `json:"version"`
	Source    Source    `json:"source"`
	ActorID   string    `json:"actor_id"`
	Reason    string    `json:"reason,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Filter selects sources.
type Filter struct {
	Kind        Signal
	Environment string
	// UsableOnly keeps ACTIVE and DEGRADED sources.
	UsableOnly bool
}

// Catalog is the persistence port of the source catalog
// (internal/store/cpsql). Every method is tenant-scoped.
type Catalog interface {
	// PutSource creates (expectedVersion 0) or CAS-updates a source and
	// appends a revision. The stored version is returned.
	PutSource(ctx context.Context, s Source, expectedVersion int64, actorID, reason string) (Source, error)
	GetSource(ctx context.Context, tenantID, id string) (Source, error)
	ListSources(ctx context.Context, tenantID string, f Filter) ([]Source, error)
	SourceRevisions(ctx context.Context, tenantID, id string, limit int) ([]Revision, error)
	// SetSourceHealth records a probe without creating a revision.
	SetSourceHealth(ctx context.Context, tenantID, id string, h Health, state string) error
}

// Rollback re-activates an earlier revision as a new version: history is
// never rewritten, and the rollback itself is a recorded change.
func Rollback(ctx context.Context, c Catalog, tenantID, id string, toVersion int64, actorID, reason string) (Source, error) {
	cur, err := c.GetSource(ctx, tenantID, id)
	if err != nil {
		return Source{}, err
	}
	revs, err := c.SourceRevisions(ctx, tenantID, id, 200)
	if err != nil {
		return Source{}, err
	}
	for _, r := range revs {
		if r.Version == toVersion {
			next := r.Source
			next.TenantID, next.ID, next.Health = tenantID, id, cur.Health
			if reason == "" {
				reason = fmt.Sprintf("rollback to version %d", toVersion)
			}
			return c.PutSource(ctx, next, cur.Version, actorID, reason)
		}
	}
	return Source{}, fmt.Errorf("%w: revision %d of source %s", ErrNotFound, toVersion, id)
}

// Route picks the sources able to answer a signal about a subject: usable,
// of the right kind and environment, with the subject in scope. The most
// authoritative come first, then healthy before degraded, then by id.
func Route(sources []Source, signal Signal, subject, environment string) []Source {
	var out []Source
	for _, s := range sources {
		if s.Kind != signal || !s.Usable() || (environment != "" && s.Environment != environment) {
			continue
		}
		if subject != "" && !s.Allows(subject) {
			continue
		}
		out = append(out, s)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Authority != b.Authority {
			return a.Authority > b.Authority
		}
		if (a.State == StateActive) != (b.State == StateActive) {
			return a.State == StateActive
		}
		return a.ID < b.ID
	})
	return out
}
