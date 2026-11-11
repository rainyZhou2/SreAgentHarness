package observability

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go-sre-harness/internal/cpkit"
)

// Query is one read request. TenantID and WorkID are set by the broker from
// the server-side request context; a caller cannot choose them.
type Query struct {
	TenantID   string    `json:"-"`
	WorkID     string    `json:"-"`
	SourceID   string    `json:"source_id"`
	Signal     Signal    `json:"signal"`
	SubjectRef string    `json:"subject_ref,omitempty"`
	QueryRef   string    `json:"query_ref,omitempty"`  // operator-managed template (preferred)
	Expression string    `json:"expression,omitempty"` // expert callers only
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
	Cursor     string    `json:"cursor,omitempty"`
	Limit      int       `json:"limit,omitempty"`
	// Rendered is the backend expression after template expansion; the
	// broker fills it for the provider.
	Rendered string `json:"-"`
}

// Digest identifies a query for caching, audit and evidence independence:
// the same source, expression and window is the same observation, whoever
// asked. The expression is hashed, never stored.
func (q Query) Digest() string {
	return cpkit.MustHash(struct {
		Source, Signal, Subject, Ref, Expr string
		Start, End                         int64
		Limit                              int
	}{q.SourceID, string(q.Signal), q.SubjectRef, q.QueryRef, q.Rendered, q.Start.Unix(), q.End.Unix(), q.Limit})
}

// Result statuses (design §5.5). Only StatusOK may support a statement that
// something is healthy.
const (
	StatusOK           = "ok"
	StatusPartial      = "partial"     // some shards or clusters failed
	StatusEmpty        = "empty"       // the query ran and matched nothing
	StatusStale        = "stale"       // samples exist but are too old
	StatusUnavailable  = "unavailable" // the backend could not be reached
	StatusUnauthorized = "unauthorized"
	StatusDenied       = "denied"   // this caller may not ask this
	StatusConflict     = "conflict" // sources disagree
)

// Result is the normalized outcome of a query. The payload itself is an
// artifact (DataRef); the model receives the summary and the reference.
type Result struct {
	SourceID   string    `json:"source_id"`
	Signal     Signal    `json:"signal"`
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
	SampledAt  time.Time `json:"sampled_at,omitempty"` // newest sample in the data
	ReceivedAt time.Time `json:"received_at"`
	DataRef    string    `json:"data_ref,omitempty"`
	Digest     string    `json:"digest,omitempty"`
	Count      int       `json:"count"`
	Truncated  bool      `json:"truncated,omitempty"`
	Status     string    `json:"status"` // ok|partial|empty|stale|unavailable|unauthorized|denied|conflict
	Warnings   []string  `json:"warnings,omitempty"`

	QueryDigest string `json:"query_digest,omitempty"`
	QueryRef    string `json:"query_ref,omitempty"`
	SubjectRef  string `json:"subject_ref,omitempty"`
	DataClass   string `json:"data_class,omitempty"`
	// RawDigest is the digest before redaction; Digest the digest of what
	// was stored. Both are kept so a redacted artifact can be tied back to
	// the observation it came from.
	RawDigest string `json:"raw_digest,omitempty"`
	Redacted  int    `json:"redacted,omitempty"`
	// Summary is a short, bounded description for the model. It is derived
	// data: conclusions cite DataRef, not the summary.
	Summary string `json:"summary,omitempty"`
	// Values are a few headline numbers (latest, min, max, rate…).
	Values map[string]float64 `json:"values,omitempty"`
	Cached bool               `json:"cached,omitempty"`
	// StoredAt is when the artifact was written.
	StoredAt time.Time `json:"stored_at,omitempty"`

	// Payload is the raw backend data a provider hands to the broker. It is
	// never serialized: the broker redacts and stores it as an artifact.
	Payload []byte `json:"-"`
}

// Healthy reports whether the result can support a health statement.
func (r Result) Healthy() bool { return r.Status == StatusOK }

// Provider reads one source. It must be read-only: anything that changes a
// backend is an action executor, never a provider.
type Provider interface {
	ID() string
	Signal() Signal
	// Probe checks reachability and authorization without reading data.
	Probe(ctx context.Context) error
	Query(ctx context.Context, q Query) (Result, error)
}

// Provider errors classify a failure for the broker.
var (
	// ErrUnauthorized: the backend refused the credential.
	ErrUnauthorized = errors.New("backend refused the credential")
	// ErrBadQuery: the backend rejected the expression (not retried).
	ErrBadQuery = errors.New("backend rejected the query")
)

// RetryAfterError asks the broker to wait before the next attempt.
type RetryAfterError struct {
	After time.Duration
	Err   error
}

func (e *RetryAfterError) Error() string { return e.Err.Error() }
func (e *RetryAfterError) Unwrap() error { return e.Err }

// Endpoint is an operator-owned backend address. It lives in the server
// configuration, keyed by the name a Source references.
type Endpoint struct {
	URL     string            `json:"url"`
	CAFile  string            `json:"ca_file,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	// OrgID is the multi-tenant header value of Loki/Mimir/Tempo
	// (X-Scope-OrgID). It is operator configuration: the backend tenant is
	// never derived from a query.
	OrgID string `json:"org_id,omitempty"`
}

// Secret is a resolved credential. It is held only for the duration of a
// call and never logged, stored or returned.
type Secret struct {
	Token  string
	Scheme string // Authorization scheme (default Bearer)
}

// CredentialBroker resolves a credential id for a tenant at call time.
type CredentialBroker interface {
	Resolve(ctx context.Context, tenantID, credentialID string) (Secret, error)
}

// Factory builds a provider for a source from its resolved endpoint. The
// credential is fetched through creds on every call (tokens rotate).
type Factory func(src Source, ep Endpoint, creds CredentialBroker) (Provider, error)

var (
	backendMu sync.RWMutex
	backends  = map[string]Factory{}
)

// RegisterBackend makes a backend available to the broker (called from the
// init of a provider plugin).
func RegisterBackend(name string, f Factory) {
	backendMu.Lock()
	defer backendMu.Unlock()
	if _, dup := backends[name]; dup {
		panic("observability: backend " + name + " registered twice")
	}
	if _, known := backendKinds[name]; !known {
		panic("observability: backend " + name + " is not a known backend kind")
	}
	backends[name] = f
}

// Backends lists the registered backends.
func Backends() []string {
	backendMu.RLock()
	defer backendMu.RUnlock()
	out := make([]string, 0, len(backends))
	for n := range backends {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func backend(name string) (Factory, bool) {
	backendMu.RLock()
	defer backendMu.RUnlock()
	f, ok := backends[name]
	return f, ok
}

// SubjectVars splits a subject ref into template variables: target (the
// whole ref), scheme, namespace and name (the last two path segments) and
// service (an alias of name).
func SubjectVars(ref string) map[string]string {
	vars := map[string]string{"target": ref}
	scheme, path, ok := strings.Cut(ref, "://")
	if !ok {
		return vars
	}
	vars["scheme"] = scheme
	parts := strings.Split(path, "/")
	vars["name"] = parts[len(parts)-1]
	vars["service"] = vars["name"]
	if len(parts) >= 2 {
		vars["namespace"] = parts[len(parts)-2]
	}
	if len(parts) >= 3 {
		vars["cluster"] = parts[0]
	}
	return vars
}

// Expand fills ${var} placeholders of a query template, escaping every
// value with esc (the backend's label-value escaping). An unknown or empty
// variable is an error: a query never runs with a hole in its selector.
func Expand(tpl string, vars map[string]string, esc func(string) string) (string, error) {
	var b strings.Builder
	for {
		i := strings.Index(tpl, "${")
		if i < 0 {
			b.WriteString(tpl)
			return b.String(), nil
		}
		j := strings.Index(tpl[i:], "}")
		if j < 0 {
			return "", fmt.Errorf("%w: unterminated placeholder", ErrInvalid)
		}
		name := tpl[i+2 : i+j]
		v, ok := vars[name]
		if !ok || v == "" {
			return "", fmt.Errorf("%w: the subject does not provide ${%s}", ErrInvalid, name)
		}
		b.WriteString(tpl[:i])
		b.WriteString(esc(v))
		tpl = tpl[i+j+1:]
	}
}
