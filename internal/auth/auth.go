// Package auth authenticates API callers and authorizes them by role (§19).
//
// Three authenticators are supported: static bearer tokens (stored as SHA-256
// hashes), OIDC JWTs verified against the issuer's JWKS, and a development
// mode that trusts X-Tenant-ID / X-Actor-ID headers. Development mode must
// never be exposed beyond localhost.
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
)

type Role string

const (
	Viewer   Role = "viewer"   // read runs, events, memory, evaluations
	Operator Role = "operator" // create runs/turns, pause/resume/cancel, steer, feedback
	Approver Role = "approver" // approve/reject tool actions and evolution candidates
	Admin    Role = "admin"    // promote/rollback profiles, manage memory and tenants
)

// implies: a role grants itself plus the roles listed here.
var implies = map[Role][]Role{
	Viewer:   {Viewer},
	Operator: {Operator, Viewer},
	Approver: {Approver, Viewer},
	Admin:    {Admin, Operator, Approver, Viewer},
}

func ValidRole(r Role) bool { _, ok := implies[r]; return ok }

type Principal struct {
	Tenant  string `json:"tenant"`
	Subject string `json:"subject"`
	Roles   []Role `json:"roles"`
	Method  string `json:"method"` // token | oidc | dev
}

func (p Principal) Has(r Role) bool {
	for _, have := range p.Roles {
		if slices.Contains(implies[have], r) {
			return true
		}
	}
	return false
}

var (
	ErrUnauthenticated = errors.New("unauthenticated")
	ErrForbidden       = errors.New("forbidden")
)

type Authenticator interface {
	// Authenticate returns (principal, true, nil) on success, (_, false, nil)
	// if the request carries no credential this authenticator understands,
	// and an error if a credential was present but invalid.
	Authenticate(r *http.Request) (Principal, bool, error)
}

// RoleSource adds roles granted in the database (role_bindings).
type RoleSource interface {
	RolesFor(ctx context.Context, tenantID, principalID string) ([]Role, error)
}

// Chain tries authenticators in order.
type Chain struct {
	Authenticators []Authenticator
	Roles          RoleSource
}

func (c *Chain) Authenticate(r *http.Request) (Principal, error) {
	for _, a := range c.Authenticators {
		p, ok, err := a.Authenticate(r)
		if err != nil {
			return Principal{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
		}
		if !ok {
			continue
		}
		if p.Tenant == "" || p.Subject == "" || !validTenant(p.Tenant) {
			return Principal{}, fmt.Errorf("%w: credential lacks tenant or subject", ErrUnauthenticated)
		}
		// A principal is bound to its tenant; a header naming another tenant is refused.
		if h := r.Header.Get("X-Tenant-ID"); h != "" && h != p.Tenant {
			return Principal{}, fmt.Errorf("%w: credential is not valid for tenant %q", ErrForbidden, h)
		}
		if c.Roles != nil && p.Method != "dev" {
			extra, err := c.Roles.RolesFor(r.Context(), p.Tenant, p.Subject)
			if err != nil {
				return Principal{}, err
			}
			p.Roles = mergeRoles(p.Roles, extra)
		}
		return p, nil
	}
	return Principal{}, fmt.Errorf("%w: no credentials", ErrUnauthenticated)
}

func validTenant(t string) bool {
	return len(t) <= 64 && !strings.ContainsAny(t, "/\\. ")
}

func mergeRoles(a, b []Role) []Role {
	out := append([]Role(nil), a...)
	for _, r := range b {
		if ValidRole(r) && !slices.Contains(out, r) {
			out = append(out, r)
		}
	}
	return out
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if t, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(t)
	}
	return ""
}

// ---------- static tokens ----------

// StaticToken is one entry of the token file. Only the SHA-256 of the token
// is stored: {"sha256":"...","tenant":"t","subject":"ci-bot","roles":["operator"]}.
type StaticToken struct {
	SHA256  string `json:"sha256"`
	Tenant  string `json:"tenant"`
	Subject string `json:"subject"`
	Roles   []Role `json:"roles"`
}

type StaticTokens struct{ tokens []StaticToken }

func LoadStaticTokens(path string) (*StaticTokens, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ts []StaticToken
	if err := json.Unmarshal(b, &ts); err != nil {
		return nil, fmt.Errorf("token file: %w", err)
	}
	return NewStaticTokens(ts)
}

func NewStaticTokens(ts []StaticToken) (*StaticTokens, error) {
	for i, t := range ts {
		if len(t.SHA256) != 64 || t.Tenant == "" || t.Subject == "" {
			return nil, fmt.Errorf("token %d: sha256 (64 hex), tenant and subject are required", i)
		}
		for _, r := range t.Roles {
			if !ValidRole(r) {
				return nil, fmt.Errorf("token %d: unknown role %q", i, r)
			}
		}
		ts[i].SHA256 = strings.ToLower(t.SHA256)
	}
	return &StaticTokens{tokens: ts}, nil
}

func HashToken(tok string) string {
	h := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(h[:])
}

func (s *StaticTokens) Authenticate(r *http.Request) (Principal, bool, error) {
	tok := bearer(r)
	if tok == "" || strings.Count(tok, ".") == 2 { // JWTs are handled by OIDC
		return Principal{}, false, nil
	}
	h := []byte(HashToken(tok))
	for _, t := range s.tokens {
		if subtle.ConstantTimeCompare(h, []byte(t.SHA256)) == 1 {
			return Principal{Tenant: t.Tenant, Subject: t.Subject, Roles: t.Roles, Method: "token"}, true, nil
		}
	}
	return Principal{}, false, errors.New("unknown token")
}

// ---------- development headers ----------

// DevHeaders trusts X-Tenant-ID / X-Actor-ID. Roles come from X-Roles
// (comma-separated) or default to all roles, which keeps demos working.
type DevHeaders struct{}

func (DevHeaders) Authenticate(r *http.Request) (Principal, bool, error) {
	t, a := r.Header.Get("X-Tenant-ID"), r.Header.Get("X-Actor-ID")
	if t == "" || a == "" {
		return Principal{}, false, nil
	}
	roles := []Role{Admin}
	if h := r.Header.Get("X-Roles"); h != "" {
		roles = nil
		for _, x := range strings.Split(h, ",") {
			if rr := Role(strings.TrimSpace(x)); ValidRole(rr) {
				roles = append(roles, rr)
			}
		}
	}
	return Principal{Tenant: t, Subject: a, Roles: roles, Method: "dev"}, true, nil
}

// ---------- context ----------

type ctxKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}
