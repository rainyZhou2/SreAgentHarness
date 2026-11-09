package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// OIDCConfig verifies JWT access/ID tokens issued by an OIDC provider.
type OIDCConfig struct {
	Issuer       string `json:"issuer"`
	Audience     string `json:"audience"`
	JWKSURL      string `json:"jwks_url,omitempty"`      // default: from discovery
	TenantClaim  string `json:"tenant_claim,omitempty"`  // default "tenant_id"
	RolesClaim   string `json:"roles_claim,omitempty"`   // default "roles"
	SubjectClaim string `json:"subject_claim,omitempty"` // default "sub"
	// ScopeClaim names an optional object claim with the Scope fields
	// (event_sources, event_types, resources, max_risk); default "harness_scope".
	ScopeClaim   string        `json:"scope_claim,omitempty"`
	Algorithms   []string      `json:"algorithms,omitempty"` // default RS256, ES256
	HTTP         *http.Client  `json:"-"`
	RefreshEvery time.Duration `json:"-"`
}

type OIDC struct {
	cfg OIDCConfig

	mu      sync.Mutex
	keys    map[string]crypto.PublicKey
	fetched time.Time
}

func NewOIDC(cfg OIDCConfig) (*OIDC, error) {
	if cfg.Issuer == "" || cfg.Audience == "" {
		return nil, errors.New("oidc: issuer and audience are required")
	}
	if cfg.TenantClaim == "" {
		cfg.TenantClaim = "tenant_id"
	}
	if cfg.RolesClaim == "" {
		cfg.RolesClaim = "roles"
	}
	if cfg.SubjectClaim == "" {
		cfg.SubjectClaim = "sub"
	}
	if cfg.ScopeClaim == "" {
		cfg.ScopeClaim = "harness_scope"
	}
	if len(cfg.Algorithms) == 0 {
		cfg.Algorithms = []string{"RS256", "ES256"}
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.RefreshEvery == 0 {
		cfg.RefreshEvery = 10 * time.Minute
	}
	return &OIDC{cfg: cfg}, nil
}

func (o *OIDC) Authenticate(r *http.Request) (Principal, bool, error) {
	tok := bearer(r)
	if tok == "" || strings.Count(tok, ".") != 2 {
		return Principal{}, false, nil
	}
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(tok, claims, o.keyFunc(r.Context()),
		jwt.WithValidMethods(o.cfg.Algorithms), jwt.WithIssuer(o.cfg.Issuer), jwt.WithAudience(o.cfg.Audience),
		jwt.WithExpirationRequired(), jwt.WithLeeway(30*time.Second))
	if err != nil {
		return Principal{}, false, err
	}
	p := Principal{Method: "oidc"}
	p.Subject, _ = claims[o.cfg.SubjectClaim].(string)
	p.Tenant, _ = claims[o.cfg.TenantClaim].(string)
	switch v := claims[o.cfg.RolesClaim].(type) {
	case []any:
		for _, x := range v {
			if s, ok := x.(string); ok && ValidRole(Role(s)) {
				p.Roles = append(p.Roles, Role(s))
			}
		}
	case string:
		for _, s := range strings.Fields(strings.ReplaceAll(v, ",", " ")) {
			if ValidRole(Role(s)) {
				p.Roles = append(p.Roles, Role(s))
			}
		}
	}
	if raw, ok := claims[o.cfg.ScopeClaim]; ok && raw != nil {
		b, err := json.Marshal(raw)
		if err != nil {
			return Principal{}, false, fmt.Errorf("scope claim: %w", err)
		}
		var sc Scope
		if err := json.Unmarshal(b, &sc); err != nil {
			return Principal{}, false, fmt.Errorf("scope claim: %w", err)
		}
		if err := sc.Validate(); err != nil {
			return Principal{}, false, err
		}
		p.Scope = &sc
	}
	return p, true, nil
}

func (o *OIDC) keyFunc(ctx context.Context) jwt.Keyfunc {
	return func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		if k := o.key(kid); k != nil {
			return k, nil
		}
		// Unknown kid: the provider may have rotated keys; refetch (rate-limited).
		if err := o.refresh(ctx, true); err != nil {
			return nil, err
		}
		if k := o.key(kid); k != nil {
			return k, nil
		}
		return nil, fmt.Errorf("oidc: unknown key id %q", kid)
	}
}

func (o *OIDC) key(kid string) crypto.PublicKey {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.keys == nil || time.Since(o.fetched) > o.cfg.RefreshEvery {
		return nil
	}
	if kid == "" && len(o.keys) == 1 {
		for _, k := range o.keys {
			return k
		}
	}
	return o.keys[kid]
}

func (o *OIDC) refresh(ctx context.Context, force bool) error {
	o.mu.Lock()
	if force && time.Since(o.fetched) < 10*time.Second && o.keys != nil {
		o.mu.Unlock()
		return nil
	}
	o.mu.Unlock()
	url := o.cfg.JWKSURL
	if url == "" {
		var disc struct {
			JWKSURI string `json:"jwks_uri"`
			Issuer  string `json:"issuer"`
		}
		if err := o.getJSON(ctx, strings.TrimRight(o.cfg.Issuer, "/")+"/.well-known/openid-configuration", &disc); err != nil {
			return err
		}
		if disc.Issuer != o.cfg.Issuer {
			return fmt.Errorf("oidc: discovery issuer %q does not match %q", disc.Issuer, o.cfg.Issuer)
		}
		url = disc.JWKSURI
	}
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := o.getJSON(ctx, url, &set); err != nil {
		return err
	}
	keys := map[string]crypto.PublicKey{}
	for _, k := range set.Keys {
		if k.Use != "" && k.Use != "sig" {
			continue
		}
		if pk, err := k.public(); err == nil {
			keys[k.Kid] = pk
		}
	}
	if len(keys) == 0 {
		return errors.New("oidc: JWKS has no usable signing keys")
	}
	o.mu.Lock()
	o.keys, o.fetched = keys, time.Now()
	o.mu.Unlock()
	return nil
}

func (o *OIDC) getJSON(ctx context.Context, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := o.cfg.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("oidc: GET %s: HTTP %d", url, resp.StatusCode)
	}
	return json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 1<<20)).Decode(v)
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func (k jwk) public() (crypto.PublicKey, error) {
	dec := base64.RawURLEncoding.DecodeString
	switch k.Kty {
	case "RSA":
		n, err := dec(k.N)
		if err != nil {
			return nil, err
		}
		e, err := dec(k.E)
		if err != nil {
			return nil, err
		}
		pk := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		if pk.N.BitLen() < 2048 {
			return nil, errors.New("rsa key too small")
		}
		return pk, nil
	case "EC":
		if !slices.Contains([]string{"P-256"}, k.Crv) {
			return nil, fmt.Errorf("unsupported curve %s", k.Crv)
		}
		x, err := dec(k.X)
		if err != nil {
			return nil, err
		}
		y, err := dec(k.Y)
		if err != nil {
			return nil, err
		}
		return &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(x), Y: new(big.Int).SetBytes(y)}, nil
	}
	return nil, fmt.Errorf("unsupported kty %s", k.Kty)
}
