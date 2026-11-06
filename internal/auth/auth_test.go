package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func req(h map[string]string) *http.Request {
	r := httptest.NewRequest("GET", "/v1/runs/x", nil)
	for k, v := range h {
		r.Header.Set(k, v)
	}
	return r
}

func TestRoles(t *testing.T) {
	op := Principal{Roles: []Role{Operator}}
	if !op.Has(Viewer) || op.Has(Approver) || op.Has(Admin) {
		t.Fatal("operator implies viewer only")
	}
	if ad := (Principal{Roles: []Role{Admin}}); !ad.Has(Approver) || !ad.Has(Operator) {
		t.Fatal("admin implies all")
	}
}

func TestStaticTokens(t *testing.T) {
	st, err := NewStaticTokens([]StaticToken{{SHA256: HashToken("s3cret"), Tenant: "tenant-a", Subject: "ci", Roles: []Role{Operator}}})
	if err != nil {
		t.Fatal(err)
	}
	c := &Chain{Authenticators: []Authenticator{st}}
	p, err := c.Authenticate(req(map[string]string{"Authorization": "Bearer s3cret"}))
	if err != nil || p.Tenant != "tenant-a" || p.Subject != "ci" || !p.Has(Operator) {
		t.Fatalf("%+v %v", p, err)
	}
	if _, err := c.Authenticate(req(map[string]string{"Authorization": "Bearer nope"})); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("bad token: %v", err)
	}
	if _, err := c.Authenticate(req(map[string]string{"Authorization": "Bearer s3cret", "X-Tenant-ID": "tenant-b"})); !errors.Is(err, ErrForbidden) {
		t.Fatalf("cross-tenant header: %v", err)
	}
	if _, err := c.Authenticate(req(map[string]string{"X-Tenant-ID": "tenant-a", "X-Actor-ID": "eve"})); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("dev headers must not authenticate when dev mode is off: %v", err)
	}
}

func TestDevHeaders(t *testing.T) {
	c := &Chain{Authenticators: []Authenticator{DevHeaders{}}}
	p, err := c.Authenticate(req(map[string]string{"X-Tenant-ID": "tenant-a", "X-Actor-ID": "alice", "X-Roles": "viewer"}))
	if err != nil || p.Has(Operator) || !p.Has(Viewer) {
		t.Fatalf("%+v %v", p, err)
	}
}

func TestOIDC(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": srv.URL, "jwks_uri": srv.URL + "/jwks"})
		case "/jwks":
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{"kty": "RSA", "kid": "k1", "use": "sig",
				"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
		}
	}))
	defer srv.Close()
	o, err := NewOIDC(OIDCConfig{Issuer: srv.URL, Audience: "sre-harness"})
	if err != nil {
		t.Fatal(err)
	}
	sign := func(claims jwt.MapClaims) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		tok.Header["kid"] = "k1"
		s, _ := tok.SignedString(key)
		return s
	}
	c := &Chain{Authenticators: []Authenticator{o}}
	good := sign(jwt.MapClaims{"iss": srv.URL, "aud": "sre-harness", "sub": "alice", "tenant_id": "tenant-a",
		"roles": []string{"approver"}, "exp": time.Now().Add(time.Hour).Unix()})
	p, err := c.Authenticate(req(map[string]string{"Authorization": "Bearer " + good}))
	if err != nil || p.Subject != "alice" || p.Tenant != "tenant-a" || !p.Has(Approver) || p.Method != "oidc" {
		t.Fatalf("%+v %v", p, err)
	}
	for name, claims := range map[string]jwt.MapClaims{
		"wrong audience": {"iss": srv.URL, "aud": "other", "sub": "a", "tenant_id": "t", "exp": time.Now().Add(time.Hour).Unix()},
		"expired":        {"iss": srv.URL, "aud": "sre-harness", "sub": "a", "tenant_id": "t", "exp": time.Now().Add(-time.Hour).Unix()},
		"wrong issuer":   {"iss": "https://evil", "aud": "sre-harness", "sub": "a", "tenant_id": "t", "exp": time.Now().Add(time.Hour).Unix()},
		"no tenant":      {"iss": srv.URL, "aud": "sre-harness", "sub": "a", "exp": time.Now().Add(time.Hour).Unix()},
	} {
		if _, err := c.Authenticate(req(map[string]string{"Authorization": "Bearer " + sign(claims)})); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	forged := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"iss": srv.URL, "aud": "sre-harness", "sub": "a",
		"tenant_id": "t", "exp": time.Now().Add(time.Hour).Unix()})
	forged.Header["kid"] = "k1"
	fs, _ := forged.SignedString(other)
	if _, err := c.Authenticate(req(map[string]string{"Authorization": "Bearer " + fs})); err == nil {
		t.Fatal("forged signature accepted")
	}
	hs := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iss": srv.URL, "aud": "sre-harness", "sub": "a",
		"tenant_id": "t", "exp": time.Now().Add(time.Hour).Unix()})
	hss, _ := hs.SignedString([]byte("k"))
	if _, err := c.Authenticate(req(map[string]string{"Authorization": "Bearer " + hss})); err == nil {
		t.Fatal("HS256 (alg confusion) accepted")
	}
}
