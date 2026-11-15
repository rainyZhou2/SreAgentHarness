package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthConfig(t *testing.T) {
	s := &Server{AuthConfig: &AuthConfig{
		Mode: "oidc", Issuer: "https://idp.example.com", ClientID: "desktop",
		Scopes: []string{"openid", "offline_access"},
	}}
	rec := httptest.NewRecorder()
	s.authConfig(rec, httptest.NewRequest(http.MethodGet, "/api/auth/config", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got AuthConfig
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Mode != "oidc" || got.Issuer != "https://idp.example.com" || got.ClientID != "desktop" {
		t.Fatalf("config = %+v", got)
	}
}

func TestAuthConfigDefaultsToToken(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.authConfig(rec, httptest.NewRequest(http.MethodGet, "/api/auth/config", nil))
	var got AuthConfig
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Mode != "token" {
		t.Fatalf("mode = %q", got.Mode)
	}
}
