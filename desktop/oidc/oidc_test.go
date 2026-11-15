package oidc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

func idToken(t *testing.T, nonce string) string {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"sub": "alice", "nonce": nonce})
	if err != nil {
		t.Fatal(err)
	}
	return "e30." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

func TestAuthorizePKCE(t *testing.T) {
	var mu sync.Mutex
	var nonce string
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(Discovery{
			Issuer:                "http://" + r.Host,
			AuthorizationEndpoint: "http://" + r.Host + "/authorize",
			TokenEndpoint:         "http://" + r.Host + "/token",
		})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		defer mu.Unlock()
		if r.Form.Get("grant_type") != "authorization_code" {
			http.Error(w, "grant", http.StatusBadRequest)
			return
		}
		if r.Form.Get("code") != "code-1" || r.Form.Get("code_verifier") == "" {
			http.Error(w, "code or verifier", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(tokenResponse{
			AccessToken: "access-1", RefreshToken: "refresh-1", IDToken: idToken(t, nonce), ExpiresIn: 60,
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := New(Config{
		Issuer: server.URL, ClientID: "desktop", HTTP: server.Client(),
		OpenURL: func(raw string) error {
			u, err := url.Parse(raw)
			if err != nil {
				return err
			}
			mu.Lock()
			nonce = u.Query().Get("nonce")
			mu.Unlock()
			redirect := u.Query().Get("redirect_uri")
			state := u.Query().Get("state")
			go func() {
				resp, err := http.Get(redirect + "?code=code-1&state=" + url.QueryEscape(state))
				if err == nil {
					resp.Body.Close()
				}
			}()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	tokens, err := client.Authorize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tokens.AccessToken != "access-1" || tokens.RefreshToken != "refresh-1" || tokens.Subject != "alice" {
		t.Fatalf("tokens = %+v", tokens)
	}
}

func TestRefresh(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(Discovery{
			Issuer:                "http://" + r.Host,
			AuthorizationEndpoint: "http://" + r.Host + "/authorize",
			TokenEndpoint:         "http://" + r.Host + "/token",
		})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if !strings.EqualFold(r.Form.Get("grant_type"), "refresh_token") || r.Form.Get("refresh_token") != "refresh-1" {
			http.Error(w, "bad refresh", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "access-2", RefreshToken: "refresh-2", ExpiresIn: 60})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := New(Config{Issuer: server.URL, ClientID: "desktop", HTTP: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := client.Refresh(context.Background(), "refresh-1")
	if err != nil {
		t.Fatal(err)
	}
	if tokens.AccessToken != "access-2" || tokens.RefreshToken != "refresh-2" {
		t.Fatalf("tokens = %+v", tokens)
	}
}
