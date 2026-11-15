package api

import "net/http"

// AuthConfig is the public login metadata a desktop client needs before it
// can start an OIDC Authorization Code + PKCE flow.
type AuthConfig struct {
	Mode     string   `json:"mode"`
	Issuer   string   `json:"issuer,omitempty"`
	ClientID string   `json:"client_id,omitempty"`
	Audience string   `json:"audience,omitempty"`
	Scopes   []string `json:"scopes,omitempty"`
}

func (s *Server) authConfig(w http.ResponseWriter, _ *http.Request) {
	cfg := s.AuthConfig
	if cfg == nil || cfg.Mode == "" {
		cfg = &AuthConfig{Mode: "token"}
	}
	writeJSON(w, http.StatusOK, cfg)
}
