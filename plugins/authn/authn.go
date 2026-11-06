// Package authn registers the built-in authenticator plugins.
package authn

import (
	"context"
	"errors"
	"fmt"
	"net"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/plugin"
)

type DevConfig struct {
	// AllowNonLoopback permits header auth on a non-loopback listener
	// (isolated test environments only).
	AllowNonLoopback bool `json:"allow_non_loopback,omitempty"`
}

type TokenConfig struct {
	TokensFile string             `json:"tokens_file,omitempty"`
	Tokens     []auth.StaticToken `json:"tokens,omitempty"`
}

func init() {
	plugin.Register(plugin.Authenticator, "dev", "trusts X-Tenant-ID / X-Actor-ID / X-Roles headers (development only)",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (auth.Authenticator, error) {
			var cfg DevConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			host, _, _ := net.SplitHostPort(h.HTTPAddr)
			ip := net.ParseIP(host)
			loopback := host == "localhost" || (ip != nil && ip.IsLoopback())
			if !loopback && !cfg.AllowNonLoopback {
				return nil, fmt.Errorf("dev header auth on non-loopback address %s; use the token or oidc authenticator "+
					"(or allow_non_loopback for an isolated test environment)", h.HTTPAddr)
			}
			h.Log.Warn("DEV AUTH MODE: X-Tenant-ID / X-Actor-ID headers are trusted; do not expose this server")
			return auth.DevHeaders{}, nil
		})
	plugin.Register(plugin.Authenticator, "token", "static bearer tokens (SHA-256 hashed)",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (auth.Authenticator, error) {
			var cfg TokenConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			switch {
			case cfg.TokensFile != "" && len(cfg.Tokens) > 0:
				return nil, errors.New("set tokens_file or tokens, not both")
			case cfg.TokensFile != "":
				return auth.LoadStaticTokens(cfg.TokensFile)
			case len(cfg.Tokens) > 0:
				return auth.NewStaticTokens(cfg.Tokens)
			}
			return nil, errors.New("tokens_file or tokens is required")
		})
	plugin.Register(plugin.Authenticator, "oidc", "OIDC/JWT bearer tokens (RS256/ES256, JWKS)",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (auth.Authenticator, error) {
			var cfg auth.OIDCConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return auth.NewOIDC(cfg)
		})
}
