// Package contextproviders registers the built-in context_provider plugins.
// Each one contributes environment facts (topology, ownership, on-call,
// recent changes, ...) that the kernel gathers once per run and pins as
// untrusted data. They are generic mechanisms: what a fact says comes from
// configuration or from the system the provider reads.
package contextproviders

import (
	"context"
	"time"

	"go-sre-harness/internal/envctx"
	"go-sre-harness/internal/plugin"
)

// timeout bounds a single provider request; the gatherer applies its own
// per-provider deadline on top.
const timeout = 5 * time.Second

func init() {
	register("static", "operator-declared facts (ownership, runbooks, maintenance windows) filtered by service glob")
	register("http", "facts from an HTTP endpoint (CMDB, on-call, change calendar); URL and headers are templates over the run scope")
	register("tool", "facts from a read-only (R0) registered tool, e.g. a deploy history or an MCP query")
	register("dependencies", "service dependencies from the memory graph")
}

func register(kind, doc string) {
	plugin.Register(plugin.ContextProvider, kind, doc,
		func(_ context.Context, h *plugin.Host, c plugin.Config) (envctx.Provider, error) {
			var cfg envctx.Config
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			cfg.Type = kind
			d := envctx.Deps{Registry: h.Tools, Network: h.Network, Timeout: timeout}
			if h.Store != nil {
				d.Dependencies = h.Store
			}
			return envctx.Build(cfg, d)
		})
}
