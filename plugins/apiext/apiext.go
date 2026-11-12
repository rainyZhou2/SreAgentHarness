// Package apiext registers the built-in API extension plugins.
package apiext

import (
	"context"
	"errors"

	"fmt"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/curate"
	curatewire "go-sre-harness/internal/curate/wire"
	"go-sre-harness/internal/extensions"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/reasoning/improve"
	"go-sre-harness/internal/runsearch"
	"go-sre-harness/internal/store/pg"
)

func init() {
	plugin.Register(plugin.API, "memory", "/v1/memories, feedback, experiences, incidents, service dependencies",
		func(_ context.Context, h *plugin.Host, _ plugin.Config) (api.Extension, error) {
			if h.Server == nil || h.Embedder == nil {
				return nil, errors.New("requires the API server and an embedder")
			}
			// Memory curation: proposals to set aside stale memory, with the
			// run record as ground truth. It only proposes; approvers decide.
			cur := &curate.Curator{Memories: curatewire.Memories(h.Store), Store: h.Store,
				History: &runsearch.Searcher{Runs: h.Store, Artifacts: h.Artifacts}}
			return &api.MemoryAPI{Store: h.Store, Writer: memory.Writer{Embedder: h.Embedder, Artifacts: h.Artifacts}, Server: h.Server, Curator: cur}, nil
		})
	plugin.Register(plugin.API, "eval", "/v1/eval, /v1/profiles, /v1/evolution (datasets, gates, canary rollouts)",
		func(_ context.Context, h *plugin.Host, _ plugin.Config) (api.Extension, error) {
			if h.Server == nil || h.Eval == nil {
				return nil, errors.New("requires the API server and the evaluation service")
			}
			return &api.EvalAPI{Service: h.Eval, Store: h.Store, Server: h.Server}, nil
		})
	plugin.Register(plugin.API, "reasoning", "/v1/reasoning (quality metrics, pattern library, on-demand improvement)",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (api.Extension, error) {
			if h.Server == nil {
				return nil, errors.New("requires the API server")
			}
			var def improve.Config
			if err := c.Decode(&def); err != nil {
				return nil, err
			}
			if def.ProfileID == "" {
				def.ProfileID = h.Server.DefaultProfile
			}
			a := &api.ReasoningAPI{Store: h.Store, Server: h.Server, Defaults: def}
			if h.Eval != nil {
				a.Evolution = h.Eval
			}
			return a, nil
		})
	plugin.Register(plugin.API, "extensions", "/v1/extensions (tenant MCP servers, HTTP tools, skills) and /v1/tools",
		func(_ context.Context, h *plugin.Host, _ plugin.Config) (api.Extension, error) {
			if h.Server == nil || h.Extensions == nil {
				return nil, errors.New("requires the API server and the extensions runtime")
			}
			srv := h.Server
			return &extensions.API{Runtime: h.Extensions, Registry: h.Tools, Toggles: h.Store,
				DefaultRules: func(ctx context.Context, tenant string) (profile.Rules, string, error) {
					ref := fmt.Sprintf("%s@%d", srv.DefaultProfile, srv.DefaultVersion)
					if srv.Rules == nil {
						return profile.Rules{}, ref, nil
					}
					r, err := srv.Rules(ctx, tenant, srv.DefaultProfile, srv.DefaultVersion)
					return r, ref, err
				},
				ResolveModel: func(tenant string) (string, string, error) {
					if h.Gateway == nil {
						return "", "", errors.New("no model gateway")
					}
					ep, err := h.Gateway.Resolve(gw.Request{ModelAlias: h.Settings.ModelAlias, NeedTools: true, NeedStreaming: true},
						gw.Scope{TenantID: tenant})
					return ep.ID, ep.ModelID, err
				},
				Audit: func(ctx context.Context, tenant, actor, action, resource string, details map[string]any) {
					_ = h.Store.Audit(ctx, pg.AuditEntry{TenantID: tenant, ActorType: "user", ActorID: actor, Action: action,
						ResourceType: "extension", ResourceID: resource, Outcome: "applied", Details: details})
				}}, nil
		})
}
