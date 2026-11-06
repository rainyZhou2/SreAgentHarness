package extensions

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/tools"
)

// API serves /v1/extensions and /v1/tools. Reads need viewer; changes and
// probes (which open outbound connections) need admin and are audited.
type API struct {
	Runtime  *Runtime
	Registry *tools.Registry
	// DefaultRules returns the rules of the tenant's default profile and
	// its reference, to report which tools new runs may use (optional).
	DefaultRules func(ctx context.Context, tenantID string) (profile.Rules, string, error)
	// ResolveModel reports the endpoint new runs of the tenant use for the
	// default alias (optional).
	ResolveModel func(tenantID string) (endpointID, modelID string, err error)
	// Audit records changes (optional).
	Audit func(ctx context.Context, tenant, actor, action, resource string, details map[string]any)
}

var pathKinds = map[string]Kind{"mcp-servers": KindMCP, "http-tools": KindHTTP, "skills": KindSkill, "models": KindModel}

func (a *API) RouteRoles() map[string]auth.Role {
	return map[string]auth.Role{
		"GET /v1/extensions":                           auth.Viewer,
		"GET /v1/tools":                                auth.Viewer,
		"PUT /v1/extensions/{kind}/{name}":             auth.Admin,
		"DELETE /v1/extensions/{kind}/{name}":          auth.Admin,
		"POST /v1/extensions/mcp-servers/{name}/probe": auth.Admin,
		"POST /v1/extensions/models/{name}/test":       auth.Admin,
		"POST /v1/extensions/models/{name}/list":       auth.Admin,
	}
}

func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/extensions", a.list)
	mux.HandleFunc("GET /v1/tools", a.listTools)
	mux.HandleFunc("PUT /v1/extensions/{kind}/{name}", a.put)
	mux.HandleFunc("DELETE /v1/extensions/{kind}/{name}", a.del)
	mux.HandleFunc("POST /v1/extensions/mcp-servers/{name}/probe", a.probe)
	mux.HandleFunc("POST /v1/extensions/models/{name}/test", a.testModel)
	mux.HandleFunc("POST /v1/extensions/models/{name}/list", a.listModels)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, map[string]string{"error": kind, "message": msg})
}

func principal(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	p, ok := auth.FromContext(r.Context())
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthenticated", "authentication required")
	}
	return p, ok
}

// View is a record as returned by the API (secrets masked).
type View struct {
	Kind      Kind            `json:"kind"`
	Name      string          `json:"name"`
	Enabled   bool            `json:"enabled"`
	Version   int64           `json:"version"`
	Spec      json.RawMessage `json:"spec"`
	UpdatedBy string          `json:"updated_by"`
	UpdatedAt string          `json:"updated_at"`
	Status    *Status         `json:"status,omitempty"`
}

func (a *API) view(rec Record) View {
	v := View{Kind: rec.Kind, Name: rec.Name, Enabled: rec.Enabled, Version: rec.Version,
		Spec: MaskSecrets(rec.Kind, rec.Spec), UpdatedBy: rec.UpdatedBy, UpdatedAt: rec.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z")}
	if st, ok := a.Runtime.StatusOf(rec.TenantID, rec.Kind, rec.Name); ok && st.Version == rec.Version {
		v.Status = &st
	}
	return v
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	recs, err := a.Runtime.Store.ListExtensions(r.Context(), p.Tenant)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "cannot list extensions")
		return
	}
	out := map[string]any{}
	lists := map[Kind][]View{KindMCP: {}, KindHTTP: {}, KindSkill: {}, KindModel: {}}
	for _, rec := range recs {
		if _, ok := lists[rec.Kind]; ok {
			lists[rec.Kind] = append(lists[rec.Kind], a.view(rec))
		}
	}
	out["mcp_servers"], out["http_tools"], out["skills"], out["models"] = lists[KindMCP], lists[KindHTTP], lists[KindSkill], lists[KindModel]
	if a.ResolveModel != nil {
		ep, model, err := a.ResolveModel(p.Tenant)
		cur := map[string]any{"endpoint": ep, "model": model, "tenant_configured": strings.HasPrefix(ep, "tenant:")}
		if err != nil {
			cur["error"] = err.Error()
		}
		out["current_model"] = cur
	}
	writeJSON(w, http.StatusOK, out)
}

type putBody struct {
	Spec            json.RawMessage `json:"spec"`
	Enabled         *bool           `json:"enabled"`
	ExpectedVersion *int64          `json:"expected_version"`
}

func (a *API) put(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	kind, ok := pathKinds[r.PathValue("kind")]
	if !ok {
		writeErr(w, http.StatusNotFound, "not_found", "kind must be mcp-servers, http-tools, skills or models")
		return
	}
	name := r.PathValue("name")
	var body putBody
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil || len(body.Spec) == 0 {
		writeErr(w, http.StatusBadRequest, "invalid_body", `body must be {"spec": {...}, "enabled"?, "expected_version"?}`)
		return
	}
	// Blank secret values keep the stored ones (the API never returns them).
	if existing, err := a.find(r.Context(), p.Tenant, kind, name); err == nil {
		if body.Spec, err = MergeSecrets(existing.Spec, body.Spec); err != nil {
			writeErr(w, http.StatusUnprocessableEntity, "secrets_required", err.Error())
			return
		}
	}
	spec, err := Validate(kind, name, body.Spec, a.Runtime.Options)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_spec", err.Error())
		return
	}
	enabled := body.Enabled == nil || *body.Enabled
	rec, err := a.Runtime.Store.UpsertExtension(r.Context(), Record{TenantID: p.Tenant, Kind: kind, Name: name, Spec: spec,
		Enabled: enabled, UpdatedBy: p.Subject}, body.ExpectedVersion)
	if errors.Is(err, ErrConflict) {
		writeErr(w, http.StatusConflict, "version_conflict", "the extension changed; reload and retry")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "cannot save extension")
		return
	}
	a.audit(r.Context(), p, "extension.put", kind, name, map[string]any{"version": rec.Version, "enabled": enabled})
	// Apply now in this process so the response reflects the new state;
	// other processes converge within one sync interval.
	a.syncNow()
	writeJSON(w, http.StatusOK, a.view(rec))
}

func (a *API) del(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	kind, ok := pathKinds[r.PathValue("kind")]
	if !ok {
		writeErr(w, http.StatusNotFound, "not_found", "kind must be mcp-servers, http-tools, skills or models")
		return
	}
	name := r.PathValue("name")
	if err := a.Runtime.Store.DeleteExtension(r.Context(), p.Tenant, kind, name); errors.Is(err, ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "extension not found")
		return
	} else if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "cannot delete extension")
		return
	}
	a.audit(r.Context(), p, "extension.delete", kind, name, nil)
	a.syncNow()
	w.WriteHeader(http.StatusNoContent)
}

// syncNow applies changes in this process. It must not use the request
// context: that one is tenant scoped (row level security), and the sync
// reads every tenant's records.
func (a *API) syncNow() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Runtime.Sync(ctx); err != nil {
		a.Runtime.Log.Warn("extensions sync after write failed", "err", err)
	}
}

func (a *API) find(ctx context.Context, tenant string, kind Kind, name string) (Record, error) {
	recs, err := a.Runtime.Store.ListExtensions(ctx, tenant)
	if err != nil {
		return Record{}, err
	}
	for _, rec := range recs {
		if rec.Kind == kind && rec.Name == name {
			return rec, nil
		}
	}
	return Record{}, ErrNotFound
}

// probe lists the tools an MCP server offers. The body may carry a spec
// (testing a server before saving it); otherwise the stored one is used.
func (a *API) probe(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	var body struct {
		Spec json.RawMessage `json:"spec"`
	}
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body)
	existing, findErr := a.find(r.Context(), p.Tenant, KindMCP, name)
	raw := body.Spec
	switch {
	case len(raw) == 0 && findErr != nil:
		writeErr(w, http.StatusNotFound, "not_found", "no stored server with this name; send a spec to probe")
		return
	case len(raw) == 0:
		raw = existing.Spec
	case findErr == nil:
		var err error
		if raw, err = MergeSecrets(existing.Spec, raw); err != nil {
			writeErr(w, http.StatusUnprocessableEntity, "secrets_required", err.Error())
			return
		}
	}
	norm, err := Validate(KindMCP, name, raw, a.Runtime.Options)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_spec", err.Error())
		return
	}
	var spec MCPSpec
	_ = json.Unmarshal(norm, &spec)
	a.audit(r.Context(), p, "extension.probe", KindMCP, name, map[string]any{"url": spec.URL, "command": spec.Command})
	list, err := a.Runtime.Probe(r.Context(), name, spec)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "probe_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tools": list})
}

// modelSpecFor reads a model spec from the body (merged with the stored
// secrets) or, without a body, the stored one. It writes the error itself.
//
// Listing needs no model yet: noModel fills a placeholder before validation.
func (a *API) modelSpecFor(w http.ResponseWriter, r *http.Request, p auth.Principal, name string, noModel bool) (ModelSpec, bool) {
	var body struct {
		Spec json.RawMessage `json:"spec"`
	}
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body)
	existing, findErr := a.find(r.Context(), p.Tenant, KindModel, name)
	raw := body.Spec
	switch {
	case len(raw) == 0 && findErr != nil:
		writeErr(w, http.StatusNotFound, "not_found", "no stored model with this name; send a spec")
		return ModelSpec{}, false
	case len(raw) == 0:
		raw = existing.Spec
	case findErr == nil:
		var err error
		if raw, err = MergeSecrets(existing.Spec, raw); err != nil {
			writeErr(w, http.StatusUnprocessableEntity, "secrets_required", err.Error())
			return ModelSpec{}, false
		}
	}
	if noModel {
		var m map[string]any
		if json.Unmarshal(raw, &m) == nil {
			if v, _ := m["model"].(string); strings.TrimSpace(v) == "" {
				m["model"] = "list"
				raw, _ = json.Marshal(m)
			}
		}
	}
	norm, err := Validate(KindModel, name, raw, a.Runtime.Options)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_spec", err.Error())
		return ModelSpec{}, false
	}
	var spec ModelSpec
	_ = json.Unmarshal(norm, &spec)
	return spec, true
}

// listModels returns the model IDs the provider offers for a spec's key.
func (a *API) listModels(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	spec, ok := a.modelSpecFor(w, r, p, name, true)
	if !ok {
		return
	}
	a.audit(r.Context(), p, "extension.list_models", KindModel, name, map[string]any{"provider": spec.Provider})
	ids, err := a.Runtime.ListModels(r.Context(), spec)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "list_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"models": ids})
}

// testModel sends a tiny request through a model (the stored one, or the
// spec in the body merged with the stored secrets) and returns the reply.
func (a *API) testModel(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	spec, ok := a.modelSpecFor(w, r, p, name, false)
	if !ok {
		return
	}
	a.audit(r.Context(), p, "extension.test", KindModel, name, map[string]any{"provider": spec.Provider, "model": spec.Model})
	start := time.Now()
	reply, model, err := a.Runtime.TestModel(r.Context(), p.Tenant, name, spec)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "model_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"reply": reply, "model": model, "latency_ms": time.Since(start).Milliseconds()})
}

// ToolView is one tool a tenant's runs can be offered.
type ToolView struct {
	tools.Descriptor
	AllowedByProfile bool `json:"allowed_by_profile"`
}

func (a *API) listTools(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	ds := a.Registry.DescriptorsFor(p.Tenant)
	rules, ref := profile.Unrestricted(), ""
	if a.DefaultRules != nil {
		if rr, rf, err := a.DefaultRules(r.Context(), p.Tenant); err == nil {
			rules, ref = rr, rf
		}
	}
	out := make([]ToolView, 0, len(ds))
	for _, d := range ds {
		eff := rules.Effective(d)
		out = append(out, ToolView{Descriptor: eff, AllowedByProfile: rules.AllowsTool(d.Name)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, map[string]any{"tools": out, "profile": ref})
}

func (a *API) audit(ctx context.Context, p auth.Principal, action string, kind Kind, name string, details map[string]any) {
	if a.Audit != nil {
		a.Audit(ctx, p.Tenant, p.Subject, action, string(kind)+"/"+name, details)
	}
}
