package workapi

import (
	"net/http"
	"strings"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/observability"
)

// ---- observability ----

func (a *API) brokerEnabled(w http.ResponseWriter) bool {
	if a.Broker == nil {
		writeErr(w, http.StatusServiceUnavailable, "disabled", "the observability broker is not configured")
		return false
	}
	return true
}

func (a *API) listSources(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	srcs, err := a.Store.ListSources(r.Context(), p.Tenant, observability.Filter{Kind: observability.Signal(q.Get("kind")),
		Environment: q.Get("environment")})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if id := q.Get("revisions_of"); id != "" {
		revs, err := a.Store.SourceRevisions(r.Context(), p.Tenant, id, 100)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": srcs, "revisions": revs})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": srcs})
}

// putSource creates (no If-Match) or updates (If-Match: version) a source.
// The body names an endpoint_ref and a credential_id the server must know;
// it cannot carry a URL or a secret, and its tenant is the caller's.
func (a *API) putSource(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !a.brokerEnabled(w) {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	var req struct {
		observability.Source
		Reason string `json:"reason,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	src := req.Source
	src.TenantID = p.Tenant
	if _, known := a.Broker.Endpoints[src.EndpointRef]; !known && src.Backend != "ledger" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "endpoint_ref is not an endpoint configured on this server")
		return
	}
	if src.CredentialID != "" {
		if _, err := a.Broker.Credentials.Resolve(r.Context(), p.Tenant, src.CredentialID); err != nil {
			// Say that it is unusable, not why in detail: the reply must not
			// become an oracle for which credentials exist.
			writeErr(w, http.StatusBadRequest, "invalid_request", "credential_id is not usable by this tenant on this server")
			return
		}
	}
	out, err := a.Store.PutSource(r.Context(), src, version, p.Subject, req.Reason)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.audit(r.Context(), p, "observability.source.put", "source", out.ID, "applied", map[string]any{"version": out.Version,
		"state": out.State, "backend": out.Backend, "endpoint_ref": out.EndpointRef, "reason": req.Reason})
	etag(w, out.Version)
	writeJSON(w, http.StatusOK, out)
}

func (a *API) rollbackSource(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	var req struct {
		ToVersion int64  `json:"to_version"`
		Reason    string `json:"reason,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	out, err := observability.Rollback(r.Context(), a.Store, p.Tenant, r.PathValue("id"), req.ToVersion, p.Subject, req.Reason)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.audit(r.Context(), p, "observability.source.rollback", "source", out.ID, "applied", map[string]any{"to_version": req.ToVersion,
		"version": out.Version})
	etag(w, out.Version)
	writeJSON(w, http.StatusOK, out)
}

func (a *API) probeSource(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !a.brokerEnabled(w) {
		return
	}
	h, err := a.Broker.Probe(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, h)
}

func (a *API) obsQuery(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !a.brokerEnabled(w) {
		return
	}
	var req struct {
		observability.Query
		WorkID string `json:"work_id,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.SubjectRef != "" && !p.AllowsResource(req.SubjectRef) {
		a.outOfScope(w, r, p, "observability.query", "subject", req.SubjectRef, "subject is outside the caller's scope")
		return
	}
	c := observability.Caller{RC: cpkit.RequestContext{TenantID: p.Tenant, PrincipalID: p.Subject, PrincipalType: "user",
		DataClass: "confidential"}, Expert: p.Has(auth.Admin)}
	if req.WorkID != "" {
		wk, err := a.Store.GetWork(r.Context(), p.Tenant, req.WorkID)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		c.RC.WorkID, c.Subjects = wk.ID, wk.Goal.SubjectRefs
	}
	res := a.Broker.Query(r.Context(), c, req.Query)
	out := map[string]any{"result": res}
	if c.RC.WorkID != "" {
		if rec, ok := observability.EvidenceOf(p.Tenant, c.RC.WorkID, res, ""); ok {
			if stored, _, err := a.Store.AddEvidence(r.Context(), rec); err == nil {
				out["evidence_ref"] = "evidence:" + stored.ID
			}
		}
	}
	if res.Status == observability.StatusDenied {
		a.audit(r.Context(), p, "observability.query", "source", req.SourceID, "denied", map[string]any{"reason": strings.Join(res.Warnings, "; ")})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) obsEvidence(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	rec, err := a.Store.GetEvidence(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}
