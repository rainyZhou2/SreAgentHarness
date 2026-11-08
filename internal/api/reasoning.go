package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/reasoning"
	"go-sre-harness/internal/reasoning/improve"
	"go-sre-harness/internal/store/pg"
)

// ReasoningStore backs the /v1/reasoning routes.
type ReasoningStore interface {
	improve.Store
	ReasoningScore(ctx context.Context, tenantID, runID string) (reasoning.RunScore, error)
	ListMemories(ctx context.Context, tenantID string, f pg.MemoryFilter) ([]memory.Memory, error)
}

// ReasoningAPI exposes reasoning-quality metrics, the reasoning-pattern
// library with measured effectiveness, and an on-demand improvement tick.
type ReasoningAPI struct {
	Store     ReasoningStore
	Evolution improve.Evolution
	Server    *Server
	// Defaults are applied to POST /v1/reasoning/improve bodies.
	Defaults improve.Config
}

func (a *ReasoningAPI) RouteRoles() map[string]auth.Role {
	return map[string]auth.Role{
		"POST /v1/reasoning/improve":                        auth.Admin,
		"POST /v1/reasoning/patterns/{memory_id}/exclude":   auth.Operator,
		"POST /v1/reasoning/patterns/{memory_id}/reinstate": auth.Operator,
	}
}

func (a *ReasoningAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/reasoning/summary", a.summary)
	mux.HandleFunc("GET /v1/reasoning/runs/{run_id}", a.run)
	mux.HandleFunc("GET /v1/reasoning/patterns", a.patterns)
	mux.HandleFunc("POST /v1/reasoning/improve", a.improve)
	mux.HandleFunc("POST /v1/reasoning/patterns/{memory_id}/exclude", a.exclusion(true))
	mux.HandleFunc("POST /v1/reasoning/patterns/{memory_id}/reinstate", a.exclusion(false))
}

func (a *ReasoningAPI) filter(w http.ResponseWriter, r *http.Request, defaultSince time.Duration) (reasoning.ScoreFilter, bool) {
	q := r.URL.Query()
	f := reasoning.ScoreFilter{ProfileID: q.Get("profile_id"), Service: q.Get("service"), Limit: 10000,
		Since: time.Now().Add(-defaultSince)}
	if v := q.Get("version"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "invalid_query", "version must be a non-negative integer")
			return f, false
		}
		f.Version = n
	}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_query", "since must be RFC 3339")
			return f, false
		}
		f.Since = t
	}
	return f, true
}

func (a *ReasoningAPI) summary(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	f, ok := a.filter(w, r, 7*24*time.Hour)
	if !ok {
		return
	}
	scores, err := a.Store.ReasoningScores(r.Context(), id.tenant, f)
	if err != nil {
		a.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"since": f.Since, "profile_id": f.ProfileID, "version": f.Version,
		"service": f.Service, "summary": reasoning.Summarize(reasoning.MetricsOf(scores))})
}

func (a *ReasoningAPI) run(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	sc, err := a.Store.ReasoningScore(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		a.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

type patternView struct {
	memory.Memory
	Stats *reasoning.PatternStat `json:"stats,omitempty"`
	// Excluded reports whether the profile's active version hides it.
	Excluded bool `json:"excluded"`
}

func (a *ReasoningAPI) patterns(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	f, ok := a.filter(w, r, 30*24*time.Hour)
	if !ok {
		return
	}
	mf := pg.MemoryFilter{Scope: "service", ScopeID: f.Service, Status: r.URL.Query().Get("status"), Limit: 500}
	ms, err := a.Store.ListMemories(r.Context(), id.tenant, mf)
	if err != nil {
		a.Server.domainErr(w, err)
		return
	}
	scores, err := a.Store.ReasoningScores(r.Context(), id.tenant, f)
	if err != nil {
		a.Server.domainErr(w, err)
		return
	}
	stats := reasoning.PatternStats(scores)
	excluded := map[string]bool{}
	probe := 0.0
	profileID := f.ProfileID
	if profileID == "" {
		profileID = a.Defaults.ProfileID
	}
	if profileID != "" {
		if v, err := a.Store.ActiveVersion(r.Context(), id.tenant, profileID); err == nil && v > 0 {
			if raw, err := a.Store.ProfileSpec(r.Context(), id.tenant, profileID, v); err == nil {
				if spec, err := profile.Parse(raw); err == nil && spec.Reasoning != nil {
					for _, x := range spec.Reasoning.PatternsExcluded {
						excluded[x] = true
					}
					probe = spec.Reasoning.PatternProbeRate
				}
			}
		}
	}
	out := []patternView{}
	for _, m := range ms {
		if !reasoning.IsPattern(m) {
			continue
		}
		v := patternView{Memory: m, Excluded: excluded[m.ID]}
		if st, ok := stats[m.ID]; ok {
			v.Stats = &st
		}
		out = append(out, v)
	}
	writeJSON(w, http.StatusOK, map[string]any{"since": f.Since, "profile_id": profileID, "pattern_probe_rate": probe,
		"patterns": out})
}

func (a *ReasoningAPI) improve(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	if a.Evolution == nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "the evaluation service is not enabled")
		return
	}
	var b struct {
		ProfileID  string   `json:"profile_id"`
		DatasetID  string   `json:"dataset_id"`
		Window     string   `json:"window"`
		MinRuns    int      `json:"min_runs"`
		MinUses    int      `json:"min_uses"`
		RetireZ    float64  `json:"retire_z"`
		MinLift    float64  `json:"min_lift"`
		ReinstateZ *float64 `json:"reinstate_z"`
		ProbeRate  float64  `json:"probe_rate"`
	}
	if !decodeBody(w, r, &b) {
		return
	}
	cfg := a.Defaults
	cfg.Tenants = []string{id.tenant}
	if b.ProfileID != "" {
		cfg.ProfileID = b.ProfileID
	}
	if b.DatasetID != "" {
		cfg.DatasetID = b.DatasetID
	}
	if b.Window != "" {
		d, err := time.ParseDuration(b.Window)
		if err != nil || d <= 0 {
			writeErr(w, http.StatusBadRequest, "invalid_body", "window must be a positive duration")
			return
		}
		cfg.Window = d
	}
	if b.MinRuns > 0 {
		cfg.MinRuns = b.MinRuns
	}
	if b.MinUses > 0 {
		cfg.MinUses = b.MinUses
	}
	if b.RetireZ > 0 {
		cfg.RetireZ = b.RetireZ
	}
	if b.MinLift > 0 {
		cfg.MinLift = b.MinLift
	}
	if b.ReinstateZ != nil {
		cfg.ReinstateZ = *b.ReinstateZ
	}
	if b.ProbeRate > 0 {
		cfg.ProbeRate = b.ProbeRate
	}
	if err := cfg.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	im := &improve.Improver{Config: cfg, Store: a.Store, Evolution: a.Evolution}
	o := im.TickTenant(r.Context(), id.tenant)
	resID := ""
	if o.Candidate != nil {
		resID = o.Candidate.ID
	}
	a.Server.audit(r.Context(), pg.AuditEntry{TenantID: id.tenant, ActorType: "user", ActorID: id.actor,
		Action: "reasoning.improve", ResourceType: "profile_candidate", ResourceID: resID, Outcome: o.Status,
		Details: map[string]any{"profile_id": cfg.ProfileID, "reason": o.Reason}})
	writeJSON(w, http.StatusOK, o)
}

// exclusion proposes a candidate that excludes or reinstates one pattern.
// The caller becomes the proposer, so a different human must approve it.
func (a *ReasoningAPI) exclusion(exclude bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := ident(w, r)
		if !ok {
			return
		}
		if a.Evolution == nil {
			writeErr(w, http.StatusServiceUnavailable, "unavailable", "the evaluation service is not enabled")
			return
		}
		var b struct {
			ProfileID string `json:"profile_id"`
			Note      string `json:"note"`
		}
		if !decodeBody(w, r, &b) {
			return
		}
		if b.ProfileID == "" {
			b.ProfileID = a.Defaults.ProfileID
		}
		if b.ProfileID == "" {
			writeErr(w, http.StatusBadRequest, "invalid_body", "profile_id is required")
			return
		}
		if len([]rune(b.Note)) > 1000 {
			writeErr(w, http.StatusBadRequest, "invalid_body", "note is limited to 1000 characters")
			return
		}
		memID := r.PathValue("memory_id")
		var add, remove []string
		action := "reasoning.pattern.exclude"
		if exclude {
			add = []string{memID}
		} else {
			remove, action = []string{memID}, "reasoning.pattern.reinstate"
		}
		c, err := improve.ProposeExclusionChange(r.Context(), a.Store, a.Evolution, id.tenant, b.ProfileID, add, remove, id.actor, b.Note)
		outcome := "applied"
		if err != nil {
			outcome = "rejected"
		}
		a.Server.audit(r.Context(), pg.AuditEntry{TenantID: id.tenant, ActorType: "user", ActorID: id.actor, Action: action,
			ResourceType: "memory", ResourceID: memID, Outcome: outcome,
			Details: map[string]any{"profile_id": b.ProfileID, "candidate_id": c.ID}})
		if err != nil {
			if errors.Is(err, eval.ErrInvalid) {
				writeErr(w, http.StatusUnprocessableEntity, "invalid_request", err.Error())
				return
			}
			a.Server.domainErr(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, c)
	}
}
