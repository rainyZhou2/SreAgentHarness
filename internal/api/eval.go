package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/store/pg"
)

type EvalStore interface {
	ListDatasets(ctx context.Context, tenantID string) ([]eval.Dataset, error)
	ListProfileVersions(ctx context.Context, tenantID, profileID string) ([]pg.ProfileVersion, error)
	ListCandidates(ctx context.Context, tenantID, profileID string) ([]eval.Candidate, error)
	ListRollouts(ctx context.Context, tenantID, profileID string) ([]eval.Rollout, error)
}

// EvalAPI exposes datasets, offline evaluation and profile evolution (§12, §32).
type EvalAPI struct {
	Service *eval.Service
	Store   EvalStore
	Server  *Server
}

func (e *EvalAPI) RouteRoles() map[string]auth.Role {
	return map[string]auth.Role{
		"POST /v1/eval/datasets":                                auth.Operator,
		"POST /v1/eval/datasets/{dataset_id}/cases":             auth.Operator,
		"POST /v1/eval/datasets/{dataset_id}/plan-cases":        auth.Operator,
		"POST /v1/eval/datasets/{dataset_id}/freeze":            auth.Operator,
		"POST /v1/eval/plan-runs":                               auth.Operator,
		"POST /v1/eval/runs":                                    auth.Operator,
		"POST /v1/evolution/candidates":                         auth.Operator,
		"POST /v1/evolution/candidates/{candidate_id}/evaluate": auth.Operator,
		"POST /v1/evolution/candidates/{candidate_id}/approve":  auth.Approver,
		"POST /v1/evolution/candidates/{candidate_id}/rollout":  auth.Admin,
		"POST /v1/evolution/rollouts/{rollout_id}/promote":      auth.Admin,
		"POST /v1/evolution/rollouts/{rollout_id}/rollback":     auth.Admin,
	}
}

func (e *EvalAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/eval/datasets", e.createDataset)
	mux.HandleFunc("GET /v1/eval/datasets", e.listDatasets)
	mux.HandleFunc("GET /v1/eval/datasets/{dataset_id}", e.getDataset)
	mux.HandleFunc("POST /v1/eval/datasets/{dataset_id}/cases", e.addCase)
	mux.HandleFunc("POST /v1/eval/datasets/{dataset_id}/plan-cases", e.addPlanCase)
	mux.HandleFunc("POST /v1/eval/datasets/{dataset_id}/freeze", e.freeze)
	mux.HandleFunc("POST /v1/eval/plan-runs", e.replayPlans)
	mux.HandleFunc("POST /v1/eval/runs", e.evaluate)
	mux.HandleFunc("GET /v1/eval/runs/{eval_id}", e.getEvalRun)
	mux.HandleFunc("GET /v1/profiles/{profile_id}/versions", e.listVersions)
	mux.HandleFunc("POST /v1/evolution/candidates", e.propose)
	mux.HandleFunc("GET /v1/evolution/candidates", e.listCandidates)
	mux.HandleFunc("POST /v1/evolution/candidates/{candidate_id}/evaluate", e.evaluateCandidate)
	mux.HandleFunc("POST /v1/evolution/candidates/{candidate_id}/approve", e.approve)
	mux.HandleFunc("POST /v1/evolution/candidates/{candidate_id}/rollout", e.startRollout)
	mux.HandleFunc("GET /v1/evolution/rollouts", e.listRollouts)
	mux.HandleFunc("POST /v1/evolution/rollouts/{rollout_id}/promote", e.promote)
	mux.HandleFunc("POST /v1/evolution/rollouts/{rollout_id}/rollback", e.rollback)
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10)).Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_body", err.Error())
		return false
	}
	return true
}

func (e *EvalAPI) auditAction(r *http.Request, id identity, action, resType, resID string, err error, details map[string]any) {
	outcome := "applied"
	if err != nil {
		outcome = "rejected"
		if details == nil {
			details = map[string]any{}
		}
		details["error"] = err.Error()
	}
	e.Server.audit(r.Context(), pg.AuditEntry{TenantID: id.tenant, ActorType: "user", ActorID: id.actor, Action: action,
		ResourceType: resType, ResourceID: resID, Outcome: outcome, Details: details})
}

func (e *EvalAPI) fail(w http.ResponseWriter, err error) {
	if errors.Is(err, eval.ErrInvalid) {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_request", err.Error())
		return
	}
	e.Server.domainErr(w, err)
}

func (e *EvalAPI) createDataset(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		Name string `json:"name"`
	}
	if !decodeBody(w, r, &b) {
		return
	}
	if b.Name == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "name is required")
		return
	}
	d, err := e.Service.Store.CreateDataset(r.Context(), id.tenant, b.Name, id.actor)
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, d)
}

func (e *EvalAPI) listDatasets(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	ds, err := e.Store.ListDatasets(r.Context(), id.tenant)
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"datasets": ds})
}

func (e *EvalAPI) getDataset(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	d, err := e.Service.Store.GetDataset(r.Context(), id.tenant, r.PathValue("dataset_id"))
	if err != nil {
		e.fail(w, err)
		return
	}
	cases, err := e.Service.Store.ListCases(r.Context(), id.tenant, d.ID, "")
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"dataset": d, "cases": cases})
}

// addCase records a case from a finished run (recorded tool outputs + labels).
func (e *EvalAPI) addCase(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		RunID           string   `json:"run_id"`
		RootCauseLabels []string `json:"root_cause_labels"`
		MustNotCall     []string `json:"must_not_call"`
		Split           string   `json:"split"`
	}
	if !decodeBody(w, r, &b) {
		return
	}
	if b.RunID == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "run_id is required")
		return
	}
	c, created, err := e.Service.RecordFromRun(r.Context(), id.tenant, r.PathValue("dataset_id"), b.RunID, b.RootCauseLabels, b.MustNotCall, b.Split)
	if err != nil {
		e.fail(w, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, c)
}

func (e *EvalAPI) addPlanCase(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		RunID string `json:"run_id"`
		Split string `json:"split"`
	}
	if !decodeBody(w, r, &b) {
		return
	}
	if b.RunID == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "run_id is required")
		return
	}
	c, created, err := e.Service.RecordPlanFromRun(r.Context(), id.tenant, r.PathValue("dataset_id"), b.RunID, b.Split)
	if err != nil {
		e.fail(w, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, c)
}

func (e *EvalAPI) replayPlans(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		DatasetID string `json:"dataset_id"`
		Split     string `json:"split"`
	}
	if !decodeBody(w, r, &b) {
		return
	}
	if b.DatasetID == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "dataset_id is required")
		return
	}
	if b.Split == "" {
		b.Split = "holdout"
	}
	if b.Split != "holdout" && b.Split != "train" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "split must be train or holdout")
		return
	}
	report, err := e.Service.ReplayPlans(r.Context(), id.tenant, b.DatasetID, b.Split)
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (e *EvalAPI) freeze(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	d, err := e.Service.Store.FreezeDataset(r.Context(), id.tenant, r.PathValue("dataset_id"))
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (e *EvalAPI) evaluate(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		DatasetID      string `json:"dataset_id"`
		ProfileID      string `json:"profile_id"`
		ProfileVersion int64  `json:"profile_version"`
		Split          string `json:"split"`
	}
	if !decodeBody(w, r, &b) {
		return
	}
	if b.Split == "" {
		b.Split = "holdout"
	}
	if b.Split != "holdout" && b.Split != "train" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "split must be train or holdout")
		return
	}
	run, m, err := e.Service.Evaluate(r.Context(), id.tenant, b.DatasetID, b.ProfileID, b.ProfileVersion, b.Split, id.actor)
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"evaluation": run, "metrics": m})
}

func (e *EvalAPI) getEvalRun(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	run, err := e.Service.Store.GetEvalRun(r.Context(), id.tenant, r.PathValue("eval_id"))
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (e *EvalAPI) listVersions(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	vs, err := e.Store.ListProfileVersions(r.Context(), id.tenant, r.PathValue("profile_id"))
	if err != nil {
		e.fail(w, err)
		return
	}
	active, _ := e.Service.Store.ActiveVersion(r.Context(), id.tenant, r.PathValue("profile_id"))
	writeJSON(w, http.StatusOK, map[string]any{"versions": vs, "active_version": active})
}

func (e *EvalAPI) propose(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		ProfileID   string          `json:"profile_id"`
		BaseVersion int64           `json:"base_version"`
		Patch       json.RawMessage `json:"patch"`
		Rationale   string          `json:"rationale"`
	}
	if !decodeBody(w, r, &b) {
		return
	}
	c, err := e.Service.Propose(r.Context(), id.tenant, b.ProfileID, b.BaseVersion, b.Patch, b.Rationale, id.actor)
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, c)
}

func (e *EvalAPI) listCandidates(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	cs, err := e.Store.ListCandidates(r.Context(), id.tenant, r.URL.Query().Get("profile_id"))
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"candidates": cs})
}

func (e *EvalAPI) evaluateCandidate(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		DatasetID string `json:"dataset_id"`
	}
	if !decodeBody(w, r, &b) {
		return
	}
	c, g, err := e.Service.EvaluateCandidate(r.Context(), id.tenant, r.PathValue("candidate_id"), b.DatasetID, id.actor)
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"candidate": c, "gate": g})
}

func (e *EvalAPI) approve(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	c, err := e.Service.Approve(r.Context(), id.tenant, r.PathValue("candidate_id"), id.actor)
	e.auditAction(r, id, "evolution.approve", "evolution_candidate", r.PathValue("candidate_id"), err, nil)
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (e *EvalAPI) startRollout(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		TrafficPercent float64         `json:"traffic_percent"`
		Guardrails     eval.Guardrails `json:"guardrails"`
	}
	if !decodeBody(w, r, &b) {
		return
	}
	ro, err := e.Service.StartCanary(r.Context(), id.tenant, r.PathValue("candidate_id"), b.TrafficPercent, b.Guardrails, id.actor)
	e.auditAction(r, id, "evolution.canary_start", "evolution_candidate", r.PathValue("candidate_id"), err,
		map[string]any{"traffic_percent": b.TrafficPercent})
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, ro)
}

func (e *EvalAPI) listRollouts(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	rs, err := e.Store.ListRollouts(r.Context(), id.tenant, r.URL.Query().Get("profile_id"))
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rollouts": rs})
}

func (e *EvalAPI) promote(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	ro, err := e.Service.Promote(r.Context(), id.tenant, r.PathValue("rollout_id"), id.actor)
	e.auditAction(r, id, "evolution.promote", "evolution_rollout", r.PathValue("rollout_id"), err, nil)
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ro)
}

func (e *EvalAPI) rollback(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&b)
	ro, err := e.Service.Rollback(r.Context(), id.tenant, r.PathValue("rollout_id"), id.actor, b.Reason)
	e.auditAction(r, id, "evolution.rollback", "evolution_rollout", r.PathValue("rollout_id"), err,
		map[string]any{"reason": b.Reason})
	if err != nil {
		e.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ro)
}
