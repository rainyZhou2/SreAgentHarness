package api

import (
	"encoding/json"
	"net/http"
	"slices"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

func (m *MemoryAPI) verify(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	principal, _ := auth.FromContext(r.Context())
	if !slices.Contains(principal.Roles, auth.Service) {
		writeErr(w, http.StatusForbidden, "forbidden", "an explicitly authenticated machine verifier is required")
		return
	}
	if m.Governance == nil {
		writeErr(w, http.StatusServiceUnavailable, "verification_unavailable", "memory verification authority is not configured")
		return
	}
	var body struct {
		ExpectedVersion int64  `json:"expected_version"`
		DecisionRef     string `json:"decision_ref"`
		OutcomeRef      string `json:"outcome_ref"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&body); err != nil ||
		body.ExpectedVersion < 1 || body.DecisionRef == "" || body.OutcomeRef == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "expected_version, decision_ref and outcome_ref are required")
		return
	}
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		writeErr(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key is required")
		return
	}
	record, current, err := m.Governance.Verify(r.Context(), memory.VerificationRequest{TenantID: id.tenant,
		MemoryID: r.PathValue("memory_id"), ExpectedVersion: body.ExpectedVersion, DecisionRef: body.DecisionRef,
		OutcomeRef: body.OutcomeRef, Verifier: id.actor, IdempotencyKey: idempotencyKey})
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"verification": record, "memory": current})
}

func (m *MemoryAPI) approve(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var body struct {
		ExpectedVersion int64 `json:"expected_version"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&body); err != nil || body.ExpectedVersion < 1 {
		writeErr(w, http.StatusBadRequest, "invalid_body", "expected_version is required")
		return
	}
	current, err := m.Store.ApproveMemory(r.Context(), id.tenant, r.PathValue("memory_id"), body.ExpectedVersion, id.actor)
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, current)
}

func rejectPublicationTransition(to string) error {
	if to == memory.Verified || to == memory.Approved {
		return &domain.RejectedError{Reason: "publication_requires_dedicated_workflow"}
	}
	return nil
}
