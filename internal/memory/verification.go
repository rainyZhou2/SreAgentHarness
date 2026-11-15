package memory

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"go-sre-harness/internal/domain"
)

type VerificationInput struct {
	TenantID        string   `json:"tenant_id"`
	MemoryID        string   `json:"memory_id"`
	ExpectedVersion int64    `json:"expected_version"`
	EvidenceRefs    []string `json:"evidence_refs"`
	DecisionRef     string   `json:"decision_ref"`
	OutcomeRef      string   `json:"outcome_ref"`
	DecisionVerdict string   `json:"decision_verdict"`
	OutcomeVerdict  string   `json:"outcome_verdict"`
	InputHash       string   `json:"input_hash"`
	ActualHash      string   `json:"actual_hash"`
}

func (v VerificationInput) Validate() error {
	if v.TenantID == "" || v.MemoryID == "" || v.ExpectedVersion < 1 || len(v.EvidenceRefs) == 0 ||
		v.DecisionRef == "" || v.OutcomeRef == "" {
		return &domain.RejectedError{Reason: "verification_evidence_required"}
	}
	if v.DecisionVerdict != "CONFIRMED" || v.OutcomeVerdict != "SATISFIED" {
		return &domain.RejectedError{Reason: "verification_verdict_not_satisfied"}
	}
	if v.InputHash == "" || v.InputHash != v.ActualHash {
		return &domain.RejectedError{Reason: "verification_input_changed"}
	}
	return nil
}

type VerificationRecord struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	MemoryID       string    `json:"memory_id"`
	MemoryVersion  int64     `json:"memory_version"`
	EvidenceRefs   []string  `json:"evidence_refs"`
	DecisionRef    string    `json:"decision_ref"`
	OutcomeRef     string    `json:"outcome_ref"`
	InputHash      string    `json:"input_hash"`
	Verifier       string    `json:"verifier"`
	IdempotencyKey string    `json:"idempotency_key"`
	CreatedAt      time.Time `json:"created_at"`
}

func VerificationInputFor(m Memory, decisionRef, outcomeRef string) VerificationInput {
	input := VerificationInput{TenantID: m.TenantID, MemoryID: m.ID, ExpectedVersion: m.Version,
		EvidenceRefs: append([]string(nil), m.EvidenceRefs...), DecisionRef: decisionRef, OutcomeRef: outcomeRef,
		DecisionVerdict: "CONFIRMED", OutcomeVerdict: "SATISFIED"}
	input.InputHash = verificationHash(input)
	input.ActualHash = input.InputHash
	return input
}

func verificationHash(input VerificationInput) string {
	canonical := struct {
		TenantID     string   `json:"tenant_id"`
		MemoryID     string   `json:"memory_id"`
		Version      int64    `json:"version"`
		EvidenceRefs []string `json:"evidence_refs"`
		DecisionRef  string   `json:"decision_ref"`
		OutcomeRef   string   `json:"outcome_ref"`
	}{input.TenantID, input.MemoryID, input.ExpectedVersion, input.EvidenceRefs, input.DecisionRef, input.OutcomeRef}
	data, _ := json.Marshal(canonical)
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
