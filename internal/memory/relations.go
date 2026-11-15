package memory

import (
	"time"

	"go-sre-harness/internal/domain"
)

type ConflictPair struct {
	FirstID  string `json:"first_memory_id"`
	SecondID string `json:"second_memory_id"`
}

type ConflictRecord struct {
	TenantID     string       `json:"tenant_id"`
	Pair         ConflictPair `json:"pair"`
	Reason       string       `json:"reason"`
	EvidenceRefs []string     `json:"evidence_refs"`
	CreatedBy    string       `json:"created_by"`
	CreatedAt    time.Time    `json:"created_at"`
	ResolvedBy   string       `json:"resolved_by,omitempty"`
	ResolvedAt   *time.Time   `json:"resolved_at,omitempty"`
	Resolution   string       `json:"resolution,omitempty"`
}

func NewConflictPair(a, b Memory) (ConflictPair, error) {
	if a.ID == "" || b.ID == "" || a.ID == b.ID || a.TenantID == "" || a.TenantID != b.TenantID ||
		a.Scope != b.Scope || a.ScopeID != b.ScopeID {
		return ConflictPair{}, &domain.RejectedError{Reason: "invalid_conflict_pair"}
	}
	if a.ID > b.ID {
		a.ID, b.ID = b.ID, a.ID
	}
	return ConflictPair{FirstID: a.ID, SecondID: b.ID}, nil
}

func ValidateSupersession(previous, next Memory) error {
	if previous.ID == "" || next.ID == "" || previous.ID == next.ID || previous.TenantID == "" ||
		previous.TenantID != next.TenantID || previous.Scope != next.Scope || previous.ScopeID != next.ScopeID ||
		previous.Subject != next.Subject || previous.Status == Revoked || previous.Status == Superseded || !RecallStatus(next.Status, next.Type) {
		return &domain.RejectedError{Reason: "invalid_supersession"}
	}
	return nil
}
