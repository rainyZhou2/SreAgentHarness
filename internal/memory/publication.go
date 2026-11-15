package memory

import (
	"strings"
	"time"
)

const OwnerPreferenceBasis = "OWNER_PREFERENCE"

func IsOwnerPreference(m Memory) bool {
	return m.Scope == "user" && m.ScopeID != "" && m.Type == "semantic" && strings.HasPrefix(m.Subject, "pref:")
}

func ApplyRevisionPublication(current *Memory, prepared NewMemory, at time.Time) {
	current.Status = StatusCandidate
	current.VerifiedBy = nil
	current.ApprovedBy = nil
	current.ApprovedAt = nil
	current.ApprovalBasis = ""
	current.DecisionRef = ""
	current.OutcomeRef = ""
	current.ConflictsWith = nil
	if prepared.Status == Approved && IsOwnerPreference(*current) {
		owner := current.ScopeID
		current.Status = Approved
		current.ApprovedBy = &owner
		current.ApprovedAt = &at
		current.ApprovalBasis = OwnerPreferenceBasis
	}
}

func ApplyOwnerPreferencePublication(current *Memory, at time.Time) {
	if current.Status != Approved || !IsOwnerPreference(*current) {
		return
	}
	owner := current.ScopeID
	current.ApprovedBy = &owner
	current.ApprovedAt = &at
	current.ApprovalBasis = OwnerPreferenceBasis
}
