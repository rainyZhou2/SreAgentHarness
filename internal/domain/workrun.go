package domain

import "strings"

// ValidateWorkRunReservation checks the reservation boundary before any write.
func ValidateWorkRunReservation(req CreateRunRequest) error {
	if req.WorkOwner == nil {
		return nil
	}
	o := req.WorkOwner
	if !validWorkRunOwner(*o) || !req.PendingActivation || req.InitiatorID != "work:"+o.WorkID ||
		req.SessionID != nil || req.BranchID != nil || req.TurnID != nil || req.ParentRunID != nil || req.ParentStepID != nil || req.FlowNodeID != "" || req.FlowDepth != 0 {
		return &RejectedError{Reason: "invalid_work_run_reservation"}
	}
	return nil
}

func validWorkRunOwner(o WorkRunOwner) bool {
	return strings.TrimSpace(o.WorkID) != "" && o.Generation > 0 && strings.TrimSpace(o.StageID) != ""
}

// SameWorkRunOwner includes legacy nil provenance in idempotency comparisons.
func SameWorkRunOwner(a, b *WorkRunOwner) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// WorkRunActivationStatus validates ownership before deciding whether activation
// changes state. Repeated activation never resets an active or terminal run.
func WorkRunActivationStatus(r Run, tenantID string, owner WorkRunOwner) (RunStatus, error) {
	if r.TenantID != tenantID {
		return r.Status, ErrNotFound
	}
	if !validWorkRunOwner(owner) || !SameWorkRunOwner(r.WorkOwner, &owner) ||
		r.SessionID != nil || r.BranchID != nil || r.TurnID != nil || r.ParentRunID != nil || r.ParentStepID != nil || r.FlowNodeID != "" || r.FlowDepth != 0 ||
		r.InitiatorID == nil || *r.InitiatorID != "work:"+owner.WorkID {
		return r.Status, &RejectedError{Reason: "work_run_owner_mismatch"}
	}
	if r.Status.Terminal() || r.DesiredState == DesiredCancelled {
		return r.Status, &RejectedError{Reason: "run_terminal"}
	}
	if r.Status != RunPendingActivation {
		return r.Status, nil
	}
	if r.DesiredState == DesiredPaused {
		return RunPaused, nil
	}
	return RunQueued, nil
}
