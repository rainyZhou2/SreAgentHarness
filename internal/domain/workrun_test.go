package domain

import (
	"errors"
	"testing"
)

func TestWorkRunActivationValidatesProvenanceBeforeState(t *testing.T) {
	owner := WorkRunOwner{WorkID: "w", Generation: 1, StageID: "s"}
	initiator := "work:w"
	value := "other"
	base := Run{TenantID: "t", WorkOwner: &owner, InitiatorID: &initiator, Status: RunPendingActivation, DesiredState: DesiredRunning}
	for name, mutate := range map[string]func(*Run){
		"session":         func(r *Run) { r.SessionID = &value },
		"branch":          func(r *Run) { r.BranchID = &value },
		"turn":            func(r *Run) { r.TurnID = &value },
		"parent run":      func(r *Run) { r.ParentRunID = &value },
		"parent step":     func(r *Run) { r.ParentStepID = &value },
		"flow node":       func(r *Run) { r.FlowNodeID = value },
		"flow depth":      func(r *Run) { r.FlowDepth = 1 },
		"legacy":          func(r *Run) { r.WorkOwner = nil },
		"initiator":       func(r *Run) { r.InitiatorID = &value },
		"canceled intent": func(r *Run) { r.DesiredState = DesiredCancelled },
	} {
		t.Run(name, func(t *testing.T) {
			for _, status := range []RunStatus{RunPendingActivation, RunQueued, RunRunning, RunPaused} {
				r := base
				r.Status = status
				mutate(&r)
				_, err := WorkRunActivationStatus(r, "t", owner)
				var rejected *RejectedError
				if !errors.As(err, &rejected) {
					t.Fatalf("%s: %v", status, err)
				}
			}
		})
	}
	for _, status := range []RunStatus{RunCompleted, RunFailed, RunCancelled} {
		r := base
		r.Status = status
		got, err := WorkRunActivationStatus(r, "t", owner)
		var rejected *RejectedError
		if got != status || !errors.As(err, &rejected) {
			t.Fatalf("terminal %s: %s %v", status, got, err)
		}
	}
	for _, owner := range []WorkRunOwner{{WorkID: "", Generation: 1, StageID: "s"}, {WorkID: "w", Generation: 0, StageID: "s"}, {WorkID: "w", Generation: 1, StageID: " "}} {
		req := CreateRunRequest{WorkOwner: &owner, PendingActivation: true, InitiatorID: "work:" + owner.WorkID}
		if err := ValidateWorkRunReservation(req); err == nil {
			t.Fatalf("invalid owner accepted: %+v", owner)
		}
	}
}
