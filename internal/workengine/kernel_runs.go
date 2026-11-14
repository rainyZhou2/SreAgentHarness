package workengine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/orchestration"
)

var _ orchestration.Runs = KernelRuns{}

// KernelRuns adapts the kernel run API (api.Server) to Runs.
type KernelRuns struct {
	Server *api.Server
	// MaxResultBytes bounds the result read from the artifact store.
	MaxResultBytes int
}

// StartRun creates (idempotently) a kernel run.
func (k KernelRuns) StartRun(ctx context.Context, req orchestration.RunRequest) (string, error) {
	gc := domain.GoalContext{ReceivedAt: time.Now().UTC(), Services: req.Services, Labels: req.Labels,
		Source: req.Source, Mode: req.Mode}
	run, _, err := k.Server.StartRun(ctx, api.StartRunRequest{TenantID: req.TenantID, ActorID: req.ActorID,
		InitiatorType: "work", Goal: req.Goal, Context: gc, ProfileID: req.ProfileID, ProfileVersion: req.ProfileVersion,
		IdempotencyKey: req.IdempotencyKey})
	if err != nil {
		return "", err
	}
	return run.ID, nil
}

// RunState reads a run and, when terminal, its result document.
func (k KernelRuns) RunState(ctx context.Context, tenantID, runID string) (orchestration.RunState, error) {
	run, err := k.Server.GetRun(ctx, tenantID, runID)
	if err != nil {
		return orchestration.RunState{}, err
	}
	st := orchestration.RunState{Status: string(run.Status), Terminal: run.Status.Terminal(), Succeeded: run.Status == domain.RunCompleted}
	switch run.Status {
	case domain.RunNeedsHuman, domain.RunWaitingApproval, domain.RunWaitingInput:
		st.NeedsHuman = true
	}
	if run.FailureReason != nil {
		st.FailureReason = *run.FailureReason
	}
	if st.Terminal && run.ResultRef != nil && k.Server.Artifacts != nil {
		b, err := k.Server.Artifacts.Get(ctx, tenantID, *run.ResultRef)
		if err != nil {
			return st, fmt.Errorf("read run result: %w", err)
		}
		limit := k.MaxResultBytes
		if limit <= 0 {
			limit = 1 << 20
		}
		if len(b) > limit {
			b = nil // oversized results are not parsed; the run summary is lost, not trusted partially
		}
		st.Result = b
	}
	return st, nil
}

// CancelRun asks the kernel to cancel a run (idempotent).
func (k KernelRuns) CancelRun(ctx context.Context, tenantID, runID, actorID, reason, key string) error {
	_, err := k.Server.Store.ApplyCommand(ctx, tenantID, runID, domain.CommandRequest{Type: domain.CmdCancel,
		IdempotencyKey: key, ActorID: actorID, Reason: reason})
	var rej *domain.RejectedError
	if errors.As(err, &rej) {
		return nil // already terminal or not cancellable: nothing to undo
	}
	return err
}
