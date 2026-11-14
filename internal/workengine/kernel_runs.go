package workengine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/work"
)

var _ orchestration.Runs = KernelRuns{}

// KernelRuns adapts the kernel run API (api.Server) to Runs.
type KernelRuns struct {
	Server        *api.Server
	WorkAuthority interface {
		CheckRunActivation(context.Context, work.RunActivation) error
	}
	// MaxResultBytes bounds the result read from the artifact store.
	MaxResultBytes int
}

type workRunStore interface {
	ActivateWorkRun(context.Context, string, string, domain.WorkRunOwner) error
	ListWorkRuns(context.Context, string, int) ([]domain.Run, error)
}

// ReserveRun creates (idempotently) a kernel run.
func (k KernelRuns) ReserveRun(ctx context.Context, req orchestration.RunRequest) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	gc := domain.GoalContext{ReceivedAt: time.Now().UTC(), Services: req.Services, Labels: req.Labels,
		Source: req.Source, Mode: req.Mode}
	owner := &domain.WorkRunOwner{WorkID: req.WorkID, Generation: req.Generation, StageID: req.StageID}
	run, _, err := k.Server.StartRun(ctx, api.StartRunRequest{TenantID: req.TenantID, ActorID: "work:" + req.WorkID,
		InitiatorType: "work", Goal: req.Goal, Context: gc, ProfileID: req.ProfileID, ProfileVersion: req.ProfileVersion,
		IdempotencyKey: req.IdempotencyKey, WorkOwner: owner})
	if err != nil {
		return "", err
	}
	return run.ID, nil
}

func (k KernelRuns) ActivateRun(ctx context.Context, req work.RunActivation) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	store, ok := k.Server.Store.(workRunStore)
	if !ok || k.WorkAuthority == nil {
		return errors.New("work run activation is unsupported")
	}
	if err := k.WorkAuthority.CheckRunActivation(ctx, req); err != nil {
		return fmt.Errorf("authorize run activation: %w", err)
	}
	return store.ActivateWorkRun(ctx, req.TenantID, req.RunID, domain.WorkRunOwner{WorkID: req.WorkID, Generation: req.Generation, StageID: req.StageID})
}

func (k KernelRuns) ListWorkRuns(ctx context.Context, after string, limit int) ([]orchestration.WorkRun, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	store, ok := k.Server.Store.(workRunStore)
	if !ok {
		return nil, errors.New("work run recovery is unsupported")
	}
	runs, err := store.ListWorkRuns(ctx, after, limit)
	if err != nil {
		return nil, err
	}
	out := make([]orchestration.WorkRun, 0, len(runs))
	for _, r := range runs {
		if r.WorkOwner == nil {
			return nil, errors.New("work run inventory omitted owner")
		}
		out = append(out, orchestration.WorkRun{TenantID: r.TenantID, RunID: r.ID, WorkID: r.WorkOwner.WorkID, Generation: r.WorkOwner.Generation, StageID: r.WorkOwner.StageID})
	}
	return out, nil
}

// RunState reads a run and, when terminal, its result document.
func (k KernelRuns) RunState(ctx context.Context, tenantID, runID string) (orchestration.RunState, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
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
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := k.Server.Store.ApplyCommand(ctx, tenantID, runID, domain.CommandRequest{Type: domain.CmdCancel,
		IdempotencyKey: key, ActorID: actorID, Reason: reason})
	var rej *domain.RejectedError
	if errors.As(err, &rej) {
		run, readErr := k.Server.GetRun(ctx, tenantID, runID)
		if readErr != nil {
			return fmt.Errorf("verify rejected cancellation: %w", readErr)
		}
		if run.Status.Terminal() {
			return nil
		}
	}
	return err
}
