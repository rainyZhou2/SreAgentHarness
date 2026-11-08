// Package orchestration drives Works through their pinned templates: it
// claims due Works, starts and observes kernel runs, waits on durable
// watches, proposes and executes actions through the action runtime, and
// verifies outcomes. It also hosts the event ingestor, the watch
// reconciler and the recovery sweeps.
//
// The database is the source of truth: every decision is one ApplyWork
// mutation (version CAS plus lease check), every wait is a watch or a
// wake_at, so any replica can resume any Work after a crash. The kernel is
// used only through its public run API; it never imports this package.
package orchestration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/world"
)

// RunRequest asks the kernel for one run.
type RunRequest struct {
	TenantID       string
	ActorID        string
	Goal           string
	ProfileID      string
	ProfileVersion int64
	Mode           string
	Services       []string
	Labels         map[string]string
	Source         string
	IdempotencyKey string
}

// RunState is what the orchestrator needs from a run.
type RunState struct {
	Status        string
	Terminal      bool
	Succeeded     bool
	NeedsHuman    bool
	FailureReason string
	// Result is the raw result document (bounded), when terminal.
	Result []byte
}

// Runs is the kernel port.
type Runs interface {
	StartRun(ctx context.Context, req RunRequest) (runID string, err error)
	RunState(ctx context.Context, tenantID, runID string) (RunState, error)
	CancelRun(ctx context.Context, tenantID, runID, actorID, reason, key string) error
}

// KernelRuns adapts the kernel run API (api.Server) to Runs.
type KernelRuns struct {
	Server *api.Server
	// MaxResultBytes bounds the result read from the artifact store.
	MaxResultBytes int
}

// StartRun creates (idempotently) a kernel run.
func (k KernelRuns) StartRun(ctx context.Context, req RunRequest) (string, error) {
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
func (k KernelRuns) RunState(ctx context.Context, tenantID, runID string) (RunState, error) {
	run, err := k.Server.GetRun(ctx, tenantID, runID)
	if err != nil {
		return RunState{}, err
	}
	st := RunState{Status: string(run.Status), Terminal: run.Status.Terminal(), Succeeded: run.Status == domain.RunCompleted}
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

// WorldFacts adapts world.Service to outcome.StateSource.
type WorldFacts struct{ World *world.Service }

// CurrentFact implements outcome.StateSource.
func (w WorldFacts) CurrentFact(ctx context.Context, tenantID, subject, predicate string, at time.Time) (outcome.FactValue, bool, error) {
	v, obs, ref, ok, err := w.World.CurrentFact(ctx, tenantID, subject, predicate, at)
	return outcome.FactValue{Value: v, ObservedAt: obs, Ref: ref}, ok, err
}
