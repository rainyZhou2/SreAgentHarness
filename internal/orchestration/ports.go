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
	"time"

	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// RunRequest reserves one non-executable Run until its Work owner is committed.
type RunRequest struct {
	WorkID         string
	Generation     int
	StageID        string
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
	ReserveRun(ctx context.Context, req RunRequest) (runID string, err error)
	ActivateRun(ctx context.Context, req work.RunActivation) error
	RunState(ctx context.Context, tenantID, runID string) (RunState, error)
	CancelRun(ctx context.Context, tenantID, runID, actorID, reason, key string) error
}

// WorldFacts adapts world.Service to outcome.StateSource.
type WorldFacts struct{ World *world.Service }

// CurrentFact implements outcome.StateSource.
func (w WorldFacts) CurrentFact(ctx context.Context, tenantID, subject, predicate string, at time.Time) (outcome.FactValue, bool, error) {
	v, obs, ref, ok, err := w.World.CurrentFact(ctx, tenantID, subject, predicate, at)
	return outcome.FactValue{Value: v, ObservedAt: obs, Ref: ref}, ok, err
}

// WorkRun identifies reservation provenance, not the current Work authority.
type WorkRun struct {
	TenantID   string
	RunID      string
	WorkID     string
	Generation int
	StageID    string
}

// RunInventory scans nonterminal Work reservations using a bounded RunID cursor.
type RunInventory interface {
	ListWorkRuns(ctx context.Context, after string, limit int) ([]WorkRun, error)
}
