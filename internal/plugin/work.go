package plugin

import (
	"context"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/world"
)

// WorkExtension contributes v2 control-plane capabilities: action
// executors (always behind the action runtime's policy, preflight,
// approval and ledger) and named, read-only metric sources for outcome
// verification. Built in the "work_extension" phase, after tools.
type WorkExtension interface {
	Actions() []action.Executor
	MetricSources() map[string]outcome.MetricSource
}

// WorkEnv is what a WorkRunner may use: world writes and event ingestion,
// both through the same validated paths as the HTTP API.
type WorkEnv struct {
	// AssertFact records an observation in the world state.
	AssertFact func(ctx context.Context, f world.Fact) error
	// Ingest submits an event to the inbox (dedup, correlation, watches).
	Ingest func(ctx context.Context, e eventing.Envelope) error
}

// WorkRunner is optionally implemented by a WorkExtension that feeds the
// world or the inbox in the background (collectors, simulators). It runs
// only when the work engine is enabled and must return when ctx ends.
type WorkRunner interface {
	RunWork(ctx context.Context, env WorkEnv) error
}

// WorkExt is the extension point.
var WorkExt = NewKind[WorkExtension]("work_extension", true, "v2 action executors and outcome metric sources (optional background feeders)")
