package plugin

import (
	"context"
	"net/http"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
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
	// AssertRelation records a topology edge; RetractRelation closes one
	// (the edge disappeared from the observed system).
	AssertRelation  func(ctx context.Context, r world.Relation) error
	RetractRelation func(ctx context.Context, tenantID, from, typ, to, source string, at time.Time) error
}

// EventContributor is optionally implemented by a WorkExtension that ships
// default correlation rules. Operator rules with the same name win.
type EventContributor interface {
	EventRules() []eventing.Rule
}

// SourceNormalizers is optionally implemented by a WorkExtension that
// accepts source-native webhooks (Alertmanager, Kubernetes events, Argo
// CD, ...) at POST /v1/sources/{source}/events.
type SourceNormalizers interface {
	Sources() []string
	Normalizer(source string) (func(ctx context.Context, tenantID string, h http.Header, body []byte, now time.Time) ([]eventing.Envelope, error), bool)
}

// TemplateContributor is optionally implemented by a WorkExtension that
// ships scenario Work templates. Operator-configured templates with the
// same id win.
type TemplateContributor interface {
	WorkTemplates() []work.Template
}

// WorkRunner is optionally implemented by a WorkExtension that feeds the
// world or the inbox in the background (collectors, simulators). It runs
// only when the work engine is enabled and must return when ctx ends.
type WorkRunner interface {
	RunWork(ctx context.Context, env WorkEnv) error
}

// WorkExt is the extension point.
var WorkExt = NewKind[WorkExtension]("work_extension", true, "v2 action executors and outcome metric sources (optional background feeders)")
