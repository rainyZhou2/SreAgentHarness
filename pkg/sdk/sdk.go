// Package sdk exposes versioned plugin definitions, explicit lifecycles and extension points.
package sdk

import (
	"context"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/eventbus"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/eventsink"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/notify"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/stream"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/world"
)

// ---- framework ----

type (
	Kind[T any]         = plugin.Kind[T]
	Definition[T any]   = plugin.Definition[T]
	Component[T any]    = plugin.Component[T]
	Descriptor          = plugin.Descriptor
	PermissionScope     = plugin.PermissionScope
	ResourceBudget      = plugin.ResourceBudget
	ErrorContract       = plugin.ErrorContract
	CompatibilityPolicy = plugin.CompatibilityPolicy
	DataClass           = plugin.DataClass
	Lifecycle           = plugin.Lifecycle
	LifecycleFuncs      = plugin.LifecycleFuncs
	Task                = plugin.Task
	TaskFunc            = plugin.TaskFunc
	NamedTask           = plugin.NamedTask
	Health              = plugin.Health
	HealthStatus        = plugin.HealthStatus
	State               = plugin.State
	Host                = plugin.Host
	Config              = plugin.Config
	Spec                = plugin.Spec
	Settings            = plugin.Settings
	Duration            = plugin.Duration
	Info                = plugin.Info
	KindInfo            = plugin.KindInfo
)

const (
	ExactContract    = plugin.ExactContract
	CompatibleMajor  = plugin.CompatibleMajor
	Healthy          = plugin.Healthy
	Degraded         = plugin.Degraded
	Unhealthy        = plugin.Unhealthy
	DataPublic       = plugin.DataPublic
	DataInternal     = plugin.DataInternal
	DataConfidential = plugin.DataConfidential
	DataRestricted   = plugin.DataRestricted
)

func Register[T any](k Kind[T], definition Definition[T]) { plugin.Register(k, definition) }

func Define[T any](name, version, doc string, validate func(Config) error,
	initialize func(context.Context, *Host, Config) (Component[T], error)) Definition[T] {
	return plugin.Define(name, version, doc, validate, initialize)
}

func StaticDefinition[T any](name, version, doc string, validate func(Config) error,
	initialize func(context.Context, *Host, Config) (T, error)) Definition[T] {
	return plugin.StaticDefinition(name, version, doc, validate, initialize)
}

func BackgroundDefinition(name, version, doc string, validate func(Config) error,
	initialize func(context.Context, *Host, Config) (Task, error)) Definition[Task] {
	return plugin.BackgroundDefinition(name, version, doc, validate, initialize)
}

func StrictConfig[C any]() func(Config) error { return plugin.StrictConfig[C]() }

func Static[T any](value T) Component[T] { return plugin.Static(value) }

func Background(task Task, name string) Component[Task] { return plugin.Background(task, name) }

func NewKind[T any](info KindInfo) Kind[T] {
	return plugin.NewKind[T](info)
}

// Build instantiates a plugin of a custom kind (for plugins that host their
// own sub-plugins).
func Build[T any](ctx context.Context, k Kind[T], h *Host, s Spec) (T, error) {
	return plugin.Build(ctx, k, h, s)
}

// Extension points.
var (
	Telemetry       = plugin.Telemetry
	ArtifactStore   = plugin.ArtifactStore
	Tools           = plugin.Tools
	Policy          = plugin.Policy
	StreamBus       = plugin.StreamBus
	Broker          = plugin.Broker
	EventBus        = plugin.EventBus
	ModelProvider   = plugin.ModelProvider
	Embedder        = plugin.Embedder
	MemoryRetriever = plugin.MemoryRetriever
	Strategy        = plugin.Strategy
	Grader          = plugin.Grader
	Job             = plugin.Job
	Authenticator   = plugin.Authenticator
	API             = plugin.API

	ProfileSource    = plugin.ProfileSource
	RoutePolicy      = plugin.RoutePolicy
	ContextAssembler = plugin.ContextAssembler
	Compactor        = plugin.Compactor
	EventSink        = plugin.EventSink
)

// ---- contracts per kind ----

type (
	TelemetryExporter   = plugin.TelemetryExporter
	Observer            = kernel.Observer
	ToolSource          = plugin.ToolSource
	ToolSourceFunc      = plugin.ToolSourceFunc
	MessageBroker       = plugin.MessageBroker
	ModelBackend        = plugin.ModelBackend
	StrategyBuilder     = plugin.StrategyBuilder
	StrategyBuilderFunc = plugin.StrategyBuilderFunc
	StrategyRuntime     = plugin.StrategyRuntime
	Gate                = policy.Gate
	APIExtension        = api.Extension
	GraderFunc          = eval.GraderFunc
	EvalGrader          = eval.Grader
	EvalCase            = eval.Case
	Authn               = auth.Authenticator
	Principal           = auth.Principal
	Role                = auth.Role
	TextEmbedder        = memory.Embedder
	Retriever           = kernel.MemoryRetriever
	ArtifactBlobStore   = artifact.Store
	DeltaBus            = stream.Bus
	Delta               = stream.Delta
	EventBusContract    = eventbus.Bus
	Publisher           = notify.Publisher
	Subscriber          = notify.Subscriber
	Handler             = notify.Handler
)

// ---- tools ----

type (
	Tool           = tools.Tool
	ToolDescriptor = tools.Descriptor
	FuncTool       = tools.Func
	CallInfo       = tools.CallInfo
	RiskClass      = domain.RiskClass
)

const (
	RiskR0 = domain.RiskR0 // read-only
	RiskR1 = domain.RiskR1 // low-risk reversible
	RiskR2 = domain.RiskR2 // production write (always needs approval)
	RiskR3 = domain.RiskR3 // destructive (always denied by the framework)
)

// CallInfoFrom returns the run/step/attempt a tool invocation belongs to.
func CallInfoFrom(ctx context.Context) (CallInfo, bool) { return tools.CallInfoFrom(ctx) }

// StaticTools is a ToolSource for a fixed tool list.
func StaticTools(ts ...Tool) ToolSource {
	return plugin.ToolSourceFunc(func(context.Context) ([]Tool, error) { return ts, nil })
}

// ErrNotDispatched tells the kernel a tool call was rejected before any side
// effect happened (safe to fail permanently, no unknown-effect handling).
var ErrNotDispatched = domain.ErrNotDispatched

// ErrUnknownEffect tells the kernel the call may or may not have taken effect.
var ErrUnknownEffect = domain.ErrUnknownEffect

// ---- policy ----

type (
	PolicyDecision = policy.Decision
	Verdict        = policy.Verdict
	ActionIntent   = domain.ActionIntent
	ToolCall       = domain.ToolCall
)

const (
	Allow           = policy.Allow
	RequireApproval = policy.RequireApproval
	Deny            = policy.Deny
)

// DefaultPolicy returns the built-in engine, for plugins that only tighten it.
func DefaultPolicy() policy.Engine { return policy.Default() }

// ---- auth ----

const (
	Viewer   = auth.Viewer
	Operator = auth.Operator
	Approver = auth.Approver
	Admin    = auth.Admin
)

// ---- models and strategies ----

type (
	ModelProviderAdapter = gw.Provider
	ModelRequest         = gw.Request
	ModelEndpoint        = gw.Endpoint
	ProfileSpec          = profile.Spec
	ReasoningStrategy    = cognitive.Strategy
	ReasoningInput       = cognitive.ReasoningInput
	ReasoningDecision    = domain.Decision

	// route_policy
	RouteScorer     = gw.Scorer
	RouteScoreInput = gw.ScoreInput
	PriorityScorer  = gw.PriorityScorer
	WeightedScorer  = gw.WeightedScorer

	// context_assembler / compactor
	ContextAssemblerPlugin = cognitive.ContextAssembler
	ContextBuilder         = cognitive.ContextBuilder
	AssemblerOptions       = cognitive.AssemblerOptions
	SectionAssembler       = cognitive.Assembler
	PromptManifest         = cognitive.PromptManifest
	CompactorPlugin        = kernel.Compactor
	CompactInput           = kernel.CompactInput
	Observation            = domain.Observation

	// profile_source
	ProfileSourcePlugin = profile.Source
	ProfileSourceFunc   = profile.SourceFunc
	ProfileDefinition   = profile.Definition

	// event_sink
	EventSinkPlugin  = eventsink.Sink
	EventSinkOptions = eventsink.Options
	RunEvent         = domain.Event
)

// DefaultAssembler returns the built-in sectioned assembler, for context
// assembler plugins that only adjust it.
func DefaultAssembler() cognitive.Assembler { return cognitive.DefaultAssembler() }

// PermanentDeliveryError marks an event sink error that must not be retried.
func PermanentDeliveryError(err error) error { return eventsink.Permanent(err) }

// ---- control plane (Works, actions, outcomes, world) ----

type (
	WorkExtension    = plugin.WorkExtension
	WorkRunner       = plugin.WorkRunner
	WorkEnv          = plugin.WorkEnv
	ActionExecutor   = action.Executor
	ActionDescriptor = action.Descriptor
	ActionPlan       = action.Plan
	ActionPreflight  = action.PreflightResult
	ActionInspection = action.InspectResult
	MetricSource     = outcome.MetricSource
	MetricQuery      = outcome.MetricQuery
	MetricResult     = outcome.MetricResult
	MetricSample     = outcome.Sample
	WorldFact        = world.Fact
	EventEnvelope    = eventing.Envelope
)

// WorkExt registers WorkExtension plugins (config: plugins.work_extension).
var WorkExt = plugin.WorkExt

// Action idempotency modes and errors.
const (
	IdempotencyNative           = action.IdemNative
	IdempotencyQueryBeforeRetry = action.IdemQueryBeforeRetry
	IdempotencyNone             = action.IdemNone
)

// ErrActionNotApplied tells the runtime the downstream rejected the call
// before any effect (safe to mark FAILED); any other error is an unknown
// effect that is reconciled, never blindly retried.
var ErrActionNotApplied = action.ErrNotApplied
