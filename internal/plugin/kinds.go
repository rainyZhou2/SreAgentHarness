package plugin

import (
	"context"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/eventsink"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/notify"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/stream"
	"go-sre-harness/internal/tools"
)

// Bootstrap order (each phase sees the Host fields of earlier phases):
//
//	telemetry → artifact_store → tools → profile_source → policy →
//	stream_bus → broker → route_policy → model_provider (→ Host.Gateway) →
//	embedder → memory_retriever → context_assembler → strategy → compactor →
//	grader → event_sink → job → authenticator → api
var (
	Telemetry       = NewKind[TelemetryExporter]("telemetry", true, "tracing/metrics exporters; may also implement kernel.Observer")
	ArtifactStore   = NewKind[artifact.Store]("artifact_store", false, "content-addressed blob store for outputs, prompts and recordings")
	Tools           = NewKind[ToolSource]("tools", true, "tool sources; each contributes tools to the shared registry")
	Policy          = NewKind[policy.Gate]("policy", false, "tool risk policy; framework floor (R2 approval, R3 deny) always applies")
	StreamBus       = NewKind[stream.Bus]("stream_bus", false, "ephemeral token/delta stream")
	Broker          = NewKind[MessageBroker]("broker", false, "wake-up notification broker (outbox → workers)")
	ModelProvider   = NewKind[ModelBackend]("model_provider", true, "LLM provider adapters with their endpoints and aliases")
	Embedder        = NewKind[memory.Embedder]("embedder", false, "text embedder for memory retrieval")
	MemoryRetriever = NewKind[kernel.MemoryRetriever]("memory_retriever", false, "retrieve_memory step implementation (optional)")
	Strategy        = NewKind[StrategyBuilder]("strategy", true, "reasoning strategies, selected by profile spec.strategy")
	Grader          = NewKind[eval.Grader]("grader", false, "offline-evaluation answer grader")
	Job             = NewKind[Runner]("job", true, "background jobs; run in the scheduler role")
	Authenticator   = NewKind[auth.Authenticator]("authenticator", true, "API authenticators, tried in order")
	API             = NewKind[api.Extension]("api", true, "HTTP route extensions with declared route roles")

	ProfileSource    = NewKind[profile.Source]("profile_source", true, "agent profile versions registered at startup (immutable per version)")
	RoutePolicy      = NewKind[gw.Scorer]("route_policy", false, "soft score for model endpoints; hard routing filters always apply")
	ContextAssembler = NewKind[cognitive.ContextAssembler]("context_assembler", false, "renders reasoning input into model requests + prompt manifests; trust rules always apply")
	Compactor        = NewKind[kernel.Compactor]("compactor", false, "summarizes old observations; the kernel always appends safety facts")
	EventSink        = NewKind[eventsink.Sink]("event_sink", true, "at-least-once delivery of committed run events (webhooks, chat, paging)")
)

// TelemetryExporter is a telemetry plugin. Implement kernel.Observer as
// well to receive step/claim/tool events.
type TelemetryExporter interface {
	Shutdown(ctx context.Context) error
}

// ToolSource contributes tools. Tools returned are registered under their
// descriptor names; the descriptor (risk, trust, target) comes from the
// plugin's operator-owned config, never from the remote tool itself.
type ToolSource interface {
	Tools(ctx context.Context) ([]tools.Tool, error)
}

// ToolSourceFunc adapts a function.
type ToolSourceFunc func(ctx context.Context) ([]tools.Tool, error)

func (f ToolSourceFunc) Tools(ctx context.Context) ([]tools.Tool, error) { return f(ctx) }

// MessageBroker publishes and consumes run wake-ups.
type MessageBroker interface {
	notify.Publisher
	notify.Subscriber
}

// ModelBackend is a model provider plugin's contribution: the adapter, the
// endpoints it serves, and the aliases it joins (alias → endpoint IDs in
// preference order; lists from several providers are concatenated in
// config order).
type ModelBackend struct {
	Provider  gw.Provider
	Endpoints []gw.Endpoint
	Aliases   map[string][]string
}

// StrategyRuntime is what a strategy may use. Gateway differs between live
// runs (ledgered, fenced) and offline evaluation (in-memory ledger).
type StrategyRuntime struct {
	Gateway *gw.Gateway
	// Context is the enabled context_assembler plugin.
	Context   cognitive.ContextAssembler
	Deltas    cognitive.DeltaPublisher // nil in offline evaluation
	Artifacts artifact.Store
	Settings  Settings
}

// StrategyBuilder builds a strategy for a profile spec.
type StrategyBuilder interface {
	Build(spec profile.Spec, rt StrategyRuntime) (cognitive.Strategy, error)
}

// StrategyBuilderFunc adapts a function.
type StrategyBuilderFunc func(spec profile.Spec, rt StrategyRuntime) (cognitive.Strategy, error)

func (f StrategyBuilderFunc) Build(spec profile.Spec, rt StrategyRuntime) (cognitive.Strategy, error) {
	return f(spec, rt)
}
