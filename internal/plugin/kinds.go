package plugin

import (
	"context"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/envctx"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/eventbus"
	"go-sre-harness/internal/eventsink"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/notify"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/sandbox"
	"go-sre-harness/internal/stream"
	"go-sre-harness/internal/tools"
)

// Bootstrap order (each phase sees the Host fields of earlier phases):
//
//	telemetry → artifact_store → tools → sandbox → profile_source → policy →
//	stream_bus → broker → event_bus → route_policy → model_provider (→ Host.Gateway) →
//	embedder → memory_retriever → context_provider → context_assembler → strategy → compactor →
//	grader → event_sink → job → authenticator → api
var (
	Telemetry       = NewKind[TelemetryExporter](KindInfo{Name: "telemetry", Multi: true, Doc: "tracing/metrics exporters; may also implement kernel.Observer", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	ArtifactStore   = NewKind[artifact.Store](KindInfo{Name: "artifact_store", Doc: "content-addressed blob store for outputs, prompts and recordings", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	Tools           = NewKind[ToolSource](KindInfo{Name: "tools", Multi: true, Doc: "tool sources; each contributes tools to the shared registry", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	Policy          = NewKind[policy.Gate](KindInfo{Name: "policy", Doc: "tool risk policy; framework floor (R2 approval, R3 deny) always applies", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	StreamBus       = NewKind[stream.Bus](KindInfo{Name: "stream_bus", Doc: "ephemeral token/delta stream", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	Broker          = NewKind[MessageBroker](KindInfo{Name: "broker", Doc: "wake-up notification broker (outbox → workers)", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	EventBus        = NewKind[eventbus.Bus](KindInfo{Name: "event_bus", Doc: "committed run event distribution to API replicas; the durable log stays authoritative", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	ModelProvider   = NewKind[ModelBackend](KindInfo{Name: "model_provider", Multi: true, Doc: "LLM provider adapters with their endpoints and aliases", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	Embedder        = NewKind[memory.Embedder](KindInfo{Name: "embedder", Doc: "text embedder for memory retrieval", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	MemoryRetriever = NewKind[kernel.MemoryRetriever](KindInfo{Name: "memory_retriever", Doc: "retrieve_memory step implementation (optional)", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	Strategy        = NewKind[StrategyBuilder](KindInfo{Name: "strategy", Multi: true, Doc: "reasoning strategies, selected by profile spec.strategy", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	Grader          = NewKind[eval.Grader](KindInfo{Name: "grader", Doc: "offline-evaluation answer grader", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	Job             = NewKind[Task](KindInfo{Name: "job", Multi: true, Doc: "background jobs; run in the scheduler role", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	Authenticator   = NewKind[auth.Authenticator](KindInfo{Name: "authenticator", Multi: true, Doc: "API authenticators, tried in order", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	API             = NewKind[api.Extension](KindInfo{Name: "api", Multi: true, Doc: "HTTP route extensions with declared route roles", ContractVersion: "v1.0.0", AllowedServices: []string{}})

	ProfileSource    = NewKind[profile.Source](KindInfo{Name: "profile_source", Multi: true, Doc: "agent profile versions registered at startup (immutable per version)", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	RoutePolicy      = NewKind[gw.Scorer](KindInfo{Name: "route_policy", Doc: "soft score for model endpoints; hard routing filters always apply", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	ContextAssembler = NewKind[cognitive.ContextAssembler](KindInfo{Name: "context_assembler", Doc: "renders reasoning input into model requests + prompt manifests; trust rules always apply", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	Compactor        = NewKind[kernel.Compactor](KindInfo{Name: "compactor", Doc: "summarizes old observations; the kernel always appends safety facts", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	EventSink        = NewKind[eventsink.Sink](KindInfo{Name: "event_sink", Multi: true, Doc: "at-least-once delivery of committed run events (webhooks, chat, paging)", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	ContextProvider  = NewKind[envctx.Provider](KindInfo{Name: "context_provider", Multi: true, Doc: "environment facts (topology, ownership, on-call, recent changes) pinned per run as untrusted data", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	Sandbox          = NewKind[sandbox.Backend](KindInfo{Name: "sandbox", Doc: "isolated per-run execution environment behind harness.sandbox.exec (optional)", ContractVersion: "v1.0.0", AllowedServices: []string{}})
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
