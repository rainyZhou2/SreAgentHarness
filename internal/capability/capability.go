// Package capability is the domain SDK of the control plane (RFC §15.1):
// a domain module (SRE, AI Infra) contributes event sources and correlation
// rules, world collectors, read tools, action executors and outcome metric
// sources through one contract, and never touches the framework's ledgers,
// policy or approvals.
//
// The framework enforces the hard constraints of §15.5 when a capability
// registers, not by convention:
//
//   - risk is operator-owned: a capability may declare a risk class, the
//     operator's floor raises it, nothing lowers it, and R3 actions are
//     refused outright;
//   - tools contributed by a domain are read-only (R0); every change goes
//     through an action (preflight, approval, ledger, verification);
//   - facts a domain asserts carry its source name and at most first-party
//     trust (T2) unless the operator marks the capability authoritative;
//   - names are unique across capabilities, so a module cannot shadow
//     another's action or metric source.
package capability

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// Capability is a domain module (RFC §15.1 DomainCapability).
type Capability interface {
	ID() string
	Version() string
	RegisterEvents(EventRegistry) error
	RegisterWorld(WorldRegistry) error
	RegisterTools(ToolRegistry) error
	RegisterActions(ActionRegistry) error
	RegisterOutcomes(OutcomeRegistry) error
}

// TemplateProvider is optionally implemented by a capability that ships
// scenario Work templates (e.g. an inference-latency incident). Operator
// templates with the same id win; templates are validated at registration.
type TemplateProvider interface {
	WorkTemplates() []work.Template
}

// Base provides no-op registrations; embed it and override what applies.
type Base struct{}

func (Base) RegisterEvents(EventRegistry) error     { return nil }
func (Base) RegisterWorld(WorldRegistry) error      { return nil }
func (Base) RegisterTools(ToolRegistry) error       { return nil }
func (Base) RegisterActions(ActionRegistry) error   { return nil }
func (Base) RegisterOutcomes(OutcomeRegistry) error { return nil }

// EventRegistry receives event sources and correlation rules.
type EventRegistry interface {
	// AddSource registers a webhook normalizer for a source name; the
	// harness serves it at POST /v1/sources/{source}/events.
	AddSource(source string, n Normalizer) error
	// AddRule adds a default correlation rule; an operator rule with the
	// same name replaces it.
	AddRule(r eventing.Rule) error
}

// Normalizer turns a source-native webhook body into event envelopes. It
// runs on untrusted input: it must bound what it reads and never trust
// tenant ids from the body (the caller's tenant is passed in).
type Normalizer interface {
	Normalize(ctx context.Context, tenantID string, h http.Header, body []byte, now time.Time) ([]eventing.Envelope, error)
}

// NormalizerFunc adapts a function.
type NormalizerFunc func(ctx context.Context, tenantID string, h http.Header, body []byte, now time.Time) ([]eventing.Envelope, error)

func (f NormalizerFunc) Normalize(ctx context.Context, tenantID string, h http.Header, body []byte, now time.Time) ([]eventing.Envelope, error) {
	return f(ctx, tenantID, h, body, now)
}

// WorldRegistry receives collectors that feed the world state.
type WorldRegistry interface {
	AddCollector(name string, every time.Duration, c Collector) error
}

// Collector observes a domain system. It is called periodically; what it
// returns is asserted for the capability's tenant. Relations a collector
// reported before and no longer reports are retracted (the edge's valid
// interval is closed), so topology follows the system.
type Collector interface {
	Collect(ctx context.Context, now time.Time) (Observation, error)
}

// CollectorFunc adapts a function.
type CollectorFunc func(ctx context.Context, now time.Time) (Observation, error)

func (f CollectorFunc) Collect(ctx context.Context, now time.Time) (Observation, error) {
	return f(ctx, now)
}

// Observation is one collection.
type Observation struct {
	Facts     []world.Fact
	Relations []world.Relation
	// Events are submitted to the inbox (e.g. a node turned NotReady).
	Events []eventing.Envelope
}

// ToolRegistry receives read tools.
type ToolRegistry interface{ AddTool(t tools.Tool) error }

// ActionRegistry receives action executors.
type ActionRegistry interface{ AddAction(e action.Executor) error }

// OutcomeRegistry receives metric sources for outcome verification.
type OutcomeRegistry interface {
	AddMetricSource(name string, s outcome.MetricSource) error
}

// ErrInvalid marks a registration refused by the SDK.
var ErrInvalid = errors.New("capability: invalid registration")

var nameRe = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)

func checkName(kind, name string) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("%w: %s name %q (lowercase, [a-z0-9_.-], max 64)", ErrInvalid, kind, name)
	}
	return nil
}

// riskRank orders risk classes.
func riskRank(r domain.RiskClass) int {
	switch r {
	case domain.RiskR0:
		return 0
	case domain.RiskR1:
		return 1
	case domain.RiskR2:
		return 2
	case domain.RiskR3:
		return 3
	}
	return -1
}

// MaxRisk returns the higher of two risk classes.
func MaxRisk(a, b domain.RiskClass) domain.RiskClass {
	if riskRank(b) > riskRank(a) {
		return b
	}
	return a
}

// riskExecutor overrides the declared risk of an executor with the
// operator's (higher) class.
type riskExecutor struct {
	action.Executor
	risk domain.RiskClass
}

func (r riskExecutor) Descriptor() action.Descriptor {
	d := r.Executor.Descriptor()
	d.Risk = r.risk
	return d
}
