package workclient

import (
	"go-sre-harness/internal/governance"
	"go-sre-harness/internal/observability"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/protocol"
	"go-sre-harness/internal/quota"
	"go-sre-harness/internal/work"
)

// Re-exported wire types.
type (
	// Event is one entry of the stable event contract.
	Event         = protocol.Event
	ProtocolError = protocol.Error
	View          = protocol.View
	Graph         = work.Graph
	CreateChild   = orchestration.ChildRequest
	CreateCause   = orchestration.CreateCause
	Improvement   = orchestration.Improvement
	Finding       = orchestration.Finding
	Source        = observability.Source
	Query         = observability.Query
	QueryResult   = observability.Result
	SourceProbe   = observability.Health
	Attestation   = governance.Attestation
	Budget        = quota.Budget
	Intent        = orchestration.Intent
	DryRunPlan    = orchestration.DryRunPlan
)
