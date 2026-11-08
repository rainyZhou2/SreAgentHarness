package cognitive

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
)

// DeltaPublisher receives token-level events for live display. It is
// best-effort: the decision artifact, not the delta stream, is the record.
type DeltaPublisher interface {
	PublishDelta(tenantID, runID, stepID, modelRequestID string, ev gw.Event)
}

// LLM is the model-backed strategy: Context Assembler → manifest artifact →
// Model Gateway (ledgered, fenced) → validated Decision.
type LLM struct {
	Gateway   *gw.Gateway
	Artifacts artifact.Store
	Assembler ContextBuilder
	Alias     string
	Deltas    DeltaPublisher
}

func (l *LLM) Name() string { return "llm:" + l.Alias }

func (l *LLM) Decide(ctx context.Context, in ReasoningInput) (domain.Decision, error) {
	req, man := l.Assembler.Build(in, l.Alias)
	scope := gw.Scope{TenantID: in.Call.TenantID, RunID: in.Call.RunID, StepID: in.Call.StepID,
		StepAttemptID: in.Call.AttemptID, WorkerID: in.Call.WorkerID, FenceToken: in.Call.FenceToken,
		ContextSnapshotID: in.ContextSnapshotID, Priority: in.Call.Priority}
	if ep, err := l.Gateway.Resolve(req, scope); err == nil {
		man.ChatTemplateVersion = ep.ChatTemplateVersion
	}
	mj, _ := json.Marshal(man)
	manRef, _, err := l.Artifacts.Put(ctx, in.Call.TenantID, "prompt-manifest", mj)
	if err != nil {
		return domain.Decision{}, domain.Retryable(fmt.Errorf("store manifest: %w", err), 0)
	}
	scope.ManifestRef = manRef

	var sink gw.Sink
	if l.Deltas != nil {
		sink = func(ev gw.Event) {
			l.Deltas.PublishDelta(in.Call.TenantID, in.Call.RunID, in.Call.StepID, ev.ModelRequestID, ev)
		}
	}
	resp, err := l.Gateway.Generate(ctx, scope, req, sink)
	if err != nil {
		return domain.Decision{}, mapGatewayError(err)
	}
	d, err := decisionFromResponse(resp)
	if err != nil {
		// Structurally unusable output: re-ask (bounded by the step's attempts).
		return domain.Decision{}, domain.Retryable(err, 0)
	}
	d.ModelRequestID, d.ManifestRef, d.RenderedRequestHash = resp.ModelRequestID, manRef, man.RenderedRequestHash
	d.Model = resp.ResolvedModel
	d.MemoryRefs = man.MemoryRefs
	// EvidenceRefs are the model's own citations only; the evidence it was
	// shown is recorded in the manifest. Filling them in would let an
	// uncited conclusion pass the citation and verifier checks.
	return d, nil
}

// mapGatewayError turns gateway failures into kernel failure classes. A model
// call has no external side effect, so anything retryable is a plain retry.
func mapGatewayError(err error) error {
	ge, ok := gw.AsError(err)
	if !ok {
		return domain.Retryable(err, 0)
	}
	switch {
	case ge.Code == gw.ErrFenceLost:
		return fmt.Errorf("%w: %v", domain.ErrLeaseLost, ge)
	case ge.Code == gw.ErrCancelled:
		return context.Canceled
	case ge.Code == gw.ErrBudgetExceeded:
		return domain.Retryable(ge, 5*time.Minute)
	case ge.Retryable():
		return domain.Retryable(ge, ge.RetryAfter)
	}
	return ge // permanent: policy rejection, capability mismatch, refusal, context window
}
