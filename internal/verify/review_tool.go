package verify

import (
	"context"
	"encoding/json"
	"errors"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

// ReviewToolName is the kernel-owned step that hands a rejected conclusion
// back to the agent. Its result is the verifier report the kernel wrote into
// the arguments, so it is durable and replayable like any tool result.
const ReviewToolName = tools.BuiltinPrefix + "conclusion_review"

// ReviewTool echoes the kernel's review. The kernel hides it from the model
// and refuses calls that it did not plan.
type ReviewTool struct{}

func (ReviewTool) Descriptor() tools.Descriptor {
	return tools.Descriptor{Name: ReviewToolName, Version: "1", Risk: domain.RiskR0, Trust: "T2", Source: "native",
		Description: "Kernel-owned: independent review of a proposed conclusion.",
		InputSchema: json.RawMessage(`{"type":"object"}`)}
}

func (ReviewTool) Invoke(_ context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	if len(args) == 0 || !json.Valid(args) {
		return nil, errors.New("conclusion review needs the review document")
	}
	return args, nil
}
