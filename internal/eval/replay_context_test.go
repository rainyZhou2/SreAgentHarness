package eval

import (
	"context"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/replay"
	"go-sre-harness/internal/tools"
)

type projectionCheckingStrategy struct{}

func (projectionCheckingStrategy) Name() string { return "projection-check" }
func (projectionCheckingStrategy) Decide(ctx context.Context, _ cognitive.ReasoningInput) (domain.Decision, error) {
	if _, err := replay.AuthorizerFromContext(ctx); err != nil {
		return domain.Decision{}, err
	}
	return domain.Decision{Kind: domain.DecisionFinal, Answer: "done"}, nil
}

func TestEvalReplayRunsInsideProjectionContext(t *testing.T) {
	replayer := Replayer{Registry: tools.NewRegistry(), Guard: replay.NewGuard(nil, replay.NewMemoryRecorder())}
	result := replayer.Replay(context.Background(), projectionCheckingStrategy{}, Case{Key: "case", Goal: "goal"})
	if !result.Completed || result.Answer != "done" || result.Error != "" {
		t.Fatalf("result=%+v", result)
	}
}
