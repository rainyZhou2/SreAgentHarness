package workbench

import (
	"context"
	"testing"

	"go-sre-harness/internal/replay"
)

type isolationProbes struct {
	calls int
}

func (p *isolationProbes) Total() int { return p.calls }

func TestWorkbenchReplayHasNoProductionSideEffects(t *testing.T) {
	probes := &isolationProbes{}
	runner := &Runner{Dir: t.TempDir(), Security: replay.NewGuard(nil, replay.NewMemoryRecorder()), Probes: probes}
	result, err := runner.Run(context.Background(), load(t, "01-rollback-verified.json"))
	if err != nil || !result.Passed {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if probes.Total() != 0 {
		t.Fatalf("production side effects=%d", probes.Total())
	}
}
