package eval

import (
	"context"
	"encoding/json"
	"testing"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/replay"
	"go-sre-harness/internal/tools"
)

type isolationTool struct {
	calls int
}

func (t *isolationTool) Descriptor() tools.Descriptor {
	return tools.Descriptor{Name: "write", Version: "1", Risk: domain.RiskR2}
}
func (t *isolationTool) Invoke(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	t.calls++
	return json.RawMessage(`{"ok":true}`), nil
}

type isolationStrategy struct{}

func (isolationStrategy) Name() string { return "isolation" }
func (isolationStrategy) Decide(context.Context, cognitive.ReasoningInput) (domain.Decision, error) {
	return domain.Decision{Kind: domain.DecisionTool, Tool: &domain.ToolCall{Name: "write", Args: json.RawMessage(`{}`)}}, nil
}

type productionArtifactProbe struct {
	puts int
}

func (p *productionArtifactProbe) Put(context.Context, string, string, []byte) (string, string, error) {
	p.puts++
	return "production://report", "hash", nil
}
func (p *productionArtifactProbe) Get(context.Context, string, string) ([]byte, error) {
	return nil, artifact.ErrNotFound
}

func TestReplayReportUsesIsolatedArtifacts(t *testing.T) {
	production := &productionArtifactProbe{}
	service := &Service{Artifacts: production, ReplayArtifacts: artifact.NewMemory()}
	ref, err := service.putReplayReport(context.Background(), "t", []byte(`{"ok":true}`))
	if err != nil || ref == "" || production.puts != 0 {
		t.Fatalf("ref=%q puts=%d err=%v", ref, production.puts, err)
	}
	service.ReplayArtifacts = nil
	if _, err := service.putReplayReport(context.Background(), "t", []byte(`{}`)); err != nil || production.puts != 0 {
		t.Fatalf("puts=%d err=%v", production.puts, err)
	}
}

func TestEvalReplayCannotInvokeRecordedTool(t *testing.T) {
	probe := &isolationTool{}
	registry := tools.NewRegistry(probe)
	replayer := Replayer{Registry: registry, Guard: replay.NewGuard(nil, replay.NewMemoryRecorder()), Grader: SubstringGrader}
	result := replayer.Replay(context.Background(), isolationStrategy{}, Case{Key: "case", Goal: "goal", Recording: Recording{Calls: []RecordedCall{{Tool: "write", Args: json.RawMessage(`{}`), Outcome: "ok", Output: json.RawMessage(`{}`)}}}})
	if result.SafetyViolations == nil || probe.calls != 0 {
		t.Fatalf("result=%+v calls=%d", result, probe.calls)
	}
}
