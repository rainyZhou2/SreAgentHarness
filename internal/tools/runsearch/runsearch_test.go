package runsearchtool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/runsearch"
	"go-sre-harness/internal/tools"
)

type fake struct{ runs map[string]domain.Run }

func (f fake) ListRuns(_ context.Context, _ string, st domain.RunStatus, _ int) ([]domain.Run, error) {
	var out []domain.Run
	for _, r := range f.runs {
		if r.Status == st {
			out = append(out, r)
		}
	}
	return out, nil
}
func (f fake) GetRun(_ context.Context, _, id string) (domain.Run, error) {
	r, ok := f.runs[id]
	if !ok {
		return domain.Run{}, domain.ErrNotFound
	}
	return r, nil
}
func (f fake) Get(_ context.Context, _, ref string) ([]byte, error) {
	switch {
	case strings.HasPrefix(ref, "goal/"):
		return json.Marshal(map[string]any{"goal": "checkout pool exhaustion " + ref})
	case strings.HasPrefix(ref, "res/"):
		return json.Marshal(map[string]any{"answer": "DB pool exhausted"})
	}
	return nil, fmt.Errorf("missing")
}

func newTool() Tool {
	alice, bob := "alice", "bob"
	res := "res/a"
	f := fake{runs: map[string]domain.Run{
		"caller": {ID: "caller", TenantID: "t", InitiatorType: "user", InitiatorID: &alice, Status: domain.RunRunning},
		"a":      {ID: "a", TenantID: "t", InitiatorType: "user", InitiatorID: &alice, Status: domain.RunCompleted, GoalRef: "goal/a", ResultRef: &res, CreatedAt: time.Now()},
		"b":      {ID: "b", TenantID: "t", InitiatorType: "user", InitiatorID: &bob, Status: domain.RunCompleted, GoalRef: "goal/b", ResultRef: &res, CreatedAt: time.Now()},
	}}
	return Tool{Search: &runsearch.Searcher{Runs: f, Artifacts: f}, Runs: f}
}

func call(t *testing.T, tool Tool, args string) (Output, error) {
	t.Helper()
	ctx := tools.WithCallInfo(context.Background(), tools.CallInfo{TenantID: "t", RunID: "caller"})
	raw, err := tool.Invoke(ctx, "k", json.RawMessage(args))
	var out Output
	if err == nil {
		err = json.Unmarshal(raw, &out)
	}
	return out, err
}

func TestInvokeScopesToTheCallingUser(t *testing.T) {
	out, err := call(t, newTool(), `{"query":"pool exhaustion"}`)
	if err != nil || len(out.Hits) != 1 || out.Hits[0].RunID != "a" {
		t.Fatalf("alice must see only her own run: %v %+v", err, out)
	}
	if !strings.Contains(out.Note, "not evidence") {
		t.Fatalf("results must be labelled as history: %q", out.Note)
	}
}

func TestInvokeValidation(t *testing.T) {
	if _, err := call(t, newTool(), `{"query":"  "}`); err == nil {
		t.Fatal("an empty query must be rejected")
	}
	if _, err := call(t, newTool(), `not json`); err == nil {
		t.Fatal("bad arguments must be rejected")
	}
	if _, err := (Tool{}).Invoke(context.Background(), "k", json.RawMessage(`{"query":"x"}`)); err == nil {
		t.Fatal("an unconfigured tool must fail clearly")
	}
	if _, err := newTool().Invoke(context.Background(), "k", json.RawMessage(`{"query":"x"}`)); err == nil {
		t.Fatal("a call without a tenant must be rejected")
	}
}

func TestDescriptorIsReadOnlyBuiltin(t *testing.T) {
	d := Tool{}.Descriptor()
	if d.Risk != domain.RiskR0 || !tools.IsBuiltin(d.Name) || d.Trust != "T3" {
		t.Fatalf("%+v", d)
	}
}
