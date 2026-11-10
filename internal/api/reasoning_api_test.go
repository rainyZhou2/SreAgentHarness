package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/reasoning"
	"go-sre-harness/internal/reasoning/improve"
	"go-sre-harness/internal/store/pg"
)

type fakeReasoningStore struct {
	spec string
	mems map[string]memory.Memory
}

func (f *fakeReasoningStore) ReasoningScores(context.Context, string, reasoning.ScoreFilter) ([]reasoning.RunScore, error) {
	return []reasoning.RunScore{{RunID: "r", MemoryIDs: []string{"m1"}, Metrics: reasoning.Metrics{Completed: true, Conclusion: "VERIFIED"}}}, nil
}
func (f *fakeReasoningStore) GetMemory(_ context.Context, _, id string) (memory.Memory, error) {
	if m, ok := f.mems[id]; ok {
		return m, nil
	}
	return memory.Memory{}, domain.ErrNotFound
}
func (f *fakeReasoningStore) ListCandidates(context.Context, string, string) ([]eval.Candidate, error) {
	return nil, nil
}
func (f *fakeReasoningStore) ActiveVersion(context.Context, string, string) (int64, error) {
	return 2, nil
}
func (f *fakeReasoningStore) ProfileSpec(context.Context, string, string, int64) (json.RawMessage, error) {
	return json.RawMessage(f.spec), nil
}
func (f *fakeReasoningStore) ListCases(context.Context, string, string, string) ([]eval.Case, error) {
	return nil, nil
}
func (f *fakeReasoningStore) ReasoningScore(context.Context, string, string) (reasoning.RunScore, error) {
	return reasoning.RunScore{}, domain.ErrNotFound
}
func (f *fakeReasoningStore) ListMemories(context.Context, string, pg.MemoryFilter) ([]memory.Memory, error) {
	var out []memory.Memory
	for _, m := range f.mems {
		out = append(out, m)
	}
	return out, nil
}

type fakeEvolution struct {
	patch json.RawMessage
	actor string
}

func (e *fakeEvolution) Propose(_ context.Context, _, _ string, _ int64, patch json.RawMessage, _, actor string) (eval.Candidate, error) {
	e.patch, e.actor = patch, actor
	return eval.Candidate{ID: "c1", Status: "PROPOSED", ProposedBy: actor}, nil
}
func (e *fakeEvolution) EvaluateCandidate(context.Context, string, string, string, string) (eval.Candidate, eval.GateResult, error) {
	return eval.Candidate{}, eval.GateResult{}, nil
}

func TestReasoningPatternEndpoints(t *testing.T) {
	pat := memory.Memory{ID: "m1", Type: "procedural", Subject: "api:" + reasoning.PatternSubject, Status: "ACTIVE"}
	st := &fakeReasoningStore{spec: `{"reasoning":{"patterns_excluded":["m1"],"pattern_probe_rate":0.05}}`,
		mems: map[string]memory.Memory{"m1": pat}}
	evo := &fakeEvolution{}
	a := &ReasoningAPI{Store: st, Evolution: evo, Server: &Server{}, Defaults: improveDefaults()}
	mux := http.NewServeMux()
	a.Register(mux)
	do := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{Tenant: "t", Subject: "alice"}))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	rec := do("GET", "/v1/reasoning/patterns", "")
	var list struct {
		Probe    float64       `json:"pattern_probe_rate"`
		Patterns []patternView `json:"patterns"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil || len(list.Patterns) != 1 || !list.Patterns[0].Excluded ||
		list.Probe != 0.05 || list.Patterns[0].Stats == nil {
		t.Fatalf("patterns: %d %s", rec.Code, rec.Body)
	}
	rec = do("POST", "/v1/reasoning/patterns/m1/reinstate", `{"note":"looks fine now"}`)
	if rec.Code != http.StatusCreated || evo.actor != "alice" || !strings.Contains(string(evo.patch), `"reasoning"`) ||
		strings.Contains(string(evo.patch), `"m1"`) {
		t.Fatalf("reinstate: %d %s patch=%s", rec.Code, rec.Body, evo.patch)
	}
	if rec = do("POST", "/v1/reasoning/patterns/m1/exclude", `{}`); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("already excluded: %d %s", rec.Code, rec.Body)
	}
	if rec = do("POST", "/v1/reasoning/patterns/nope/exclude", `{}`); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unknown memory: %d %s", rec.Code, rec.Body)
	}
	roles := a.RouteRoles()
	if roles["POST /v1/reasoning/patterns/{memory_id}/exclude"] != auth.Operator || roles["POST /v1/reasoning/improve"] != auth.Admin {
		t.Fatalf("roles: %v", roles)
	}
}

func improveDefaults() improve.Config { return improve.Config{ProfileID: "p"} }
