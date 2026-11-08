package kernel

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

type countingStore struct {
	artifact.Store
	gets atomic.Int32
}

func (c *countingStore) Get(ctx context.Context, tenantID, ref string) ([]byte, error) {
	c.gets.Add(1)
	return c.Store.Get(ctx, tenantID, ref)
}

func TestCachedStore(t *testing.T) {
	ctx := context.Background()
	base := &countingStore{Store: artifact.NewMemory()}
	var hits atomic.Int32
	cs := newCachedStore(base, 100, func(e string) {
		if e == "artifact_cache_hit" {
			hits.Add(1)
		}
	})
	ref, _, _ := cs.Put(ctx, "t1", "k", []byte("0123456789"))
	for i := 0; i < 3; i++ {
		if b, err := cs.Get(ctx, "t1", ref); err != nil || string(b) != "0123456789" {
			t.Fatal(b, err)
		}
	}
	if base.gets.Load() != 0 || hits.Load() != 3 {
		t.Fatalf("gets %d hits %d", base.gets.Load(), hits.Load())
	}
	if _, err := cs.Get(ctx, "t2", ref); err == nil {
		t.Fatal("another tenant read a cached artifact")
	}
	// Eviction keeps the cache within its bound.
	for i := 0; i < 20; i++ {
		_, _, _ = cs.Put(ctx, "t1", "k", []byte(strings.Repeat(string(rune('a'+i)), 10)))
	}
	if c := cs.(*cachedStore); c.size > 100 || len(c.m) > 10 {
		t.Fatalf("size %d entries %d", c.size, len(c.m))
	}
	if newCachedStore(base, -1, nil) != artifact.Store(base) {
		t.Fatal("negative size must disable the cache")
	}
}

func TestModelAliasRouting(t *testing.T) {
	w := &Worker{cfg: Config{RoutineModelAlias: "small", EscalatedModelAlias: "large"}}
	cases := []struct {
		in        cognitive.ReasoningInput
		stalled   bool
		converged bool
		alias     string
	}{
		{cognitive.ReasoningInput{}, false, false, "large"},
		{cognitive.ReasoningInput{HasToolSteps: true}, false, false, "small"},
		{cognitive.ReasoningInput{HasToolSteps: true}, true, false, "large"},
		{cognitive.ReasoningInput{HasToolSteps: true}, false, true, "large"},
		{cognitive.ReasoningInput{HasToolSteps: true, ForceConclude: true}, false, false, "large"},
		{cognitive.ReasoningInput{HasToolSteps: true, Repair: "x"}, false, false, "large"},
	}
	for i, c := range cases {
		if a, _ := w.modelAlias(c.in, c.stalled, c.converged); a != c.alias {
			t.Errorf("case %d: %s want %s", i, a, c.alias)
		}
	}
	if a, _ := (&Worker{}).modelAlias(cognitive.ReasoningInput{}, true, true); a != "" {
		t.Fatal("routing without aliases must keep the strategy alias")
	}
	llm := &cognitive.LLM{Alias: "default"}
	if s := withAlias(llm, "large").(*cognitive.LLM); s.Alias != "large" || llm.Alias != "default" {
		t.Fatal("withAlias must copy")
	}
}

func TestConvergence(t *testing.T) {
	e := func(id, st string, c float64) cognitive.LedgerEntry {
		return cognitive.LedgerEntry{ID: id, Status: st, Confidence: c}
	}
	if id, ok := convergence([]cognitive.LedgerEntry{e("h1", "VERIFIED", 0.9), e("h2", "REFUTED", 0.1), e("h3", "UNVERIFIED", 0.1)}); !ok || id != "h1" {
		t.Fatal("settled ledger not detected")
	}
	for _, l := range [][]cognitive.LedgerEntry{
		{e("h1", "VERIFIED", 0.7)},
		{e("h1", "VERIFIED", 0.9), e("h2", "UNVERIFIED", 0.3)},
		{e("h1", "UNVERIFIED", 0.95)},
		nil,
	} {
		if _, ok := convergence(l); ok {
			t.Errorf("%v reported as converged", l)
		}
	}
}

func TestSelectToolsStable(t *testing.T) {
	var vis []tools.Descriptor
	for _, n := range []string{"a.one", "b.two", "kafka.lag", "c.three", "d.four", LoadToolsName} {
		vis = append(vis, tools.Descriptor{Name: n, Description: n + " tool."})
	}
	shown, more := selectTools(vis, "kafka lag is high", map[string]bool{"d.four": true}, 3)
	names := map[string]bool{}
	for _, d := range shown {
		names[d.Name] = true
	}
	if !names["kafka.lag"] || !names["d.four"] || !names[LoadToolsName] || len(more) != 3 {
		t.Fatalf("shown %v more %v", names, more)
	}
	shown2, _ := selectTools(vis, "kafka lag is high", map[string]bool{"d.four": true}, 3)
	for i := range shown {
		if shown[i].Name != shown2[i].Name {
			t.Fatal("selection is not deterministic")
		}
	}
	all, none := selectTools(vis, "x", nil, 10)
	if len(none) != 0 || len(all) != 5 {
		t.Fatalf("no selection under k: %d %d", len(all), len(none))
	}
}

func TestSpeculationTake(t *testing.T) {
	var s speculation
	started := s.start("k", "1", domain.RiskR0, func() (json.RawMessage, error) { return json.RawMessage(`{"x":1}`), nil })
	if !started || s.start("k", "1", domain.RiskR0, nil) {
		t.Fatal("duplicate speculation started")
	}
	if _, ok := s.take(context.Background(), "k", "2", domain.RiskR0); ok {
		t.Fatal("result of another tool version reused")
	}
	s.start("k", "1", domain.RiskR0, func() (json.RawMessage, error) { return json.RawMessage(`{"x":1}`), nil })
	if out, ok := s.take(context.Background(), "k", "1", domain.RiskR0); !ok || string(out) != `{"x":1}` {
		t.Fatal("speculative result not taken")
	}
	if _, ok := s.take(context.Background(), "k", "1", domain.RiskR0); ok {
		t.Fatal("result taken twice")
	}
}

func TestJSONDigest(t *testing.T) {
	body := `{"status":"degraded","pods":[1,2,3],"meta":{"b":1,"a":2},"note":"` + strings.Repeat("x", 500) + `"}`
	d := jsonDigest(body, 100)
	for _, want := range []string{"status=\"degraded\"", "pods=[3 items]", "meta={a,b}", "note="} {
		if !strings.Contains(d, want) {
			t.Errorf("missing %q in %s", want, d)
		}
	}
	if jsonDigest(`{"a":1}`, 100) != "" || jsonDigest("plain text "+strings.Repeat("y", 200), 100) != "" {
		t.Fatal("short or non-object bodies are left alone")
	}
}
