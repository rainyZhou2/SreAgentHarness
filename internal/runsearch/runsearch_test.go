package runsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

type fakeStore struct {
	mu    sync.Mutex
	runs  []domain.Run
	blobs map[string][]byte
	gets  int
}

func (f *fakeStore) ListRuns(_ context.Context, tenant string, st domain.RunStatus, limit int) ([]domain.Run, error) {
	var out []domain.Run
	for _, r := range f.runs {
		if r.TenantID == tenant && r.Status == st {
			out = append(out, r)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeStore) Get(_ context.Context, _, ref string) ([]byte, error) {
	f.mu.Lock()
	f.gets++
	f.mu.Unlock()
	b, ok := f.blobs[ref]
	if !ok {
		return nil, fmt.Errorf("missing %s", ref)
	}
	return b, nil
}

var t0 = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

func (f *fakeStore) add(id, goal, answer string, mod func(*domain.Run)) {
	f.addH(id, goal, answer, "pool exhaustion", mod)
}

func (f *fakeStore) addH(id, goal, answer, hyp string, mod func(*domain.Run)) {
	r := domain.Run{ID: id, TenantID: "t", Status: domain.RunCompleted, GoalRef: "goal/" + id, InitiatorType: "user",
		CreatedAt: t0.Add(-time.Duration(len(f.runs)) * time.Hour)}
	u := "alice"
	r.InitiatorID = &u
	if answer != "" {
		ref := "res/" + id
		r.ResultRef = &ref
		f.blobs[ref], _ = json.Marshal(map[string]any{"answer": answer,
			"hypotheses": []map[string]any{{"statement": hyp, "status": "VERIFIED", "confidence_estimate": 0.8}}})
	}
	f.blobs["goal/"+id], _ = json.Marshal(map[string]any{"goal": goal})
	if mod != nil {
		mod(&r)
	}
	f.runs = append(f.runs, r)
}

func newSearcher() (*Searcher, *fakeStore) {
	f := &fakeStore{blobs: map[string][]byte{}}
	return &Searcher{Runs: f, Artifacts: f, Now: func() time.Time { return t0 }}, f
}

func TestRanksRareTermsAndReturnsFields(t *testing.T) {
	s, f := newSearcher()
	f.add("r1", "checkout latency high", "Probable cause: DB connection pool exhausted after deploy d-881", nil)
	f.add("r2", "checkout error rate", "Traffic spike refuted", nil)
	f.addH("r3", "payments timeout", "etcd quorum lost", "etcd outage", nil)
	res, err := s.Search(context.Background(), Query{TenantID: "t", Text: "connection pool exhausted checkout", Caller: Caller{InitiatorType: "user", InitiatorID: "alice"}})
	if err != nil || len(res.Hits) == 0 {
		t.Fatalf("%v %+v", err, res)
	}
	if res.Hits[0].RunID != "r1" || res.Hits[0].Ref != "run:r1" || len(res.Hits[0].Hypotheses) != 1 {
		t.Fatalf("best hit: %+v", res.Hits[0])
	}
	for _, h := range res.Hits {
		if h.RunID == "r3" {
			t.Fatalf("an unrelated run matched: %+v", h)
		}
	}
	if res.Scanned != 3 || res.Partial {
		t.Fatalf("%+v", res)
	}
}

func TestVisibility(t *testing.T) {
	s, f := newSearcher()
	f.add("mine", "pool exhaustion", "a", nil)
	f.add("theirs", "pool exhaustion", "b", func(r *domain.Run) { o := "bob"; r.InitiatorID = &o })
	f.add("trigger", "pool exhaustion alert", "c", func(r *domain.Run) { r.InitiatorType = "trigger"; r.InitiatorID = nil })
	f.add("self", "pool exhaustion", "d", nil)
	f.add("child", "pool exhaustion", "e", func(r *domain.Run) { p := "mine"; r.ParentRunID = &p })
	got := map[string]bool{}
	res, _ := s.Search(context.Background(), Query{TenantID: "t", Text: "pool exhaustion", Limit: 10,
		Caller: Caller{RunID: "self", InitiatorType: "user", InitiatorID: "alice"}})
	for _, h := range res.Hits {
		got[h.RunID] = true
	}
	if !got["mine"] || !got["trigger"] || got["theirs"] || got["self"] || got["child"] {
		t.Fatalf("visible set wrong: %v", got)
	}
	// A caller with no user identity (a trigger-started run) sees no user chats.
	res, _ = s.Search(context.Background(), Query{TenantID: "t", Text: "pool exhaustion", Limit: 10, Caller: Caller{RunID: "x", InitiatorType: "trigger"}})
	for _, h := range res.Hits {
		if h.RunID != "trigger" {
			t.Fatalf("a non-user caller must not see user chats: %+v", h)
		}
	}
}

func TestStatusServiceAndRedaction(t *testing.T) {
	s, f := newSearcher()
	f.add("ok", "checkout slow", "token=sk-ant-abcdefghijklmnopqrstuvwxyz0123456789 pool", nil)
	f.add("bad", "checkout slow", "", func(r *domain.Run) { r.Status = domain.RunFailed })
	f.add("other", "search slow", "pool", nil)
	caller := Caller{InitiatorType: "user", InitiatorID: "alice"}
	res, _ := s.Search(context.Background(), Query{TenantID: "t", Text: "slow pool", Caller: caller, Limit: 10})
	if len(res.Hits) != 2 {
		t.Fatalf("default status is COMPLETED: %+v", res.Hits)
	}
	if strings.Contains(res.Hits[0].Answer+res.Hits[1].Answer, "sk-ant-abc") {
		t.Fatalf("secrets must be redacted: %+v", res.Hits)
	}
	res, _ = s.Search(context.Background(), Query{TenantID: "t", Text: "slow", Status: "any", Service: "checkout", Caller: caller, Limit: 10})
	if len(res.Hits) != 2 {
		t.Fatalf("service filter + any status: %+v", res.Hits)
	}
	res, _ = s.Search(context.Background(), Query{TenantID: "t", Text: "slow", Status: "FAILED", Caller: caller})
	if len(res.Hits) != 1 || res.Hits[0].RunID != "bad" {
		t.Fatalf("%+v", res.Hits)
	}
}

func TestCachesAndBounds(t *testing.T) {
	s, f := newSearcher()
	for i := 0; i < 5; i++ {
		f.add(fmt.Sprintf("r%d", i), "pool exhaustion", "answer", nil)
	}
	q := Query{TenantID: "t", Text: "pool", Caller: Caller{InitiatorType: "user", InitiatorID: "alice"}}
	_, _ = s.Search(context.Background(), q)
	first := f.gets
	_, _ = s.Search(context.Background(), q)
	if f.gets != first {
		t.Fatalf("finished runs are immutable: the second search must read nothing (%d then %d)", first, f.gets)
	}
	s.Cfg.MaxScan = 3
	res, _ := s.Search(context.Background(), q)
	if !res.Partial || res.Scanned > 3 {
		t.Fatalf("the scan bound must be reported: %+v", res)
	}
}

func TestCJKAndEmptyQuery(t *testing.T) {
	s, f := newSearcher()
	f.add("zh", "结账服务延迟升高", "数据库连接池耗尽", nil)
	f.add("en", "unrelated", "nothing", nil)
	caller := Caller{InitiatorType: "user", InitiatorID: "alice"}
	res, _ := s.Search(context.Background(), Query{TenantID: "t", Text: "连接池", Caller: caller})
	if len(res.Hits) != 1 || res.Hits[0].RunID != "zh" {
		t.Fatalf("%+v", res.Hits)
	}
	res, err := s.Search(context.Background(), Query{TenantID: "t", Text: " the of ", Caller: caller})
	if err != nil || len(res.Hits) != 0 {
		t.Fatalf("%v %+v", err, res)
	}
}
