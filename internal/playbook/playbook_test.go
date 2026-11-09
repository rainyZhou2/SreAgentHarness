package playbook

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
)

var ctx = context.Background()

type fakeWorks struct {
	works map[string]work.Work
	evals map[string][]outcome.Evaluation
	acts  map[string][]action.Record
	links map[string][]work.RunLink
}

func (f *fakeWorks) GetWork(_ context.Context, _, id string) (work.Work, error) {
	w, ok := f.works[id]
	if !ok {
		return w, work.ErrNotFound
	}
	return w, nil
}
func (f *fakeWorks) ListWorks(context.Context, work.Filter) ([]work.Work, error) { return nil, nil }
func (f *fakeWorks) ListRunLinks(_ context.Context, _, id string) ([]work.RunLink, error) {
	return f.links[id], nil
}
func (f *fakeWorks) ListOutcomes(_ context.Context, _, id string, _ int) ([]outcome.Evaluation, error) {
	return f.evals[id], nil
}
func (f *fakeWorks) ListActions(_ context.Context, _, id string) ([]action.Record, error) {
	return f.acts[id], nil
}

func (f *fakeWorks) RecentlyClosedWorks(context.Context, time.Time, int) ([]work.Work, error) {
	var out []work.Work
	for _, w := range f.works {
		if w.Status.Terminal() {
			out = append(out, w)
		}
	}
	return out, nil
}

// ledger is the local memory store with run hits supplied by the test.
type ledger struct {
	*localstore.Store
	hits map[string][]string
}

func (l ledger) RunMemoryIDs(_ context.Context, _, runID string) ([]string, error) {
	return l.hits[runID], nil
}

func setup(t *testing.T) (*Service, *fakeWorks, ledger) {
	t.Helper()
	s, err := localstore.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	l := ledger{Store: s, hits: map[string][]string{}}
	fw := &fakeWorks{works: map[string]work.Work{}, evals: map[string][]outcome.Evaluation{}, acts: map[string][]action.Record{},
		links: map[string][]work.RunLink{}}
	w := memory.Writer{Embedder: memory.HashingEmbedder{Dims: 32}, Artifacts: artifact.NewMemory()}
	return &Service{Works: fw, Memories: l, Prepare: w.Prepare}, fw, l
}

func addWork(fw *fakeWorks, id string, status work.Status, verdict outcome.Verdict, specHash string, runs ...string) {
	fw.works[id] = work.Work{ID: id, TenantID: "t", Kind: work.KindIncident, Status: status, Title: "checkout 5xx after deploy",
		Template: work.Template{ID: "incident"},
		Goal: work.GoalSpec{Intent: "restore checkout", SubjectRefs: []string{"deployment://prod/checkout"},
			Labels: map[string]string{"environment": "prod"}}}
	if verdict != "" {
		v := 0.002
		fw.evals[id] = []outcome.Evaluation{{ID: "ev-" + id, SpecHash: specHash, Verdict: verdict,
			Objectives:   []outcome.ObjectiveResult{{ID: "error_rate", Kind: "metric", Verdict: verdict, Value: &v, Reason: "0.2% < 1%"}},
			EvidenceRefs: []string{"prom://q"}}}
	}
	args, _ := json.Marshal(map[string]string{"to_image": "checkout:v1"})
	fw.acts[id] = []action.Record{{Plan: action.Plan{ActionName: "deployment.rollback", TargetRef: "deployment://prod/checkout",
		Risk: domain.RiskR2, Args: args}, State: action.StateVerified}}
	for _, r := range runs {
		fw.links[id] = append(fw.links[id], work.RunLink{RunID: r})
	}
}

func TestProposeRequiresVerifiedSuccess(t *testing.T) {
	svc, fw, _ := setup(t)
	addWork(fw, "w-open", work.StatusActive, "", "")
	addWork(fw, "w-noeval", work.StatusSucceeded, "", "")
	addWork(fw, "w-failed", work.StatusFailed, outcome.NotSatisfied, "sha256:x")
	for _, id := range []string{"w-open", "w-noeval", "w-failed"} {
		if _, err := svc.Propose(ctx, ProposeRequest{TenantID: "t", WorkID: id, ActorID: "alice"}); !errors.Is(err, ErrNotVerified) {
			t.Errorf("%s: %v", id, err)
		}
	}
}

func TestProposePromoteAndFeedback(t *testing.T) {
	svc, fw, l := setup(t)
	addWork(fw, "w1", work.StatusSucceeded, outcome.Satisfied, "sha256:spec", "run-1")
	m, err := svc.Propose(ctx, ProposeRequest{TenantID: "t", WorkID: "w1", ActorID: "alice",
		VersionConstraints: []string{"kubernetes>=1.28"}})
	if err != nil {
		t.Fatal(err)
	}
	a := m.Applicability
	if m.Status != "CANDIDATE" || m.Type != "procedural" || a == nil || a.Kind != "playbook" || a.RiskOfReuse != "high" ||
		a.SourceWorkIDs[0] != "w1" || a.Services[0] != "checkout" || a.Environments[0] != "prod" || a.SourceRunIDs[0] != "run-1" {
		t.Fatalf("candidate: %+v / %+v", m, a)
	}
	if m.Verification.SuccessCount != 1 || !strings.Contains(m.Content, "deployment.rollback on deployment://prod/checkout") ||
		!strings.Contains(m.Content, "error_rate") {
		t.Fatalf("content/verification: %+v\n%s", m.Verification, m.Content)
	}
	// Proposing again does not double count the source Work.
	again, err := svc.Propose(ctx, ProposeRequest{TenantID: "t", WorkID: "w1", ActorID: "alice",
		VersionConstraints: []string{"kubernetes>=1.28"}})
	if err != nil || again.ID != m.ID || again.Verification.SuccessCount != 1 {
		t.Fatalf("re-propose: %+v %v", again.Verification, err)
	}
	// The proposer cannot promote it; another reviewer can.
	if _, err := l.TransitionMemory(ctx, "t", m.ID, "CANDIDATE", "ACTIVE", "alice", "lgtm"); err == nil {
		t.Fatal("self-promotion accepted")
	}
	if _, err := l.TransitionMemory(ctx, "t", m.ID, "CANDIDATE", "ACTIVE", "bob", "lgtm"); err != nil {
		t.Fatalf("promotion: %v", err)
	}

	// Later Works that retrieved the playbook update its record from
	// their verified outcomes, once each.
	l.hits["run-2"] = []string{m.ID}
	l.hits["run-3"] = []string{m.ID}
	l.hits["run-4"] = []string{m.ID}
	addWork(fw, "w2", work.StatusSucceeded, outcome.Satisfied, "sha256:spec", "run-2")
	addWork(fw, "w3", work.StatusFailed, outcome.NotSatisfied, "human", "run-3")
	addWork(fw, "w4", work.StatusCancelled, outcome.Inconclusive, "sha256:spec", "run-4")
	for _, id := range []string{"w2", "w3", "w4", "w2"} {
		if _, err := svc.WorkClosed(ctx, fw.works[id]); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := svc.Sweep(ctx, fw, time.Time{}, 100); err != nil || n != 0 {
		t.Fatalf("sweep recounted: %d %v", n, err)
	}
	got, _ := l.GetMemory(ctx, "t", m.ID)
	if got.Verification.SuccessCount != 2 || got.Verification.FailureCount != 1 || got.Verification.LastVerifiedAt == nil {
		t.Fatalf("verification: %+v", got.Verification)
	}
	// Plain memories retrieved by a run are not scored.
	plain, _, err := l.CreateMemory(ctx, memory.NewMemory{TenantID: "t", Scope: "organization", Type: "semantic", Status: "ACTIVE",
		Subject: "s", Title: "t", Content: "c", ContentHash: "h1", CreatedBy: "x"})
	if err != nil {
		t.Fatal(err)
	}
	l.hits["run-5"] = []string{plain.ID}
	addWork(fw, "w5", work.StatusSucceeded, outcome.Satisfied, "sha256:spec", "run-5")
	if n, _ := svc.WorkClosed(ctx, fw.works["w5"]); n != 0 {
		t.Fatalf("plain memory scored: %d", n)
	}
}

func TestVerifiedVerdict(t *testing.T) {
	ok := []outcome.Evaluation{{ID: "e", Verdict: outcome.Satisfied}}
	if v, has := VerifiedVerdict(work.Work{Status: work.StatusSucceeded}, ok); !has || !v.Success || v.Source != "machine" {
		t.Fatal("success")
	}
	if _, has := VerifiedVerdict(work.Work{Status: work.StatusFailed}, ok); has {
		t.Fatal("failed Work with a satisfied evaluation has no verdict")
	}
	if _, has := VerifiedVerdict(work.Work{Status: work.StatusSucceeded}, nil); has {
		t.Fatal("no evaluation")
	}
}
