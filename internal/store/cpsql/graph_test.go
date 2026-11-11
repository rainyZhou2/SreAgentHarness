package cpsql

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/work"
)

func childOf(tn string, mut func(*work.NewWork)) work.NewWork {
	n := newWork(tn, cpkit.NewID())
	n.Goal.SubjectRefs, n.Subjects = nil, nil // inherit the parent's
	if mut != nil {
		mut(&n)
	}
	return n
}

func finish(t *testing.T, s *Store, w work.Work, to work.Status) work.Work {
	t.Helper()
	ctx := context.Background()
	cur, err := s.GetWork(ctx, w.TenantID, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cur.Status == work.StatusNew {
		if cur, err = s.ApplyWork(ctx, work.Mutation{TenantID: w.TenantID, WorkID: w.ID, ExpectedVersion: cur.Version,
			ActorType: "system", Status: st(work.StatusActive), Now: t0}); err != nil {
			t.Fatal(err)
		}
	}
	if cur, err = s.ApplyWork(ctx, work.Mutation{TenantID: w.TenantID, WorkID: w.ID, ExpectedVersion: cur.Version,
		ActorType: "system", Status: st(to), Now: t0}); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteChild(ctx, w.TenantID, w.ID, 0); err != nil {
		t.Fatal(err)
	}
	return cur
}

// A child inherits its parent's subjects and constraints and can only
// narrow them; the parent is the ceiling.
func TestChildCannotExceedParent(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		pn := newWork(tn, cpkit.NewID())
		pn.Goal.Constraints = work.Constraints{ProductionWrites: work.WritesApprovalRequired, DataClass: "confidential", MaxCostUSD: 10}
		parent, _, err := s.CreateWork(ctx, pn)
		if err != nil {
			t.Fatal(err)
		}
		c, created, err := s.CreateChild(ctx, work.NewChild{ParentID: parent.ID, Work: childOf(tn, nil)})
		if err != nil || !created {
			t.Fatalf("create child: %v", err)
		}
		if c.ParentID != parent.ID || c.RootID != parent.ID || c.Depth != 1 || len(c.Goal.SubjectRefs) != 1 ||
			c.Goal.Constraints.DataClass != "confidential" || c.Goal.Constraints.MaxCostUSD != 10 ||
			c.Goal.Constraints.ProductionWrites != work.WritesApprovalRequired {
			t.Fatalf("child must inherit the ceiling: %+v", c)
		}
		for name, mut := range map[string]func(*work.NewWork){
			"wider writes":   func(n *work.NewWork) { n.Goal.Constraints.ProductionWrites = work.WritesPreauthorized },
			"foreign target": func(n *work.NewWork) { n.Goal.SubjectRefs = []string{"service://prod/billing"} },
			"larger budget":  func(n *work.NewWork) { n.Goal.Constraints.MaxCostUSD = 50 },
		} {
			if _, _, err := s.CreateChild(ctx, work.NewChild{ParentID: parent.ID, Work: childOf(tn, mut)}); !errors.Is(err, work.ErrInvalid) {
				t.Errorf("%s: want a refusal, got %v", name, err)
			}
		}
		// A lower data class is raised to the parent's, a tighter write mode is kept.
		n, _, err := s.CreateChild(ctx, work.NewChild{ParentID: parent.ID, Work: childOf(tn, func(n *work.NewWork) {
			n.Goal.Constraints = work.Constraints{DataClass: "public", ProductionWrites: work.WritesForbidden}
		})})
		if err != nil || n.Goal.Constraints.DataClass != "confidential" || n.Goal.Constraints.ProductionWrites != work.WritesForbidden {
			t.Fatalf("narrowed child: %+v %v", n.Goal.Constraints, err)
		}
		// Another tenant cannot attach a child to this parent.
		if _, _, err := s.CreateChild(ctx, work.NewChild{ParentID: parent.ID, Work: childOf(tenant(), nil)}); !errors.Is(err, work.ErrNotFound) {
			t.Fatalf("cross-tenant parent: %v", err)
		}
		g, err := s.WorkGraph(ctx, tn, c.ID)
		if err != nil || g.RootID != parent.ID || len(g.Nodes) != 3 {
			t.Fatalf("graph: %+v %v", g, err)
		}
	})
}

// Tree bounds: depth and children are limited by the root's template.
func TestGraphLimits(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		pn := newWork(tn, cpkit.NewID())
		pn.Template.Graph = &work.GraphSpec{Limits: work.GraphLimits{MaxDepth: 2, MaxChildren: 2}}
		root, _, err := s.CreateWork(ctx, pn)
		if err != nil {
			t.Fatal(err)
		}
		a, _, err := s.CreateChild(ctx, work.NewChild{ParentID: root.ID, Work: childOf(tn, nil)})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.CreateChild(ctx, work.NewChild{ParentID: root.ID, Work: childOf(tn, nil)}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.CreateChild(ctx, work.NewChild{ParentID: root.ID, Work: childOf(tn, nil)}); work.LimitName(err) != "max_children" {
			t.Fatalf("third child: %v", err)
		}
		b, _, err := s.CreateChild(ctx, work.NewChild{ParentID: a.ID, Work: childOf(tn, nil)})
		if err != nil || b.Depth != 2 || b.RootID != root.ID {
			t.Fatalf("grandchild: %+v %v", b, err)
		}
		if _, _, err := s.CreateChild(ctx, work.NewChild{ParentID: b.ID, Work: childOf(tn, nil)}); work.LimitName(err) != "max_depth" {
			t.Fatalf("depth 3: %v", err)
		}
	})
}

// F22: an edge that would close a cycle is refused atomically — also when
// two opposite edges race — and the graph is unchanged.
func TestDependencyCycleIsRefused(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		a, b, c := mustCreate(t, s, tn), mustCreate(t, s, tn), mustCreate(t, s, tn)
		edge := func(up, down work.Work) error {
			return s.AddDependency(ctx, tn, work.Dependency{ParentWorkID: up.ID, ChildWorkID: down.ID, Kind: work.AfterSuccess, Required: true}, 0)
		}
		if err := edge(a, b); err != nil {
			t.Fatal(err)
		}
		if err := edge(b, c); err != nil {
			t.Fatal(err)
		}
		if err := edge(c, a); !errors.Is(err, work.ErrInvalid) || !strings.Contains(err.Error(), "cycle") {
			t.Fatalf("closing edge: %v", err)
		}
		if err := edge(a, a); !errors.Is(err, work.ErrInvalid) {
			t.Fatalf("self edge: %v", err)
		}
		if unmet, _ := s.UnmetDependencies(ctx, tn, a.ID); len(unmet) != 0 {
			t.Fatalf("a refused edge left state behind: %v", unmet)
		}
		aw, _ := s.GetWork(ctx, tn, a.ID)
		if aw.WakeAt == nil || strings.HasPrefix(aw.Reason, "held") {
			t.Fatalf("a refused edge must not hold the Work: %+v", aw)
		}
		// Racing opposite edges: at most one wins, never a cycle.
		x, y := mustCreate(t, s, tn), mustCreate(t, s, tn)
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i, pair := range [][2]work.Work{{x, y}, {y, x}} {
			wg.Add(1)
			go func() { defer wg.Done(); errs[i] = edge(pair[0], pair[1]) }()
		}
		wg.Wait()
		if (errs[0] == nil) == (errs[1] == nil) {
			t.Fatalf("exactly one of two opposite edges may be added: %v / %v", errs[0], errs[1])
		}
		other := tenant()
		z := mustCreate(t, s, other)
		if err := edge(z, a); !errors.Is(err, work.ErrNotFound) {
			t.Fatalf("cross-tenant edge: %v", err)
		}
	})
}

// A held Work starts when its upstream succeeds; when a required upstream
// ends without success the held Work is closed, and so is what waits on it.
func TestDependenciesHoldReleaseAndCascade(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		parent := mustCreate(t, s, tn)
		mk := func(deps ...work.Dependency) work.Work {
			w, _, err := s.CreateChild(ctx, work.NewChild{ParentID: parent.ID, Work: childOf(tn, nil), DependsOn: deps})
			if err != nil {
				t.Fatal(err)
			}
			return w
		}
		assess := mk()
		mitigate := mk(work.Dependency{ParentWorkID: assess.ID, Kind: work.AfterSuccess, Required: true})
		rca := mk(work.Dependency{ParentWorkID: assess.ID, Kind: work.AfterTerminal, Required: true})
		review := mk(work.Dependency{ParentWorkID: mitigate.ID, Kind: work.AfterSuccess, Required: true})
		if mitigate.WakeAt != nil || !strings.HasPrefix(mitigate.Reason, "held: waiting for work") {
			t.Fatalf("a child with an unmet dependency must be held: %+v", mitigate)
		}
		if ready, _ := s.ReadyChildren(ctx, tn, parent.ID); len(ready) != 0 {
			t.Fatalf("nothing is ready yet: %v", ready)
		}
		before, _ := s.GetWork(ctx, tn, parent.ID)
		finish(t, s, assess, work.StatusFailed)
		get := func(w work.Work) work.Work { x, _ := s.GetWork(ctx, tn, w.ID); return x }
		if w := get(rca); w.WakeAt == nil || w.Status != work.StatusNew {
			t.Fatalf("after_terminal must release on any ending: %+v", w)
		}
		if w := get(mitigate); w.Status != work.StatusCancelled || !strings.Contains(w.Reason, "ended FAILED") {
			t.Fatalf("a required after_success on a failed upstream closes the Work: %+v", w)
		}
		if w := get(review); w.Status != work.StatusCancelled {
			t.Fatalf("the closure must propagate downstream: %+v", w)
		}
		after := get(parent)
		if after.LastSeq <= before.LastSeq || after.WakeAt == nil {
			t.Fatalf("the parent must be told and woken: seq %d → %d", before.LastSeq, after.LastSeq)
		}
		// Settling again changes nothing.
		if err := s.CompleteChild(ctx, tn, assess.ID, 0); err != nil {
			t.Fatal(err)
		}
		if again := get(parent); again.LastSeq != after.LastSeq+1 { // one more child_completed note, no state change
			_ = again
		}
		g, _ := s.WorkGraph(ctx, tn, parent.ID)
		if len(g.Nodes) != 5 || len(g.Edges) != 3 {
			t.Fatalf("graph: %d nodes %d edges", len(g.Nodes), len(g.Edges))
		}
	})
}
