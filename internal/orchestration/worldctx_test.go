package orchestration

import (
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/envctx"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// RFC §7.5/§7.6: runs of a Work read a pinned World snapshot through the
// context gatherer; later changes do not leak into that run, and other
// providers' facts become low-trust World history.
func TestPinnedSnapshotAndEnvBridge(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	h.orch.World = h.world
	target := "deployment://prod/checkout"
	assert := func(pred, val string) {
		t.Helper()
		now := h.clock.Now()
		if _, _, err := h.store.AssertFact(h.ctx, world.Fact{TenantID: "t1", SubjectRef: target, Predicate: pred,
			Value: val, Source: "k8s", TrustTier: world.TrustFirstParty, ValidFrom: now, ObservedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	assert("image", "checkout:v2")
	w := h.create("incident", nil)
	h.settle(w.ID, work.StatusActive)
	h.runs.mu.Lock()
	req := h.runs.runs[h.runs.order[0]].req
	h.runs.mu.Unlock()
	snapID := req.Labels[SnapshotLabel]
	if snapID == "" {
		t.Fatalf("no snapshot pinned: %v", req.Labels)
	}
	h.clock.Add(time.Minute)
	assert("image", "checkout:v3") // changes after the pin

	wc := WorldContext{World: h.world}
	q := envctx.Query{TenantID: "t1", Labels: req.Labels, Services: []string{target}}
	facts, err := wc.Facts(h.ctx, q)
	if err != nil || len(facts) != 1 || !strings.Contains(facts[0].Summary, "checkout:v2") ||
		!strings.Contains(facts[0].Summary, snapID) || facts[0].Trust != world.TrustFirstParty {
		t.Fatalf("pinned facts: %v %+v", err, facts)
	}
	// Without a pin a run sees the current World.
	facts, _ = wc.Facts(h.ctx, envctx.Query{TenantID: "t1", Services: []string{target}})
	if len(facts) != 1 || !strings.Contains(facts[0].Summary, "checkout:v3") {
		t.Fatalf("current facts: %+v", facts)
	}
	// Another tenant cannot read the pinned snapshot.
	if _, err := wc.Facts(h.ctx, envctx.Query{TenantID: "t2", Labels: req.Labels}); err == nil {
		t.Fatal("cross-tenant snapshot read")
	}

	// Other providers' facts are recorded as low-trust history; World facts
	// are not echoed back.
	at := h.clock.Now().Add(-time.Minute)
	rec := EnvFactRecorder{Store: h.store, Clock: h.clock.Now}
	rec.Record(h.ctx, q, []domain.ContextFact{
		{Provider: "cmdb", Kind: "Owner Team", Subject: "checkout", Summary: "team-payments", At: &at, Trust: "T2"},
		{Provider: "notes", Kind: "runbook", Subject: "checkout", Summary: "restart is safe", Trust: "bogus"},
		{Provider: "world", Kind: "fact:image", Subject: target, Summary: "image = v3"},
	})
	fs, _ := h.store.FactsAt(h.ctx, "t1", []string{"service://checkout"}, nil, h.clock.Now(), h.clock.Now())
	got := map[string]world.Fact{}
	for _, f := range fs {
		got[f.Predicate] = f
	}
	if f := got["env.owner_team"]; f.Value != "team-payments" || f.Source != "envctx:cmdb" || f.TrustTier != "T2" {
		t.Fatalf("cmdb fact: %+v", f)
	}
	if f := got["env.runbook"]; f.TrustTier != world.TrustUntrusted {
		t.Fatalf("unknown trust must become T4: %+v", f)
	}
	if len(fs) != 2 {
		t.Fatalf("recorded facts: %+v", fs)
	}
}
