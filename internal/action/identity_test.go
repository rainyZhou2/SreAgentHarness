package action_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/world"
)

// plainExec is an executor without world preconditions. Execute blocks on
// hold (when set) so a test can keep an effect in flight, and fails with
// fail (an error that is not ErrNotApplied leaves the effect unknown).
type plainExec struct {
	name   string
	kinds  []string
	risk   domain.RiskClass
	idem   string
	schema string
	fail   error

	mu    sync.Mutex
	calls int
}

func (p *plainExec) Descriptor() action.Descriptor {
	idem := p.idem
	if idem == "" {
		idem = action.IdemQueryBeforeRetry
	}
	return action.Descriptor{Name: p.name, Version: "1", ResourceKinds: p.kinds, Risk: p.risk, IdempotencyMode: idem,
		InputSchema: json.RawMessage(p.schema), BlastRadius: "single_resource"}
}
func (p *plainExec) Preflight(context.Context, action.Plan) (action.PreflightResult, error) {
	return action.PreflightResult{Allowed: true}, nil
}
func (p *plainExec) Execute(context.Context, action.Plan, string) (json.RawMessage, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.fail != nil {
		return nil, p.fail
	}
	return json.RawMessage(`{"ok":true}`), nil
}
func (p *plainExec) Inspect(context.Context, action.Plan) (action.InspectResult, error) {
	return action.InspectResult{}, nil // cannot tell
}
func (p *plainExec) count() int { p.mu.Lock(); defer p.mu.Unlock(); return p.calls }

const svcRef = "service://prod/checkout"

// alias registers the service ref as another name of the deployment.
func (e *env) alias(uid string) {
	e.t.Helper()
	for _, ent := range []world.Entity{
		{TenantID: e.tn, Ref: target, Kind: "deployment", Source: "k8s", Attributes: map[string]string{action.AttrUID: uid}},
		{TenantID: e.tn, Ref: svcRef, Kind: "service", Source: "cmdb", Attributes: map[string]string{action.AttrIdentity: target}},
	} {
		if _, err := e.store.UpsertEntity(e.ctx, ent, e.clock.now()); err != nil {
			e.t.Fatal(err)
		}
	}
}

func (e *env) plain(rt *action.Runtime, workID, name, ref string) action.Record {
	e.t.Helper()
	r, err := rt.Propose(e.ctx, action.ProposeRequest{TenantID: e.tn, WorkID: workID, Generation: 1, Name: name, Target: ref,
		ProposedBy: "work-owner"})
	if err != nil {
		e.t.Fatal(err)
	}
	if r.State != action.StateReady {
		e.t.Fatalf("%s on %s: %s (%s)", name, ref, r.State, r.Reason)
	}
	return r
}

// F15: two actions name the same resource by different refs. The second is
// refused while the first one's effect is open — also after the lock of the
// first has expired, because an unknown effect has no expiry.
func TestAliasesShareOneTargetLock(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		e.alias("uid-1")
		rollback := &plainExec{name: "deployment.restart", kinds: []string{"deployment"}, risk: domain.RiskR1,
			fail: errors.New("read tcp: i/o timeout")}
		shift := &plainExec{name: "traffic.drain", kinds: []string{"service"}, risk: domain.RiskR1}
		rt := e.runtime(rollback, shift)
		rt.Identity = action.WorldIdentity{World: e.world}
		rt.LockTTL = time.Minute
		w := e.work()
		a := e.plain(rt, w, "deployment.restart", target)
		b := e.plain(rt, w, "traffic.drain", svcRef)
		if a.TargetIdentity == "" || a.TargetIdentity != b.TargetIdentity || a.TargetUID != "uid-1" {
			t.Fatalf("both refs must resolve to one identity: %q %q", a.TargetIdentity, b.TargetIdentity)
		}
		got, err := rt.Execute(e.ctx, e.tn, a.ID)
		if err != nil || got.State != action.StateEffectUnknown || got.DispatchClass != action.PossiblyDispatched || got.LockRef != target {
			t.Fatalf("first action: %+v %v", got.State, err)
		}
		if _, err := rt.Execute(e.ctx, e.tn, b.ID); !errors.Is(err, action.ErrTargetBusy) {
			t.Fatalf("the alias must not get past the open effect: %v", err)
		}
		e.clock.add(5 * time.Minute) // the lock is gone, the unknown effect is not
		if _, err := rt.Execute(e.ctx, e.tn, b.ID); !errors.Is(err, action.ErrTargetBusy) || !strings.Contains(err.Error(), "EFFECT_UNKNOWN") {
			t.Fatalf("an unknown effect outlives its lock: %v", err)
		}
		if shift.count() != 0 {
			t.Fatalf("the second action was dispatched %d time(s)", shift.count())
		}
		// A human settles the first effect; then the second may run.
		if _, err := rt.Resolve(e.ctx, e.tn, a.ID, "sre-bob", false, "checked the cluster: not restarted"); err != nil {
			t.Fatal(err)
		}
		if got, err := rt.Execute(e.ctx, e.tn, b.ID); err != nil || got.State != action.StateApplied {
			t.Fatalf("after resolution: %s %v", got.State, err)
		}
	})
}

// F16 / §7.2: a resource deleted and created again under the same name is a
// different target; the approval for the old one does not carry over.
func TestRecreatedResourceInvalidatesPlan(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		e.alias("uid-1")
		ex := &plainExec{name: "deployment.restart", kinds: []string{"deployment"}, risk: domain.RiskR1}
		rt := e.runtime(ex)
		rt.Identity = action.WorldIdentity{World: e.world}
		a := e.plain(rt, e.work(), "deployment.restart", target)
		e.alias("uid-2")
		got, err := rt.Execute(e.ctx, e.tn, a.ID)
		if !errors.Is(err, action.ErrPrecondition) || got.State != action.StateRejected || !strings.Contains(got.Reason, "identity changed") {
			t.Fatalf("got %s %q %v", got.State, got.Reason, err)
		}
		if ex.count() != 0 {
			t.Fatal("dispatched to a recreated resource")
		}
	})
}

// F24: an executor whose contract changed without a version bump no longer
// matches the approved plan.
func TestChangedDescriptorInvalidatesPlan(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		ex := &plainExec{name: "deployment.restart", kinds: []string{"deployment"}, risk: domain.RiskR1,
			schema: `{"type":"object","properties":{"grace":{"type":"integer"}}}`}
		rt := e.runtime(ex)
		a := e.plain(rt, e.work(), "deployment.restart", target)
		ex.schema = `{"type":"object","properties":{"grace":{"type":"string"},"force":{"type":"boolean"}}}`
		got, err := rt.Execute(e.ctx, e.tn, a.ID)
		if !errors.Is(err, action.ErrPrecondition) || got.State != action.StateRejected || !strings.Contains(got.Reason, "contract differs") {
			t.Fatalf("got %s %q %v", got.State, got.Reason, err)
		}
		if ex.count() != 0 {
			t.Fatal("a plan was interpreted under a new schema")
		}
	})
}

// G3: a dry run passes every dispatch check and sends nothing.
func TestDryRunNeverDispatches(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		ex := &plainExec{name: "deployment.restart", kinds: []string{"deployment"}, risk: domain.RiskR1}
		rt := e.runtime(ex)
		rt.DryRun = true
		a := e.plain(rt, e.work(), "deployment.restart", target)
		got, err := rt.Execute(e.ctx, e.tn, a.ID)
		if !errors.Is(err, action.ErrPrecondition) || !got.DryRun || got.State != action.StateRejected ||
			got.MayHaveDispatched || got.DispatchClass != action.DefinitelyNotSent {
			t.Fatalf("dry run: %+v %v", got, err)
		}
		if ex.count() != 0 {
			t.Fatal("a dry run reached the executor")
		}
	})
}

// G4: only allow-listed actions on in-scope resources can be planned.
func TestAllowlistAndScope(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		restart := &plainExec{name: "deployment.restart", kinds: []string{"deployment"}, risk: domain.RiskR1}
		scale := &plainExec{name: "deployment.scale", kinds: []string{"deployment"}, risk: domain.RiskR1}
		rt := e.runtime(restart, scale)
		rt.Allowed, rt.Scope = []string{"deployment.restart"}, []string{"deployment://prod/checkout"}
		w := e.work()
		if _, err := rt.Propose(e.ctx, action.ProposeRequest{TenantID: e.tn, WorkID: w, Generation: 1, Name: "deployment.scale",
			Target: target, ProposedBy: "o"}); !errors.Is(err, action.ErrNotApplied) {
			t.Fatalf("an action outside the allow-list: %v", err)
		}
		r, err := rt.Propose(e.ctx, action.ProposeRequest{TenantID: e.tn, WorkID: w, Generation: 1, Name: "deployment.restart",
			Target: "deployment://prod/billing", ProposedBy: "o"})
		if err != nil || r.State != action.StateRejected || !strings.Contains(r.Reason, "outside the resources enabled") {
			t.Fatalf("out-of-scope target: %s %q %v", r.State, r.Reason, err)
		}
		e.plain(rt, w, "deployment.restart", target)
	})
}

// G5 / INV-03: pre-authorization releases an R2 action only when the Work
// asked for it, the operator listed the action, a scope is set and the
// downstream is idempotent — and it is recorded against the intent hash.
func TestPreauthorization(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		idem := &plainExec{name: "cache.flush", kinds: []string{"deployment"}, risk: domain.RiskR2}
		blind := &plainExec{name: "legacy.script", kinds: []string{"deployment"}, risk: domain.RiskR2, idem: action.IdemNone}
		rt := e.runtime(idem, blind)
		rt.Preauthorized, rt.Scope = []string{"cache.flush", "legacy.script"}, []string{target}
		w := e.work()
		propose := func(name, mode string) action.Record {
			r, err := rt.Propose(e.ctx, action.ProposeRequest{TenantID: e.tn, WorkID: w, Generation: 1, Name: name, Target: target,
				Args: json.RawMessage(`{"mode":"` + mode + name + `"}`), ProposedBy: "o", WritesMode: mode})
			if err != nil {
				t.Fatal(err)
			}
			return r
		}
		if r := propose("cache.flush", "approval_required"); r.State != action.StateWaitingApproval {
			t.Fatalf("without the Work constraint: %s", r.State)
		}
		if r := propose("legacy.script", "preauthorized"); r.State != action.StateWaitingApproval {
			t.Fatalf("a non-idempotent action must not be pre-authorized: %s", r.State)
		}
		r := propose("cache.flush", "preauthorized")
		if r.State != action.StateReady || len(r.ApprovalLog) != 1 || r.ApprovalLog[0].Decision != "preauthorized" ||
			r.ApprovalLog[0].IntentHash != r.IntentHash {
			t.Fatalf("pre-authorized plan: %s %+v", r.State, r.ApprovalLog)
		}
		rt.Scope = nil
		if r := propose("cache.flush", "preauthorized"); r.State == action.StateReady && len(r.ApprovalLog) > 0 && r.ID != "" && r.Reason == "" {
			t.Fatalf("no scope, no pre-authorization: %s", r.State)
		}
	})
}

// §7.5: a break-glass grant lapses, is bound to its target, and needs a
// second person's review.
func TestBreakGlassGrantExpiresAndIsReviewed(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		ex := &plainExec{name: "deployment.restart", kinds: []string{"deployment"}, risk: domain.RiskR2}
		rt := e.runtime(ex)
		rt.ApprovalTTL = time.Hour
		w := e.work()
		hold := func(n string) action.Record {
			r, err := rt.Propose(e.ctx, action.ProposeRequest{TenantID: e.tn, WorkID: w, Generation: 1, Name: "deployment.restart",
				Target: target, Args: json.RawMessage(`{"n":"` + n + `"}`), ProposedBy: "o"})
			if err != nil || r.State != action.StateWaitingApproval {
				t.Fatalf("%s %v", r.State, err)
			}
			return r
		}
		just := "SEV1: checkout is down for every user and the approvers are unreachable"
		a := hold("1")
		if _, err := rt.BreakGlass(e.ctx, e.tn, a.ID, "ic-alice", a.IntentHash, just, ""); !errors.Is(err, action.ErrApprovalMismatch) {
			t.Fatalf("R2 without an incident: %v", err)
		}
		got, err := rt.BreakGlass(e.ctx, e.tn, a.ID, "ic-alice", a.IntentHash, just, "INC-42")
		if err != nil || got.BreakGlass.Scope != target || !got.BreakGlass.ExpiresAt.Equal(e.clock.now().Add(15*time.Minute)) {
			t.Fatalf("grant: %+v %v", got.BreakGlass, err)
		}
		e.clock.add(16 * time.Minute)
		got, err = rt.Execute(e.ctx, e.tn, a.ID)
		if !errors.Is(err, action.ErrPrecondition) || !strings.Contains(got.Reason, "break-glass grant lapsed") || ex.count() != 0 {
			t.Fatalf("a lapsed grant must not dispatch: %q %v", got.Reason, err)
		}
		b := hold("2")
		if _, err := rt.BreakGlass(e.ctx, e.tn, b.ID, "ic-alice", b.IntentHash, just, "INC-42"); err != nil {
			t.Fatal(err)
		}
		if _, err := rt.Execute(e.ctx, e.tn, b.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := rt.ReviewBreakGlass(e.ctx, e.tn, b.ID, "ic-alice", "looks fine"); !errors.Is(err, action.ErrApprovalMismatch) {
			t.Fatalf("self review: %v", err)
		}
		rv, err := rt.ReviewBreakGlass(e.ctx, e.tn, b.ID, "sre-lead", "justified; restart was the documented fix")
		if err != nil || rv.BreakGlass.Review == nil || rv.BreakGlass.Review.Reviewer != "sre-lead" {
			t.Fatalf("review: %+v %v", rv.BreakGlass, err)
		}
	})
}
