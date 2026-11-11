// Package actiontest certifies action executors against the runtime's
// contract (design §7.3). Every R1/R2 executor is expected to pass Certify
// against a fake of its downstream before it is enabled in production:
// the runtime's guarantees (no duplicate dispatch, unknown effects never
// replayed) hold only when executors classify their own failures honestly.
//
// The suite checks what the runtime cannot check for itself:
//
//  1. Preflight is read-only.
//  2. Execute with one idempotency key changes the downstream at most once
//     (or the executor declares IdemNone and is never retried).
//  3. Inspect answers applied / not applied, and "cannot tell" when the
//     downstream is unreachable — never "not applied" for a failed read.
//  4. A plan for an older target version is refused before any write.
//  5. A compensation is a different, registered action.
//  6. A violated safety guard refuses the plan in Preflight.
//  7. Errors are classified: a refusal before the write is ErrNotApplied,
//     anything after a possible write is not; a multi-step effect that was
//     interrupted can be inspected and completed with the same key.
package actiontest

import (
	"context"
	"errors"
	"testing"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/domain"
)

// Fault is a failure injected into the fake downstream.
type Fault int

const (
	// None: the downstream works.
	None Fault = iota
	// Refuse: writes are rejected before anything changes (4xx).
	Refuse
	// LoseResponse: a write is applied, then the response is lost (timeout
	// or 5xx after the effect).
	LoseResponse
	// Unreachable: every request fails, reads included.
	Unreachable
)

// Case describes one executor and the fake it runs against.
type Case struct {
	Executor action.Executor
	// Plan returns a fresh valid plan for the fake's current state.
	Plan func() action.Plan
	// State fingerprints the downstream state the action changes.
	State func() string
	// Inject switches the fake's failure mode.
	Inject func(Fault)
	// Reset restores the fake to its initial state.
	Reset func()
	// Stale changes the target's version behind the plan's back (optional;
	// required when the descriptor declares a VersionFact).
	Stale func()
	// ViolateGuard makes a safety guard fail — no capacity, a disruption
	// budget, a too-large step (optional).
	ViolateGuard func()
	// Registry resolves the compensation action (optional).
	Registry *action.Registry
}

// Report lists the checks that ran.
type Report struct {
	Action  string
	Checked []string
	Skipped []string
}

// Certify runs the contract checks; failures are reported on t.
func Certify(t *testing.T, c Case) Report {
	t.Helper()
	ctx := context.Background()
	d := c.Executor.Descriptor()
	rep := Report{Action: d.Name}
	ran := func(name string) { rep.Checked = append(rep.Checked, name) }
	skip := func(name string) { rep.Skipped = append(rep.Skipped, name) }
	reset := func() {
		c.Inject(None)
		c.Reset()
	}

	// Descriptor: what the runtime and the gates rely on.
	if d.Name == "" || d.Version == "" || len(d.ResourceKinds) == 0 {
		t.Errorf("%s: descriptor needs a name, a version and resource kinds", d.Name)
	}
	if d.Risk.HasSideEffects() {
		switch d.IdempotencyMode {
		case action.IdemNative, action.IdemQueryBeforeRetry, action.IdemNone:
		default:
			t.Errorf("%s: a side-effecting action must declare its idempotency mode", d.Name)
		}
	}
	if d.Risk == domain.RiskR3 {
		t.Errorf("%s: R3 actions are never dispatched", d.Name)
	}
	ran("descriptor")

	// 1. Preflight is read-only.
	reset()
	before := c.State()
	for i := 0; i < 2; i++ {
		if pf, err := c.Executor.Preflight(ctx, c.Plan()); err != nil || !pf.Allowed {
			t.Errorf("%s: preflight of a valid plan: allowed=%v reasons=%v err=%v", d.Name, pf.Allowed, pf.Reasons, err)
		}
	}
	if c.State() != before {
		t.Errorf("%s: Preflight changed the downstream", d.Name)
	}
	ran("preflight_read_only")

	// 2. One key, at most one effect.
	reset()
	p := c.Plan()
	if _, err := c.Executor.Execute(ctx, p, p.EffectKey); err != nil {
		t.Errorf("%s: execute: %v", d.Name, err)
	}
	applied := c.State()
	if applied == before {
		t.Errorf("%s: Execute reported success without changing the downstream", d.Name)
	}
	if d.IdempotencyMode == action.IdemNone {
		skip("idempotent_replay (IdemNone: the runtime never retries)")
	} else {
		// A replay may succeed or be refused as already applied; either
		// way the downstream must not change again.
		if _, err := c.Executor.Execute(ctx, p, p.EffectKey); err != nil && !errors.Is(err, action.ErrNotApplied) {
			t.Errorf("%s: a replay with the same key must be a no-op or ErrNotApplied, got %v", d.Name, err)
		}
		if c.State() != applied {
			t.Errorf("%s: a replay with the same key changed the downstream again", d.Name)
		}
		ran("idempotent_replay")
	}

	// 3. Inspect.
	if d.IdempotencyMode == action.IdemNone {
		skip("inspect (IdemNone)")
	} else {
		if ir, err := c.Executor.Inspect(ctx, p); err != nil || ir.Applied == nil || !*ir.Applied {
			t.Errorf("%s: Inspect after the effect must report applied (applied=%v err=%v)", d.Name, ir.Applied, err)
		}
		reset()
		fresh := c.Plan()
		if ir, err := c.Executor.Inspect(ctx, fresh); err == nil && (ir.Applied == nil || *ir.Applied) && !errors.Is(err, action.ErrUnsupported) {
			t.Errorf("%s: Inspect before the effect must report not applied (applied=%v)", d.Name, ir.Applied)
		}
		c.Inject(Unreachable)
		if ir, err := c.Executor.Inspect(ctx, fresh); err == nil && ir.Applied != nil {
			t.Errorf("%s: Inspect answered applied=%v while the downstream was unreachable; a failed read is unknown", d.Name, *ir.Applied)
		}
		ran("inspect")
	}

	// 4. Stale target version.
	switch {
	case c.Stale != nil:
		reset()
		stale := c.Plan()
		c.Stale()
		at := c.State()
		_, err := c.Executor.Execute(ctx, stale, stale.EffectKey)
		if !errors.Is(err, action.ErrNotApplied) {
			t.Errorf("%s: a plan for an older target version must be refused with ErrNotApplied, got %v", d.Name, err)
		}
		if c.State() != at {
			t.Errorf("%s: a stale plan changed the downstream", d.Name)
		}
		ran("stale_version")
	case d.VersionFact != "":
		t.Errorf("%s: the descriptor pins a version fact; the case must provide Stale", d.Name)
	default:
		skip("stale_version (no version fact)")
	}

	// 5. Compensation is a new action.
	if d.SupportsRollback || d.Compensation != "" {
		if d.Compensation == "" {
			t.Errorf("%s: SupportsRollback without a compensation action", d.Name)
		}
		if c.Registry != nil {
			if _, ok := c.Registry.Lookup(d.Compensation); !ok {
				t.Errorf("%s: compensation %q is not a registered action", d.Name, d.Compensation)
			}
		}
		ran("compensation")
	} else {
		skip("compensation (none declared)")
	}

	// 6. Safety guards refuse in Preflight.
	if c.ViolateGuard != nil {
		reset()
		c.ViolateGuard()
		at := c.State()
		pf, err := c.Executor.Preflight(ctx, c.Plan())
		if err == nil && pf.Allowed {
			t.Errorf("%s: Preflight allowed a plan that violates a safety guard", d.Name)
		}
		if c.State() != at {
			t.Errorf("%s: a refused Preflight changed the downstream", d.Name)
		}
		ran("safety_guard")
	} else {
		skip("safety_guard (none declared)")
	}

	// 7. Error classes.
	reset()
	p = c.Plan()
	c.Inject(Refuse)
	at := c.State()
	if _, err := c.Executor.Execute(ctx, p, p.EffectKey); !errors.Is(err, action.ErrNotApplied) {
		t.Errorf("%s: a write refused by the downstream is definitely_not_sent (ErrNotApplied), got %v", d.Name, err)
	}
	if c.State() != at {
		t.Errorf("%s: a refused write changed the downstream", d.Name)
	}
	reset()
	p = c.Plan()
	c.Inject(LoseResponse)
	_, err := c.Executor.Execute(ctx, p, p.EffectKey)
	if err == nil {
		t.Errorf("%s: a lost response must not be reported as success", d.Name)
	}
	if errors.Is(err, action.ErrNotApplied) {
		t.Errorf("%s: a write whose response was lost is possibly_dispatched; it was classified as not applied", d.Name)
	}
	c.Inject(None)
	if d.IdempotencyMode != action.IdemNone {
		// The write whose response was lost may have been the whole effect
		// or the first step of several. Inspect must answer truthfully
		// either way, and where it says "not applied" a replay with the
		// same key must finish the job without doing any step twice.
		ir, ierr := c.Executor.Inspect(ctx, p)
		switch {
		case ierr != nil || ir.Applied == nil:
			t.Errorf("%s: Inspect must be able to tell after a lost response (applied=%v err=%v)", d.Name, ir.Applied, ierr)
		case !*ir.Applied:
			if _, err := c.Executor.Execute(ctx, p, p.EffectKey); err != nil && !errors.Is(err, action.ErrNotApplied) {
				t.Errorf("%s: completing a partially applied effect with the same key: %v", d.Name, err)
			}
			if got := c.State(); got != applied {
				t.Errorf("%s: after a lost response and a replay the downstream is %q, a clean execution gives %q", d.Name, got, applied)
			}
			if ir, err := c.Executor.Inspect(ctx, p); err != nil || ir.Applied == nil || !*ir.Applied {
				t.Errorf("%s: Inspect must confirm the completed effect", d.Name)
			}
		}
	}
	ran("error_classes")
	return rep
}
