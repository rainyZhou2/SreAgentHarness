package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/world"
)

// Registry holds executors by name.
type Registry struct {
	mu   sync.RWMutex
	byID map[string]Executor
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{byID: map[string]Executor{}} }

// Register adds an executor; a duplicate name is an error.
func (r *Registry) Register(e Executor) error {
	d := e.Descriptor()
	if d.Name == "" || len(d.ResourceKinds) == 0 {
		return fmt.Errorf("action descriptor %q needs a name and resource kinds", d.Name)
	}
	switch d.IdempotencyMode {
	case IdemNative, IdemQueryBeforeRetry, IdemNone:
	default:
		return fmt.Errorf("action %s: unknown idempotency mode %q", d.Name, d.IdempotencyMode)
	}
	if d.Risk == "" {
		return fmt.Errorf("action %s: risk class is required", d.Name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byID[d.Name]; dup {
		return fmt.Errorf("action %q is registered twice", d.Name)
	}
	r.byID[d.Name] = e
	return nil
}

// Lookup returns an executor.
func (r *Registry) Lookup(name string) (Executor, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.byID[name]
	return e, ok
}

// Descriptors lists registered actions sorted by name.
func (r *Registry) Descriptors() []Descriptor {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Descriptor, 0, len(r.byID))
	for _, e := range r.byID {
		out = append(out, e.Descriptor())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Runtime drives actions through their lifecycle.
type Runtime struct {
	Store    Store
	Registry *Registry
	// Policy is the guarded policy gate (framework floor always applies).
	Policy policy.Gate
	World  *world.Service
	// Outcomes verifies effects.
	Outcomes *outcome.Evaluator
	Clock    cpkit.Clock
	Log      *slog.Logger
	// ApprovalTTL is how long a plan (and its approval) is valid (default 30m).
	ApprovalTTL time.Duration
	// FreshFor is the default max age of required facts (default 5m).
	FreshFor time.Duration
	// RequireKnownTarget rejects targets the World does not know.
	RequireKnownTarget bool
	// RequireDistinctApprover forbids the Work owner to approve its actions.
	RequireDistinctApprover bool
	// LockTTL bounds the target lock of a dispatched action (default 1h);
	// it is released when the action settles.
	LockTTL time.Duration
	// Effects, when set, is the kernel effect ledger: an action does not
	// dispatch while an agent tool call on its target is unresolved.
	Effects EffectLedger
	// Observer receives metric signals (optional).
	Observer Observer

	// Identity resolves a target ref to its stable identity and aliases
	// (design §7.2). With it, the target lock, the kernel-effect check and
	// the intent hash follow the resource, not the name it was called by.
	Identity IdentityResolver
	// Quality refuses plans whose required facts come from a collector that
	// stopped reporting, or on which sources disagree without an authority.
	Quality FactQuality
	// DryRun is rollout stage G3: plans are preflighted, approved and
	// checked for dispatch, but nothing is sent.
	DryRun bool
	// Allowed, when non-empty, lists the only actions that may be proposed
	// (rollout stage G4); Scope the only resources they may target.
	Allowed []string
	Scope   []string
	// Preauthorized are actions a Work with production_writes=preauthorized
	// may run without a per-action approval (rollout stage G5). Only
	// idempotent or inspectable actions inside Scope qualify, and never
	// where the policy denies.
	Preauthorized []string
	// BreakGlassTTL bounds an emergency release (default 15m).
	BreakGlassTTL time.Duration
}

func (rt *Runtime) now() time.Time { return rt.Clock.Now() }

func (rt *Runtime) log() *slog.Logger {
	if rt.Log != nil {
		return rt.Log
	}
	return slog.Default()
}

// ProposeRequest asks for a new action plan.
type ProposeRequest struct {
	TenantID   string
	WorkID     string
	RunID      string
	StageID    string
	Generation int
	Name       string
	Target     string
	Args       json.RawMessage
	Effect     *outcome.Spec
	// ProposedBy is the Work owner actor (or the system actor).
	ProposedBy string
	// WritesMode is the Work constraint (approval_required|forbidden|preauthorized).
	WritesMode string
	// AllowedTargets restricts targets (the Work subjects); empty = any.
	AllowedTargets []string
	FreshFor       time.Duration
	CompensatesID  string
}

// Limits.
const MaxArgsBytes = 64 << 10

// Propose builds, preflights and persists a plan. The returned record is
// REJECTED, WAITING_APPROVAL or READY. Proposing the same plan twice returns
// the existing record.
func (rt *Runtime) Propose(ctx context.Context, req ProposeRequest) (out Record, err error) {
	ctx, sp := span(ctx, "action.preflight", attribute.String("work_id", req.WorkID), attribute.String("action.name", req.Name))
	defer func() { endSpan(sp, &out, &err) }()
	exec, ok := rt.Registry.Lookup(req.Name)
	if !ok {
		return Record{}, fmt.Errorf("%w: unknown action %q", ErrNotApplied, req.Name)
	}
	desc := exec.Descriptor()
	if len(rt.Allowed) > 0 && !slices.Contains(rt.Allowed, desc.Name) {
		rt.obs().Denied("allowlist")
		return Record{}, fmt.Errorf("%w: action %q is not enabled at this rollout stage", ErrNotApplied, desc.Name)
	}
	now := rt.now()
	ttl := rt.ApprovalTTL
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	p := Plan{ID: cpkit.NewID(), TenantID: req.TenantID, WorkID: req.WorkID, RunID: req.RunID, StageID: req.StageID,
		Generation: req.Generation, ActorScope: "work:" + req.WorkID, ActionName: desc.Name, ActionVersion: desc.Version,
		Risk: desc.Risk, BlastRadius: desc.BlastRadius, Effect: req.Effect, CompensatesID: req.CompensatesID,
		ProposedBy: req.ProposedBy, CreatedAt: now, ExpiresAt: now.Add(ttl).Truncate(time.Second),
		DescriptorHash: desc.ContractHash()}
	var reasons []string
	target, err := cpkit.NormalizeRef(req.Target)
	if err != nil {
		return Record{}, fmt.Errorf("%w: target: %v", ErrNotApplied, err)
	}
	p.TargetRef = target
	if !slices.Contains(desc.ResourceKinds, cpkit.RefScheme(target)) {
		reasons = append(reasons, fmt.Sprintf("action %s cannot target %s resources", desc.Name, cpkit.RefScheme(target)))
	}
	inScope := len(rt.Scope) == 0
	for _, sc := range rt.Scope {
		inScope = inScope || cpkit.RefMatches(sc, target)
	}
	if !inScope {
		reasons = append(reasons, "target is outside the resources enabled at this rollout stage")
	}
	var id ResourceIdentity
	if rt.Identity != nil {
		if id, _, _, err = rt.identity(ctx, req.TenantID, target); err != nil {
			if !errors.Is(err, ErrAmbiguousTarget) {
				return Record{}, err
			}
			reasons = append(reasons, err.Error())
		} else {
			p.TargetIdentity, p.TargetUID = id.Digest(), id.UID
		}
	}
	if len(req.AllowedTargets) > 0 {
		in := false
		for _, a := range req.AllowedTargets {
			if cpkit.RefMatches(a, target) || (id.Ref != "" && cpkit.RefMatches(a, id.Ref)) {
				in = true
			}
		}
		if !in {
			reasons = append(reasons, "target is outside the Work subjects")
		}
	}
	args := req.Args
	if len(args) == 0 {
		args = json.RawMessage(`{}`)
	}
	if len(args) > MaxArgsBytes || !json.Valid(args) || args[0] != '{' {
		return Record{}, fmt.Errorf("%w: args must be a JSON object up to %d bytes", ErrNotApplied, MaxArgsBytes)
	}
	canon, _ := cpkit.CanonicalJSON(args)
	p.Args = canon
	if req.Effect != nil {
		e := *req.Effect
		if err := e.Normalize(); err != nil {
			return Record{}, err
		}
		p.Effect = &e
	}

	// Policy: the descriptor risk and the guarded gate decide; Work
	// constraints may only tighten.
	gate := rt.Policy
	if gate == nil {
		gate = policy.Default()
	}
	gate = policy.Guarded(gate)
	td := toolDescriptor(desc)
	d := gate.Evaluate(&td, target)
	p.PolicyVersion, p.PolicyVerdict, p.PolicyReason, p.Approvals = d.PolicyVersion, string(d.Verdict), d.Reason, d.RequiredApprovals
	if d.Risk != "" {
		p.Risk = d.Risk
	}
	if d.Verdict == policy.Deny {
		reasons = append(reasons, "policy: "+d.Reason)
	}
	if req.WritesMode == "forbidden" && p.Risk.HasSideEffects() {
		reasons = append(reasons, "the Work forbids production writes")
	}
	if d.Verdict == policy.RequireApproval && p.Approvals < 1 {
		p.Approvals = 1
	}

	// World: identity, required facts, freshness, target version.
	pf := PreflightResult{CheckedAt: now}
	fresh := req.FreshFor
	if fresh <= 0 {
		fresh = rt.FreshFor
	}
	if fresh <= 0 {
		fresh = 5 * time.Minute
	}
	if rt.World != nil {
		if rt.RequireKnownTarget {
			res, err := rt.World.Resolve(ctx, req.TenantID, target)
			if err != nil {
				return Record{}, err
			}
			if !res.Unique() {
				reasons = append(reasons, "target identity is not uniquely resolved")
			}
		}
		required := append([]string(nil), desc.RequiredFacts...)
		if desc.VersionFact != "" && !slices.Contains(required, desc.VersionFact) {
			required = append(required, desc.VersionFact)
		}
		snap, err := rt.World.Snapshot(ctx, world.SnapshotRequest{TenantID: req.TenantID, Subjects: []string{target},
			Required: required, MaxAge: fresh, Persist: true})
		if err != nil {
			return Record{}, err
		}
		p.SnapshotID, p.SnapshotDigest = snap.ID, snap.Digest
		pf.SnapshotID, pf.SnapshotDigest = snap.ID, snap.Digest
		reasons = append(reasons, freshnessProblems(&snap, target, required)...)
		reasons = append(reasons, rt.qualityProblems(ctx, req.TenantID, target, required, &snap)...)
		if desc.VersionFact != "" {
			if f, ok, conflict := snap.Value(target, desc.VersionFact); ok {
				p.TargetVersion = f.Value
				if conflict && rt.Quality == nil {
					reasons = append(reasons, "sources disagree on "+desc.VersionFact)
				}
			}
		}
	} else if len(desc.RequiredFacts) > 0 || desc.VersionFact != "" {
		reasons = append(reasons, "world state is unavailable; required facts cannot be checked")
	}
	pf.TargetVersion = p.TargetVersion
	p.Seal()

	// Executor preflight (read-only).
	if len(reasons) == 0 {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		xp, err := exec.Preflight(cctx, p)
		cancel()
		switch {
		case err != nil:
			reasons = append(reasons, "preflight failed: "+cpkit.Truncate(err.Error(), 300))
		case !xp.Allowed:
			reasons = append(reasons, xp.Reasons...)
			if len(xp.Reasons) == 0 {
				reasons = append(reasons, "preflight refused")
			}
		default:
			pf.EvidenceRefs = xp.EvidenceRefs
		}
	}
	pf.Allowed = len(reasons) == 0
	pf.Reasons = reasons
	rec := Record{Plan: p, State: StateProposed, Version: 1, Preflight: &pf, UpdatedAt: now, LockRef: id.Ref}
	switch {
	case !pf.Allowed:
		rec.State, rec.Reason = StateRejected, strings.Join(reasons, "; ")
	case d.Verdict == policy.Allow && p.Approvals == 0:
		rec.State = StateReady
	case rt.preauthorized(req, desc, d):
		// Strict pre-authorization (INV-03): the operator listed this
		// action for this scope ahead of time. It is recorded like an
		// approval, bound to this plan's intent hash.
		rec.State, rec.Reason = StateReady, "pre-authorized by operator policy"
		rec.ApprovalLog = append(rec.ApprovalLog, Approval{ApproverID: "preauthorization:" + desc.Name, Decision: "preauthorized",
			IntentHash: p.IntentHash, Comment: "production_writes=preauthorized within the configured scope", At: now})
	default:
		rec.State = StateWaitingApproval
	}
	out, created, err := rt.Store.CreateAction(ctx, rec)
	if err == nil && created {
		switch out.State {
		case StateRejected:
			rt.obs().Preflight("rejected")
			rt.obs().Denied("preflight")
		case StateReady:
			rt.obs().Preflight("ready")
		default:
			rt.obs().Preflight("approval")
		}
		rt.log().Info("action proposed", "tenant", out.TenantID, "work_id", out.WorkID, "action_id", out.ID,
			"action", out.ActionName, "target", out.TargetRef, "state", out.State)
	}
	return out, err
}

// preauthorized reports whether a plan may skip the per-action approval:
// the Work asked for it, the operator listed the action, a resource scope
// is configured (and already matched), the downstream is idempotent or
// inspectable, and the policy does not deny. R3 never qualifies.
func (rt *Runtime) preauthorized(req ProposeRequest, desc Descriptor, d policy.Decision) bool {
	return req.WritesMode == "preauthorized" && slices.Contains(rt.Preauthorized, desc.Name) && len(rt.Scope) > 0 &&
		desc.IdempotencyMode != IdemNone && desc.IdempotencyMode != "" && d.Verdict != policy.Deny &&
		desc.Risk != domain.RiskR3 && d.Risk != domain.RiskR3
}

// qualityProblems asks the fact-quality policy (collector health, source
// authority) about the facts a plan relies on.
func (rt *Runtime) qualityProblems(ctx context.Context, tenantID, target string, required []string, snap *world.Snapshot) []string {
	if rt.Quality == nil || len(required) == 0 {
		return nil
	}
	probs := rt.Quality.Problems(ctx, tenantID, target, required, snap)
	for _, p := range probs {
		switch {
		case strings.Contains(p, "conflict"):
			rt.obs().CollectorStale("conflict")
		case strings.Contains(p, "failing"):
			rt.obs().CollectorStale("failing")
		default:
			rt.obs().CollectorStale("stale")
		}
	}
	return probs
}

func freshnessProblems(s *world.Snapshot, target string, required []string) []string {
	var out []string
	for _, m := range s.Missing {
		out = append(out, "required fact missing: "+strings.TrimPrefix(m, target+"|"))
	}
	for _, p := range required {
		if f, ok, _ := s.Value(target, p); ok && s.IsStale(f.ID) {
			out = append(out, "required fact is stale: "+p)
		}
	}
	return out
}

func toolDescriptor(d Descriptor) tools.Descriptor {
	return tools.Descriptor{Name: "action:" + d.Name, Version: d.Version, Risk: d.Risk, TargetArg: "target",
		Description: d.Description, Trust: "T2", Source: "action"}
}

// Decide records an approval decision. intentHash must equal the plan's
// IntentHash: an approver approves exactly what they saw.
func (rt *Runtime) Decide(ctx context.Context, tenantID, actionID, approver, decision, intentHash, comment, workOwner string) (Record, error) {
	rec, err := rt.Store.GetAction(ctx, tenantID, actionID)
	if err != nil {
		return Record{}, err
	}
	if rec.State != StateWaitingApproval {
		return rec, errState(rec, StateWaitingApproval)
	}
	if intentHash != rec.IntentHash {
		return rec, fmt.Errorf("%w: intent hash does not match the current plan", ErrApprovalMismatch)
	}
	now := rt.now()
	if decision != "approve" && decision != "reject" {
		return rec, fmt.Errorf("%w: decision must be approve or reject", ErrApprovalMismatch)
	}
	if rt.RequireDistinctApprover && approver == workOwner && decision == "approve" {
		return rec, fmt.Errorf("%w: the Work owner cannot approve its own action", ErrApprovalMismatch)
	}
	for _, a := range rec.ApprovalLog {
		if a.ApproverID == approver && a.IntentHash == rec.IntentHash {
			return rec, nil // idempotent: one vote per approver
		}
	}
	prev := rec.Version
	if !now.Before(rec.ExpiresAt) {
		rec.State, rec.Reason = StateRejected, "approval window expired"
		rec.UpdatedAt = now
		return rt.Store.UpdateAction(ctx, rec, prev)
	}
	rec.ApprovalLog = append(rec.ApprovalLog, Approval{ApproverID: approver, Decision: decision,
		IntentHash: intentHash, Comment: cpkit.Truncate(comment, 1000), At: now})
	if decision == "reject" {
		rec.State, rec.Reason = StateRejected, "rejected by "+approver
	} else {
		n := 0
		for _, a := range rec.ApprovalLog {
			if a.Decision == "approve" && a.IntentHash == rec.IntentHash {
				n++
			}
		}
		if n >= max(rec.Approvals, 1) {
			rec.State = StateReady
		}
	}
	rec.UpdatedAt = now
	out, err := rt.Store.UpdateAction(ctx, rec, prev)
	if err == nil && out.State != StateWaitingApproval {
		rt.obs().ApprovalLag(decision, now.Sub(rec.CreatedAt))
	}
	return out, err
}

// MinJustification is the shortest break-glass justification accepted.
const MinJustification = 20

// BreakGlass releases a WAITING_APPROVAL action on the authority of one
// emergency approver (§19.4). It is bound to the intent hash like any
// approval, cannot outlive the plan, and cannot release what the policy
// denies (R3 and policy denials never reach WAITING_APPROVAL). The grant is
// stored on the record; callers write the separate audit trail.
//
// A release of an R2 action must name the incident it belongs to. The grant
// covers the plan's one target, lapses after BreakGlassTTL when it was not
// dispatched, and stays open for a second person's review
// (ReviewBreakGlass). While an effect on the target is unknown, nothing —
// not even break-glass — dispatches another write to it (Execute).
func (rt *Runtime) BreakGlass(ctx context.Context, tenantID, actionID, actor, intentHash, justification, incidentRef string) (Record, error) {
	rec, err := rt.Store.GetAction(ctx, tenantID, actionID)
	if err != nil {
		return Record{}, err
	}
	if rec.State != StateWaitingApproval {
		return rec, errState(rec, StateWaitingApproval)
	}
	if intentHash != rec.IntentHash {
		return rec, fmt.Errorf("%w: intent hash does not match the current plan", ErrApprovalMismatch)
	}
	justification = strings.TrimSpace(justification)
	if len([]rune(justification)) < MinJustification {
		return rec, fmt.Errorf("%w: a break-glass justification of at least %d characters is required",
			ErrApprovalMismatch, MinJustification)
	}
	if rec.Risk == "R3" || rec.PolicyVerdict == "deny" {
		return rec, fmt.Errorf("%w: the framework floor cannot be overridden", ErrApprovalMismatch)
	}
	incidentRef = strings.TrimSpace(incidentRef)
	if rec.Risk == domain.RiskR2 && incidentRef == "" {
		return rec, fmt.Errorf("%w: a break-glass release of an R2 action must name its incident", ErrApprovalMismatch)
	}
	if len(incidentRef) > 200 {
		return rec, fmt.Errorf("%w: incident reference is too long", ErrApprovalMismatch)
	}
	now := rt.now()
	prev := rec.Version
	if !now.Before(rec.ExpiresAt) {
		rec.State, rec.Reason, rec.UpdatedAt = StateRejected, "approval window expired", now
		return rt.Store.UpdateAction(ctx, rec, prev)
	}
	j := cpkit.Truncate(justification, 2000)
	rec.ApprovalLog = append(rec.ApprovalLog, Approval{ApproverID: actor, Decision: "break_glass",
		IntentHash: intentHash, Comment: j, At: now})
	bgTTL := rt.BreakGlassTTL
	if bgTTL <= 0 {
		bgTTL = 15 * time.Minute
	}
	until := now.Add(bgTTL)
	if rec.ExpiresAt.Before(until) {
		until = rec.ExpiresAt
	}
	rec.BreakGlass = &BreakGlassGrant{Actor: actor, Justification: j, IntentHash: intentHash, At: now,
		IncidentRef: incidentRef, Scope: rec.LockKey(), ExpiresAt: until}
	rec.State, rec.Reason, rec.UpdatedAt = StateReady, "released by break-glass ("+actor+")", now
	out, err := rt.Store.UpdateAction(ctx, rec, prev)
	if err == nil {
		rt.obs().BreakGlass()
		rt.obs().ApprovalLag("break_glass", now.Sub(rec.CreatedAt))
		rt.log().Warn("action released by break-glass", "tenant", tenantID, "work_id", out.WorkID,
			"action_id", out.ID, "action", out.ActionName, "target", out.TargetRef, "risk", out.Risk, "actor", actor)
	}
	return out, err
}

// ReviewBreakGlass records the after-the-fact review of an emergency
// release by a second person. The reviewer cannot be the releaser.
func (rt *Runtime) ReviewBreakGlass(ctx context.Context, tenantID, actionID, reviewer, comment string) (Record, error) {
	rec, err := rt.Store.GetAction(ctx, tenantID, actionID)
	if err != nil {
		return Record{}, err
	}
	switch {
	case rec.BreakGlass == nil:
		return rec, fmt.Errorf("%w: action %s was not released by break-glass", ErrInvalidState, rec.ID)
	case rec.BreakGlass.Review != nil:
		return rec, nil // idempotent: the first review stands
	case reviewer == "" || reviewer == rec.BreakGlass.Actor:
		return rec, fmt.Errorf("%w: a break-glass release is reviewed by a second person", ErrApprovalMismatch)
	}
	prev := rec.Version
	now := rt.now()
	rec.BreakGlass.Review = &BreakGlassReview{Reviewer: reviewer, Comment: cpkit.Truncate(comment, 2000), At: now}
	rec.UpdatedAt = now
	return rt.Store.UpdateAction(ctx, rec, prev)
}

// ErrApprovalMismatch is returned for approvals that do not bind the plan.
var ErrApprovalMismatch = errors.New("approval does not match the action")

// Expire rejects a waiting action whose approval window passed.
func (rt *Runtime) Expire(ctx context.Context, tenantID, actionID string) (Record, error) {
	rec, err := rt.Store.GetAction(ctx, tenantID, actionID)
	if err != nil || rec.State != StateWaitingApproval || rt.now().Before(rec.ExpiresAt) {
		return rec, err
	}
	prev := rec.Version
	rec.State, rec.Reason, rec.UpdatedAt = StateRejected, "approval window expired", rt.now()
	rt.obs().ApprovalLag("expired", rec.UpdatedAt.Sub(rec.CreatedAt))
	return rt.Store.UpdateAction(ctx, rec, prev)
}

// Execute dispatches a READY action exactly once. It re-checks expiry,
// policy, target version and freshness against a fresh snapshot, takes the
// target lock, commits DISPATCHED, and only then calls the executor.
func (rt *Runtime) Execute(ctx context.Context, tenantID, actionID string) (out Record, err error) {
	ctx, sp := span(ctx, "action.execute", attribute.String("action_id", actionID))
	defer func() { endSpan(sp, &out, &err) }()
	rec, err := rt.Store.GetAction(ctx, tenantID, actionID)
	if err != nil {
		return Record{}, err
	}
	if rec.State != StateReady {
		return rec, errState(rec, StateReady)
	}
	exec, ok := rt.Registry.Lookup(rec.ActionName)
	if !ok || exec.Descriptor().Version != rec.ActionVersion {
		rt.obs().Dispatch(rec.ActionName, string(rec.Risk), "refused")
		return rt.reject(ctx, rec, "action implementation changed since the plan was approved")
	}
	desc := exec.Descriptor()
	if rec.DescriptorHash != "" && desc.ContractHash() != rec.DescriptorHash {
		// Same version string, different contract (schema, risk,
		// idempotency): the approved plan must not be read as new arguments.
		rt.obs().Dispatch(rec.ActionName, string(rec.Risk), "refused")
		return rt.reject(ctx, rec, "action implementation changed since the plan was approved (descriptor contract differs)")
	}
	now := rt.now()
	if !now.Before(rec.ExpiresAt) {
		rt.obs().Dispatch(rec.ActionName, string(rec.Risk), "refused")
		return rt.reject(ctx, rec, "plan expired before dispatch")
	}
	if g := rec.BreakGlass; g != nil && !g.ExpiresAt.IsZero() && !now.Before(g.ExpiresAt) {
		rt.obs().Dispatch(rec.ActionName, string(rec.Risk), "refused")
		return rt.reject(ctx, rec, "plan expired before dispatch: the break-glass grant lapsed")
	}
	// Policy re-check: a stricter policy since approval wins.
	gate := rt.Policy
	if gate == nil {
		gate = policy.Default()
	}
	td := toolDescriptor(desc)
	d := policy.Guarded(gate).Evaluate(&td, rec.TargetRef)
	if d.Verdict == policy.Deny || d.PolicyVersion != rec.PolicyVersion || d.Risk != rec.Risk {
		return rt.reject(ctx, rec, "policy changed since the plan was approved: "+d.Reason)
	}
	// Fresh world check: the target must still be the version we planned on.
	if rt.World != nil && (desc.VersionFact != "" || len(desc.RequiredFacts) > 0) {
		required := append([]string(nil), desc.RequiredFacts...)
		if desc.VersionFact != "" {
			required = append(required, desc.VersionFact)
		}
		fresh := rt.FreshFor
		if fresh <= 0 {
			fresh = 5 * time.Minute
		}
		snap, err := rt.World.Snapshot(ctx, world.SnapshotRequest{TenantID: tenantID, Subjects: []string{rec.TargetRef},
			Required: required, MaxAge: fresh})
		if err != nil {
			return rec, err
		}
		probs := append(freshnessProblems(&snap, rec.TargetRef, required),
			rt.qualityProblems(ctx, tenantID, rec.TargetRef, required, &snap)...)
		if len(probs) > 0 {
			rt.obs().Dispatch(rec.ActionName, string(rec.Risk), "refused")
			return rt.reject(ctx, rec, "precondition failed: "+strings.Join(probs, "; "))
		}
		if desc.VersionFact != "" {
			f, _, conflict := snap.Value(rec.TargetRef, desc.VersionFact)
			if conflict && rt.Quality == nil {
				return rt.reject(ctx, rec, "precondition failed: sources disagree on "+desc.VersionFact)
			}
			if f.Value != rec.TargetVersion {
				return rt.reject(ctx, rec, fmt.Sprintf("precondition failed: target version is %q, plan was for %q",
					f.Value, rec.TargetVersion))
			}
		}
	}
	// The target as a resource, not as a name: every alias of it is checked
	// against both ledgers and locked under one canonical ref.
	id, refs, effectTargets, err := rt.identity(ctx, tenantID, rec.TargetRef)
	if err != nil {
		if errors.Is(err, ErrAmbiguousTarget) {
			return rt.reject(ctx, rec, "precondition failed: "+err.Error())
		}
		return rec, err
	}
	if rec.TargetIdentity != "" && rt.Identity != nil && id.Digest() != rec.TargetIdentity {
		// Same name, different resource (recreated, re-pointed alias): the
		// approval was for the old one.
		rt.obs().Dispatch(rec.ActionName, string(rec.Risk), "refused")
		return rt.reject(ctx, rec, "precondition failed: target identity changed since the plan was approved")
	}
	lockKey := rec.LockKey()
	if rt.Identity != nil {
		lockKey = id.Ref
	}
	if uf, ok := rt.Store.(UnsettledFinder); ok {
		// An unknown effect outlives its lock: while one is open on the
		// resource, no other write is dispatched to it (INV-05).
		open, err := uf.UnsettledActions(ctx, tenantID, refs, rec.ID, 1)
		if err != nil {
			return rec, err
		}
		if len(open) > 0 {
			rt.obs().Denied("target_busy")
			return rec, fmt.Errorf("%w: action %s on %s is %s; reconcile or resolve it first", ErrTargetBusy, open[0].ID,
				open[0].TargetRef, open[0].State)
		}
	}
	if rt.Effects != nil {
		open, err := rt.Effects.KernelEffects(ctx, tenantID, effectTargets, true, 5)
		if err != nil {
			return rec, fmt.Errorf("read kernel effect ledger: %w", err)
		}
		if len(open) > 0 {
			e := open[0]
			rt.log().Warn("action dispatch waits for a kernel effect", "action_id", rec.ID, "target", rec.TargetRef,
				"effect_id", e.ID, "effect_state", e.State, "run_id", e.RunID)
			rt.obs().Denied("pending_effect")
			return rec, fmt.Errorf("%w: effect %s of run %s is %s on %s", ErrPendingEffect, e.ID, e.RunID, e.State, e.Target)
		}
	}
	lockTTL := rt.LockTTL
	if lockTTL <= 0 {
		lockTTL = time.Hour
	}
	if rt.DryRun {
		// Stage G3: everything a dispatch would check has passed. Nothing is
		// locked or sent, and the record says so — it is never APPLIED.
		prev := rec.Version
		rec.State, rec.DryRun, rec.DispatchClass, rec.UpdatedAt = StateRejected, true, DefinitelyNotSent, now
		rec.Reason = "dry-run: the plan passed every dispatch check; dispatch is simulated at this rollout stage"
		rt.obs().Dispatch(rec.ActionName, string(rec.Risk), "dry_run")
		out, err := rt.Store.UpdateAction(ctx, rec, prev)
		if err != nil {
			return rec, err
		}
		return out, fmt.Errorf("%w: %s", ErrPrecondition, out.Reason)
	}
	got, holder, err := rt.Store.LockTarget(ctx, tenantID, lockKey, rec.ID, now.Add(lockTTL), now)
	if err != nil {
		return rec, err
	}
	if !got {
		rt.obs().Denied("target_busy")
		return rec, fmt.Errorf("%w: %s is held by action %s", ErrTargetBusy, lockKey, holder)
	}
	rec.LockRef = lockKey
	// Ledger first: once DISPATCHED is committed the call may happen, and a
	// crash from here on is an unknown effect, never a silent retry.
	prev := rec.Version
	rec.State, rec.MayHaveDispatched, rec.DispatchedAt, rec.UpdatedAt = StateDispatched, true, &now, now
	rec, err = rt.Store.UpdateAction(ctx, rec, prev)
	if err != nil {
		// Do not release the lock: on a version conflict another executor
		// owns this dispatch, and an ambiguous commit may have dispatched.
		// The lock expires on its own.
		return rec, err
	}
	timeout := desc.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	result, xerr := exec.Execute(cctx, rec.Plan, rec.EffectKey)
	cancel()
	done := rt.now()
	prev = rec.Version
	rec.UpdatedAt = done
	switch {
	case xerr == nil:
		rec.State, rec.Result, rec.DispatchClass = StateApplied, boundJSON(result), ConfirmedApplied
		rt.obs().Dispatch(rec.ActionName, string(rec.Risk), "applied")
	case errors.Is(xerr, ErrNotApplied):
		rec.State, rec.LastError, rec.DispatchClass = StateFailed, cpkit.Truncate(xerr.Error(), 1000), DefinitelyNotSent
		rt.obs().Dispatch(rec.ActionName, string(rec.Risk), "not_applied")
	default:
		// Anything the executor cannot prove harmless may have been sent.
		rec.State, rec.LastError, rec.DispatchClass = StateEffectUnknown, cpkit.Truncate(xerr.Error(), 1000), PossiblyDispatched
		rt.obs().EffectUnknown("dispatch_error")
		rt.obs().Dispatch(rec.ActionName, string(rec.Risk), "unknown")
	}
	out, err = rt.Store.UpdateAction(context.WithoutCancel(ctx), rec, prev)
	if err != nil {
		// The effect may have happened but its result is not durable: the
		// record stays DISPATCHED and the recovery sweep turns it into
		// EFFECT_UNKNOWN. Never report success here.
		return rec, fmt.Errorf("record action result: %w", err)
	}
	if out.State == StateFailed {
		_ = rt.Store.UnlockTarget(ctx, tenantID, out.LockKey(), out.ID)
	}
	rt.log().Info("action dispatched", "tenant", tenantID, "work_id", out.WorkID, "action_id", out.ID,
		"action", out.ActionName, "target", out.TargetRef, "state", out.State)
	return out, nil
}

func boundJSON(b json.RawMessage) json.RawMessage {
	if len(b) == 0 || !json.Valid(b) {
		return nil
	}
	if len(b) > 256<<10 {
		return json.RawMessage(`{"truncated":true}`)
	}
	return b
}

func (rt *Runtime) reject(ctx context.Context, rec Record, reason string) (Record, error) {
	rt.obs().Denied(rejectClass(reason))
	prev := rec.Version
	rec.State, rec.Reason, rec.UpdatedAt = StateRejected, reason, rt.now()
	out, err := rt.Store.UpdateAction(ctx, rec, prev)
	if err != nil {
		return rec, err
	}
	return out, fmt.Errorf("%w: %s", ErrPrecondition, reason)
}

// rejectClass maps a dispatch-time rejection to a bounded metric label.
func rejectClass(reason string) string {
	switch {
	case strings.HasPrefix(reason, "policy"):
		return "policy"
	case strings.HasPrefix(reason, "precondition"):
		return "precondition"
	case strings.HasPrefix(reason, "plan expired"):
		return "expired"
	}
	return "implementation_changed"
}

// ErrPrecondition is returned when a dispatch-time check refused the action.
var ErrPrecondition = errors.New("action precondition failed")

// Reconcile resolves a stale DISPATCHED or EFFECT_UNKNOWN action with a
// read-only Inspect. It never re-executes. Unresolvable effects go to
// MANUAL_RECOVERY.
func (rt *Runtime) Reconcile(ctx context.Context, tenantID, actionID string) (Record, error) {
	rec, err := rt.Store.GetAction(ctx, tenantID, actionID)
	if err != nil {
		return Record{}, err
	}
	if rec.State == StateDispatched {
		prev := rec.Version
		rt.obs().EffectUnknown("lost_result")
		rec.State, rec.UpdatedAt = StateEffectUnknown, rt.now()
		rec.LastError = "no result was recorded after dispatch (worker crash or lost commit)"
		if rec, err = rt.Store.UpdateAction(ctx, rec, prev); err != nil {
			return rec, err
		}
	}
	if rec.State != StateEffectUnknown {
		return rec, errState(rec, StateDispatched, StateEffectUnknown)
	}
	exec, ok := rt.Registry.Lookup(rec.ActionName)
	var ir InspectResult
	var ierr error = ErrUnsupported
	started := time.Now()
	if ok && exec.Descriptor().IdempotencyMode != IdemNone {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		ir, ierr = exec.Inspect(cctx, rec.Plan)
		cancel()
	}
	result := "unknown"
	switch {
	case ierr != nil && !errors.Is(ierr, ErrUnsupported):
		result = "error"
	case ierr == nil && ir.Applied != nil && *ir.Applied:
		result = "applied"
	case ierr == nil && ir.Applied != nil:
		result = "not_applied"
	}
	rt.obs().Reconciled(result, time.Since(started))
	prev := rec.Version
	rec.UpdatedAt = rt.now()
	switch {
	case ierr == nil && ir.Applied != nil && *ir.Applied:
		rec.State, rec.Inspect, rec.DispatchClass = StateApplied, &ir, ConfirmedApplied
	case ierr == nil && ir.Applied != nil:
		// Proven not applied. It is still not retried automatically: a
		// human (or a new plan) decides whether to try again.
		rec.State, rec.Inspect, rec.Reason = StateManualRecovery, &ir, "inspect: effect not applied; re-plan or retry manually"
	default:
		rec.State, rec.Reason = StateManualRecovery, "effect could not be determined"
		if ierr != nil {
			rec.Reason += ": " + cpkit.Truncate(ierr.Error(), 300)
		}
	}
	return rt.Store.UpdateAction(ctx, rec, prev)
}

// Resolve is the human reconciliation of an uncertain action: applied=true
// moves it to APPLIED (verification continues), false to FAILED.
func (rt *Runtime) Resolve(ctx context.Context, tenantID, actionID, actor string, applied bool, comment string) (Record, error) {
	rec, err := rt.Store.GetAction(ctx, tenantID, actionID)
	if err != nil {
		return Record{}, err
	}
	if rec.State != StateEffectUnknown && rec.State != StateManualRecovery {
		return rec, errState(rec, StateEffectUnknown, StateManualRecovery)
	}
	prev := rec.Version
	rec.UpdatedAt = rt.now()
	note := "reconciled by " + actor
	if comment != "" {
		note += ": " + cpkit.Truncate(comment, 500)
	}
	if applied {
		rec.State, rec.Reason = StateApplied, note
	} else {
		rec.State, rec.Reason = StateFailed, note
	}
	out, err := rt.Store.UpdateAction(ctx, rec, prev)
	if err == nil && out.State == StateFailed {
		_ = rt.Store.UnlockTarget(ctx, tenantID, out.LockKey(), out.ID)
	}
	return out, err
}

// VerifyEffect evaluates the action's effect spec. It returns the record
// (VERIFIED, DEGRADED, or still VERIFYING when inconclusive before the
// deadline). Without an effect spec, a positive Inspect verifies; an
// executor that cannot inspect is verified by its own result only.
func (rt *Runtime) VerifyEffect(ctx context.Context, tenantID, actionID string, deadline time.Time) (out Record, err error) {
	ctx, sp := span(ctx, "action.effect_verify", attribute.String("action_id", actionID))
	defer func() { endSpan(sp, &out, &err) }()
	rec, err := rt.Store.GetAction(ctx, tenantID, actionID)
	if err != nil {
		return Record{}, err
	}
	if rec.State != StateApplied && rec.State != StateVerifying {
		return rec, errState(rec, StateApplied, StateVerifying)
	}
	now := rt.now()
	prev := rec.Version
	rec.UpdatedAt = now
	if rec.Effect == nil {
		verdict := outcome.Satisfied
		reason := "executor result (no effect check configured)"
		if exec, ok := rt.Registry.Lookup(rec.ActionName); ok {
			cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			ir, ierr := exec.Inspect(cctx, rec.Plan)
			cancel()
			switch {
			case ierr == nil && ir.Applied != nil && !*ir.Applied:
				verdict, reason = outcome.NotSatisfied, "inspect: effect not present"
			case ierr == nil && ir.Applied != nil:
				reason = "inspect confirmed the effect"
				rec.Inspect = &ir
			}
		}
		rec.EffectEval = &outcome.Evaluation{Verdict: verdict, RawVerdict: verdict, EvaluatedAt: now, Reasons: []string{reason}}
	} else {
		var prevEval *outcome.Evaluation
		if rec.EffectEval != nil {
			prevEval = rec.EffectEval
		}
		ev := rt.Outcomes.Evaluate(ctx, *rec.Effect, outcome.Input{TenantID: tenantID, WorkID: rec.WorkID, Now: now,
			DefaultTarget: rec.TargetRef, Previous: prevEval})
		rec.EffectEval = &ev
	}
	switch v := rec.EffectEval.Verdict; {
	case v == outcome.Satisfied:
		rec.State = StateVerified
	case v == outcome.NotSatisfied || !now.Before(deadline):
		rec.State = StateDegraded
		rec.Reason = "effect not verified: " + strings.Join(rec.EffectEval.Reasons, "; ")
	default:
		rec.State = StateVerifying
	}
	out, err = rt.Store.UpdateAction(ctx, rec, prev)
	if err == nil && out.State.Settled() {
		_ = rt.Store.UnlockTarget(ctx, tenantID, out.LockKey(), out.ID)
		if out.DispatchedAt != nil {
			rt.obs().Verify(out.State, now.Sub(*out.DispatchedAt))
		}
	}
	return out, err
}

// ProposeCompensation plans the compensating action of a DEGRADED action
// (a new action through the same gate, ledger and approvals) and moves the
// original to COMPENSATION_PENDING.
func (rt *Runtime) ProposeCompensation(ctx context.Context, tenantID, actionID, proposedBy, writesMode string) (Record, Record, error) {
	rec, err := rt.Store.GetAction(ctx, tenantID, actionID)
	if err != nil {
		return Record{}, Record{}, err
	}
	if rec.State != StateDegraded {
		return rec, Record{}, errState(rec, StateDegraded)
	}
	exec, ok := rt.Registry.Lookup(rec.ActionName)
	if !ok || !exec.Descriptor().SupportsRollback || exec.Descriptor().Compensation == "" {
		return rec, Record{}, fmt.Errorf("%w: %s has no compensation", ErrUnsupported, rec.ActionName)
	}
	comp, err := rt.Propose(ctx, ProposeRequest{TenantID: tenantID, WorkID: rec.WorkID, RunID: rec.RunID,
		StageID: rec.StageID, Generation: rec.Generation, Name: exec.Descriptor().Compensation, Target: rec.TargetRef,
		Args: rec.Args, ProposedBy: proposedBy, WritesMode: writesMode, CompensatesID: rec.ID})
	if err != nil {
		return rec, Record{}, err
	}
	prev := rec.Version
	rec.State, rec.CompensationID, rec.UpdatedAt = StateCompensationPending, comp.ID, rt.now()
	rec, err = rt.Store.UpdateAction(ctx, rec, prev)
	return rec, comp, err
}

// SettleCompensation marks the original COMPENSATED once its compensation
// is verified (or back to DEGRADED when the compensation failed).
func (rt *Runtime) SettleCompensation(ctx context.Context, tenantID, actionID string) (Record, error) {
	rec, err := rt.Store.GetAction(ctx, tenantID, actionID)
	if err != nil || rec.State != StateCompensationPending {
		return rec, err
	}
	comp, err := rt.Store.GetAction(ctx, tenantID, rec.CompensationID)
	if err != nil {
		return rec, err
	}
	prev := rec.Version
	switch comp.State {
	case StateVerified:
		rec.State = StateCompensated
	case StateRejected, StateFailed, StateDegraded, StateManualRecovery:
		rec.State, rec.Reason = StateDegraded, "compensation "+comp.ID+" ended "+string(comp.State)
	default:
		return rec, nil
	}
	rec.UpdatedAt = rt.now()
	return rt.Store.UpdateAction(ctx, rec, prev)
}

// RecoverStale turns DISPATCHED actions older than age into reconciled
// records (crash recovery sweep). It returns how many it touched.
func (rt *Runtime) RecoverStale(ctx context.Context, age time.Duration, limit int) (int, error) {
	recs, err := rt.Store.ListStaleDispatched(ctx, rt.now().Add(-age), limit)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, r := range recs {
		if _, err := rt.Reconcile(ctx, r.TenantID, r.ID); err != nil && !errors.Is(err, ErrVersionConflict) {
			rt.log().Warn("action reconcile", "action_id", r.ID, "err", err)
			continue
		}
		n++
	}
	return n, nil
}
