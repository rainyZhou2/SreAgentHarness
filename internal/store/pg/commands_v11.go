package pg

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
)

// v1.1 commands (§29.5, §35.6). All run inside ApplyCommand's transaction with
// the run row locked; none of them can act while a worker holds the lease.

func (s *Store) applyV11(ctx context.Context, r *runTx, cmd *domain.Command, req domain.CommandRequest) (string, bool, error) {
	switch req.Type {
	case domain.CmdOverrideResult:
		reason, err := s.overrideResult(ctx, r, cmd, req)
		return reason, true, err
	case domain.CmdReplacePendingInput:
		reason, err := s.replacePendingInput(ctx, r, cmd, req)
		return reason, true, err
	case domain.CmdRetry:
		reason, err := s.retryStep(ctx, r, cmd, req)
		return reason, true, err
	case domain.CmdMigrateProfile:
		reason, err := s.migrateProfile(ctx, r, cmd, req)
		return reason, true, err
	}
	return "", false, nil
}

func idle(r *runTx) bool {
	switch r.run.Status {
	case domain.RunPaused, domain.RunNeedsHuman, domain.RunWaitingApproval, domain.RunQueued:
		return true
	}
	return false
}

// overrideResult records a human-supplied output for a committed step as a new
// revision; the original execution output is kept as revision 0 (§29.5).
// Later reasoning sees the override; earlier decisions are not rewritten.
func (s *Store) overrideResult(ctx context.Context, r *runTx, cmd *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		StepID              string `json:"step_id"`
		SideEffectsReverted bool   `json:"side_effects_reverted"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || !validID(p.StepID) {
		return "step_id_required", nil
	}
	if req.Internal == nil || req.Internal.OutputRef == "" {
		return "output_required", nil
	}
	if req.Reason == "" {
		return "reason_required", nil
	}
	if !idle(r) {
		return "run_must_be_paused_or_waiting", nil
	}
	var status domain.StepStatus
	var kind domain.StepKind
	var outRef, outHash *string
	var rev int
	err := r.tx.QueryRow(ctx, `SELECT status, kind, output_ref, output_hash, output_revision FROM agent_steps
		WHERE id=$1 AND run_id=$2 FOR UPDATE`, p.StepID, r.run.ID).Scan(&status, &kind, &outRef, &outHash, &rev)
	if errors.Is(err, pgx.ErrNoRows) {
		return "step_not_found", nil
	}
	if err != nil {
		return "", err
	}
	if kind != domain.StepTool || (status != domain.StepSucceeded && status != domain.StepFailed) {
		return "only_finished_tool_steps_can_be_overridden", nil
	}
	if rev == 0 && outRef != nil {
		if _, err := r.tx.Exec(ctx, `INSERT INTO step_output_revisions (step_id, revision, tenant_id, output_ref, output_hash,
			source) VALUES ($1,0,$2,$3,$4,'EXECUTION') ON CONFLICT DO NOTHING`, p.StepID, r.run.TenantID, *outRef,
			deref(outHash)); err != nil {
			return "", err
		}
	}
	rev++
	if _, err := r.tx.Exec(ctx, `INSERT INTO step_output_revisions (step_id, revision, tenant_id, output_ref, output_hash,
		source, command_id, reason, side_effects_reverted) VALUES ($1,$2,$3,$4,$5,'HUMAN_OVERRIDE',$6,$7,$8)`,
		p.StepID, rev, r.run.TenantID, req.Internal.OutputRef, req.Internal.OutputHash, cmd.ID, req.Reason,
		p.SideEffectsReverted); err != nil {
		return "", err
	}
	if _, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status='SUCCEEDED', output_ref=$2, output_hash=$3,
		output_revision=$4, status_reason=$5, updated_at=now() WHERE id=$1`, p.StepID, req.Internal.OutputRef,
		req.Internal.OutputHash, rev, "output overridden by "+req.ActorID+": "+req.Reason); err != nil {
		return "", err
	}
	r.run.Version++
	return "", r.event(ctx, "step.output_overridden", "user", req.ActorID, &p.StepID, nil, map[string]any{
		"command_id": cmd.ID, "revision": rev, "reason": req.Reason, "side_effects_reverted": p.SideEffectsReverted})
}

// replacePendingInput replaces the arguments of a tool step that is waiting
// for approval. The intent is recomputed server-side, every pending approval
// is superseded and a fresh approval is requested for the new action hash.
func (s *Store) replacePendingInput(ctx context.Context, r *runTx, cmd *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		StepID string `json:"step_id"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || !validID(p.StepID) {
		return "step_id_required", nil
	}
	in := req.Internal
	if in == nil || in.InputRef == "" {
		return "input_required", nil
	}
	if r.run.Status != domain.RunWaitingApproval && r.run.Status != domain.RunPaused {
		return "invalid_state", nil
	}
	tag, err := r.tx.Exec(ctx, `UPDATE agent_steps SET input_ref=$3, input_hash=$4, updated_at=now()
		WHERE id=$1 AND run_id=$2 AND status='WAITING_APPROVAL'`, p.StepID, r.run.ID, in.InputRef, in.InputHash)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() != 1 {
		return "step_not_waiting_approval", nil
	}
	if err := r.supersedeApprovals(ctx); err != nil {
		return "", err
	}
	if err := r.event(ctx, "step.input_replaced", "user", req.ActorID, &p.StepID, nil, map[string]any{
		"command_id": cmd.ID, "input_hash": in.InputHash, "reason": req.Reason, "policy": in.PolicyDecision}); err != nil {
		return "", err
	}
	r.run.Version++
	if in.Denied != "" || in.Intent == nil {
		reason := in.Denied
		if reason == "" {
			reason = "replacement input has no valid intent"
		}
		return "", s.skipBlockedStep(ctx, r, p.StepID, "denied by policy: "+reason)
	}
	intentID := newID()
	in.Intent.ID = intentID
	x := in.Intent
	if _, err := r.tx.Exec(ctx, `INSERT INTO action_intents (id, tenant_id, run_id, step_id, environment, target,
		tool_name, tool_version, input_hash, action_hash, risk_class, policy_version, effects)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, intentID, r.run.TenantID, r.run.ID, p.StepID,
		x.Environment, x.Target, x.ToolName, x.ToolVersion, x.InputHash, x.ActionHash, x.RiskClass, x.PolicyVersion,
		jsonb(x.Effects)); err != nil {
		if isUniqueViolation(err) {
			return "input_unchanged", nil
		}
		return "", err
	}
	if in.RequiredApprovals == 0 {
		// The new action no longer needs approval (e.g. downgraded to read-only).
		if _, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status='PENDING', updated_at=now() WHERE id=$1`, p.StepID); err != nil {
			return "", err
		}
		if r.run.Status == domain.RunWaitingApproval {
			r.setStatus(domain.RunQueued)
			return "", r.settle(ctx)
		}
		return "", nil
	}
	return "", r.requestApproval(ctx, p.StepID, intentID, x, in.RequiredApprovals)
}

// retryStep re-plans a failed or cancelled step that has no external side
// effect. Side-effecting steps must go through resolve_unknown (§5.3).
func (s *Store) retryStep(ctx context.Context, r *runTx, cmd *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		StepID string `json:"step_id"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || !validID(p.StepID) {
		return "step_id_required", nil
	}
	if r.run.Status != domain.RunNeedsHuman && r.run.Status != domain.RunPaused {
		return "invalid_state", nil
	}
	st, err := scanStep(r.tx.QueryRow(ctx, `SELECT `+stepCols+` FROM agent_steps WHERE id=$1 AND run_id=$2`, p.StepID, r.run.ID))
	if errors.Is(err, domain.ErrNotFound) {
		return "step_not_found", nil
	}
	if err != nil {
		return "", err
	}
	if st.Status != domain.StepFailed && st.Status != domain.StepCancelled {
		return "step_not_failed", nil
	}
	intent, err := loadIntent(ctx, r.tx, r.run.TenantID, st.ID)
	if err != nil {
		return "", err
	}
	if intent != nil && intent.RiskClass.HasSideEffects() {
		return "side_effecting_step_requires_resolve_unknown", nil
	}
	if err := r.event(ctx, "step.retry_requested", "user", req.ActorID, &st.ID, nil,
		map[string]string{"command_id": cmd.ID, "reason": req.Reason}); err != nil {
		return "", err
	}
	plan := PlannedStep{Kind: st.Kind, Executor: st.ExecutorName, InputRef: st.InputRef, InputHash: st.InputHash,
		Status: domain.StepPending, Intent: intent}
	if r.run.Status == domain.RunPaused {
		_, err := r.insertStep(ctx, plan)
		return "", err
	}
	return "", r.appendPlan(ctx, []PlannedStep{plan})
}

// migrateProfile moves an idle run to another version of its profile. Pending
// approvals must be decided first: they were evaluated under the old policy.
func (s *Store) migrateProfile(ctx context.Context, r *runTx, cmd *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		ProfileVersion int64 `json:"profile_version"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || p.ProfileVersion <= 0 {
		return "profile_version_required", nil
	}
	if r.run.Status != domain.RunPaused && r.run.Status != domain.RunQueued && r.run.Status != domain.RunNeedsHuman {
		return "invalid_state", nil
	}
	var pending int
	if err := r.tx.QueryRow(ctx, `SELECT count(*) FROM approval_requests a JOIN action_intents i ON i.id=a.action_intent_id
		WHERE i.run_id=$1 AND a.status='PENDING'`, r.run.ID).Scan(&pending); err != nil {
		return "", err
	}
	if pending > 0 {
		return "pending_approvals_must_be_resolved", nil
	}
	var status string
	err := r.tx.QueryRow(ctx, `SELECT status FROM agent_profiles WHERE tenant_id=$1 AND profile_id=$2 AND version=$3`,
		r.run.TenantID, r.run.ProfileID, p.ProfileVersion).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "unknown_profile_version", nil
	}
	if err != nil {
		return "", err
	}
	if p.ProfileVersion == r.run.ProfileVersion {
		return "", nil
	}
	if status != "ACTIVE" {
		return "profile_version_not_runnable", nil
	}
	from := r.run.ProfileVersion
	r.run.ProfileVersion = p.ProfileVersion
	r.run.Version++
	return "", r.event(ctx, "run.profile_migrated", "user", req.ActorID, nil, nil, map[string]any{
		"command_id": cmd.ID, "from_version": from, "to_version": p.ProfileVersion, "reason": req.Reason})
}

// StepOutputRevisions lists the revision history of a step's output.
type OutputRevision struct {
	Revision            int     `json:"revision"`
	OutputRef           string  `json:"output_ref"`
	Source              string  `json:"source"`
	CommandID           *string `json:"command_id,omitempty"`
	Reason              *string `json:"reason,omitempty"`
	SideEffectsReverted bool    `json:"side_effects_reverted"`
}

func (s *Store) StepOutputRevisions(ctx context.Context, tenantID, stepID string) ([]OutputRevision, error) {
	rows, err := s.pool.Query(ctx, `SELECT revision, output_ref, source, command_id, reason, side_effects_reverted
		FROM step_output_revisions WHERE tenant_id=$1 AND step_id=$2 ORDER BY revision`, tenantID, stepID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (OutputRevision, error) {
		var o OutputRevision
		err := row.Scan(&o.Revision, &o.OutputRef, &o.Source, &o.CommandID, &o.Reason, &o.SideEffectsReverted)
		return o, err
	})
}
