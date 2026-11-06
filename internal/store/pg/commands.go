package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
)

// ApplyCommand persists and applies an intervention command atomically
// (§4.5, §13.3). "Accepted" (APPLIED here) means the desired state or
// approval was recorded; whether the run has actually paused is a separate
// event (run.paused). Rejected commands are persisted for audit.
//
// Version rules (v1.1.1): pause / cancel / inject_evidence accept an optional
// expected_version; resume / approve / reject / resolve require it.
func (s *Store) ApplyCommand(ctx context.Context, tenantID, runID string, req domain.CommandRequest) (domain.Command, error) {
	var cmd domain.Command
	var rejected error
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		r, err := s.lockRun(ctx, tx, tenantID, runID)
		if err != nil {
			return err
		}
		existing, err := getCommand(ctx, tx, tenantID, "", runID, req.IdempotencyKey)
		if err == nil {
			cmd = existing
			if existing.Status == domain.CommandRejected && existing.RejectReason != nil {
				rejected = rejection(*existing.RejectReason, existing)
			}
			return nil
		}
		if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		payload := req.Payload
		if len(payload) == 0 {
			payload = json.RawMessage(`{}`)
		}
		cmd = domain.Command{ID: newID(), TenantID: tenantID, RunID: runID, Type: req.Type, ActorID: req.ActorID,
			ExpectedVersion: req.ExpectedVersion, Status: domain.CommandPending, Payload: payload}
		if _, err := tx.Exec(ctx, `INSERT INTO run_commands (id, tenant_id, run_id, idempotency_key, command_type,
			actor_id, expected_version, payload) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, cmd.ID, tenantID, runID,
			req.IdempotencyKey, req.Type, req.ActorID, req.ExpectedVersion, jsonb(payload)); err != nil {
			return err
		}
		if err := r.event(ctx, "command.received", "user", req.ActorID, nil, nil,
			map[string]any{"command_id": cmd.ID, "type": req.Type, "reason": req.Reason}); err != nil {
			return err
		}

		// A rejected command must not change anything but its own record.
		// The savepoint undoes partial writes, and it also recovers the
		// transaction when the rejection came from a failed statement
		// (unique violation), which otherwise aborts every later statement.
		before := r.run
		if _, err := tx.Exec(ctx, `SAVEPOINT apply_command`); err != nil {
			return err
		}
		reason, err := s.applyCommand(ctx, r, &cmd, req)
		if err != nil {
			return err
		}
		if reason != "" {
			if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT apply_command`); err != nil {
				return err
			}
			r.run = before
		} else if _, err := tx.Exec(ctx, `RELEASE SAVEPOINT apply_command`); err != nil {
			return err
		}
		now := r.now
		cmd.AppliedAt = &now
		if reason != "" {
			cmd.Status, cmd.RejectReason = domain.CommandRejected, &reason
			rejected = rejection(reason, cmd)
			if err := r.event(ctx, "command.rejected", "user", req.ActorID, nil, nil,
				map[string]string{"command_id": cmd.ID, "reason": reason}); err != nil {
				return err
			}
		} else {
			cmd.Status = domain.CommandApplied
			if err := r.event(ctx, "command.applied", "user", req.ActorID, nil, nil,
				map[string]string{"command_id": cmd.ID}); err != nil {
				return err
			}
		}
		v := r.run.Version
		cmd.RunVersionAfter = &v
		if _, err := tx.Exec(ctx, `UPDATE run_commands SET status=$2, reject_reason=$3, run_version_after=$4, applied_at=now()
			WHERE id=$1`, cmd.ID, cmd.Status, cmd.RejectReason, v); err != nil {
			return err
		}
		return r.save(ctx)
	})
	if err != nil {
		return domain.Command{}, err
	}
	return cmd, rejected
}

func rejection(reason string, cmd domain.Command) error {
	if reason == "version_conflict" {
		return fmt.Errorf("%w: %w", domain.ErrVersionConflict, &domain.RejectedError{Reason: reason, Cmd: cmd})
	}
	return &domain.RejectedError{Reason: reason, Cmd: cmd}
}

// applyCommand returns a non-empty rejection reason for domain rejections.
func (s *Store) applyCommand(ctx context.Context, r *runTx, cmd *domain.Command, req domain.CommandRequest) (string, error) {
	run := &r.run
	needsVersion := map[domain.CommandType]bool{domain.CmdResume: true, domain.CmdApproveTool: true,
		domain.CmdRejectTool: true, domain.CmdResolveUnknown: true, domain.CmdOverrideResult: true,
		domain.CmdReplacePendingInput: true, domain.CmdRetry: true, domain.CmdMigrateProfile: true}
	if needsVersion[req.Type] && req.ExpectedVersion == nil {
		return "expected_version_required", nil
	}
	if req.ExpectedVersion != nil && *req.ExpectedVersion != run.Version {
		return "version_conflict", nil
	}
	if run.Status.Terminal() {
		return "run_terminal", nil
	}
	if reason, handled, err := s.applyV11(ctx, r, cmd, req); handled || err != nil {
		return reason, err
	}
	hasLease := run.Status == domain.RunRunning && run.LeaseUntil != nil && run.LeaseUntil.After(r.now)

	switch req.Type {
	case domain.CmdPause:
		switch {
		case run.Status == domain.RunPaused:
			return "", nil
		case hasLease || run.Status == domain.RunRunning:
			if run.DesiredState != domain.DesiredPaused {
				run.DesiredState = domain.DesiredPaused
				run.Version++
			}
			return "", r.event(ctx, "run.pause_requested", "user", req.ActorID, nil, nil, map[string]string{"command_id": cmd.ID})
		case run.Status == domain.RunNeedsHuman:
			return "invalid_state", nil
		case run.Status == domain.RunPendingActivation:
			// Not schedulable yet: record intent; activation's settle() pauses it (§29.3).
			if run.DesiredState != domain.DesiredPaused {
				run.DesiredState = domain.DesiredPaused
				run.Version++
			}
			return "", r.event(ctx, "run.pause_requested", "user", req.ActorID, nil, nil, map[string]string{"command_id": cmd.ID})
		default: // QUEUED, WAITING_APPROVAL: no lease, commit directly
			run.DesiredState = domain.DesiredPaused
			run.NotBefore = nil
			r.setStatus(domain.RunPaused)
			return "", r.event(ctx, "run.paused", "user", req.ActorID, nil, nil, map[string]string{"command_id": cmd.ID})
		}

	case domain.CmdResume:
		switch run.Status {
		case domain.RunPaused:
			run.DesiredState = domain.DesiredRunning
			r.setStatus(domain.RunQueued)
			if err := r.event(ctx, "run.resumed", "user", req.ActorID, nil, nil, map[string]string{"command_id": cmd.ID}); err != nil {
				return "", err
			}
			return "", r.settle(ctx)
		case domain.RunRunning, domain.RunPendingActivation:
			if run.DesiredState == domain.DesiredPaused {
				run.DesiredState = domain.DesiredRunning
				run.Version++
				return "", r.event(ctx, "run.pause_withdrawn", "user", req.ActorID, nil, nil, nil)
			}
		}
		return "invalid_state", nil

	case domain.CmdCancel:
		if hasLease {
			if run.DesiredState != domain.DesiredCancelled {
				run.DesiredState = domain.DesiredCancelled
				run.Version++
			}
			return "", r.event(ctx, "run.cancel_requested", "user", req.ActorID, nil, nil, map[string]string{"command_id": cmd.ID})
		}
		if run.Status == domain.RunRunning { // expired lease: recover first so the attempt is accounted for
			if err := s.recoverExpired(ctx, r); err != nil {
				return "", err
			}
		}
		return "", r.cancel(ctx, "user", req.ActorID, req.Reason)

	case domain.CmdInjectEvidence:
		var p struct {
			Content string `json:"content"`
			Source  string `json:"source"`
		}
		if json.Unmarshal(req.Payload, &p) != nil || p.Content == "" || p.Source == "" {
			return "evidence_requires_content_and_source", nil
		}
		data, _ := json.Marshal(domain.Evidence{Content: p.Content, Source: p.Source, ActorID: req.ActorID, CreatedAt: r.now})
		key := "evidence/" + cmd.ID
		if err := r.event(ctx, "evidence.injected", "user", req.ActorID, nil, nil,
			map[string]string{"command_id": cmd.ID, "kv_key": key, "source": p.Source}); err != nil {
			return "", err
		}
		// Takes effect in the next context snapshot only; running steps keep theirs.
		_, err := kvWrite(ctx, r.tx, KVWrite{Key: KVKey{TenantID: run.TenantID, Scope: "run", OwnerID: run.ID, Name: key},
			Data: data, SchemaID: "evidence/v1"}, kvProvenance{RunID: run.ID, ActorType: "user", ActorID: req.ActorID})
		return "", err

	case domain.CmdApproveTool, domain.CmdRejectTool:
		return s.decideApproval(ctx, r, cmd, req)

	case domain.CmdResolveUnknown:
		return s.resolveUnknown(ctx, r, cmd, req)
	}
	return "unsupported_command", nil
}

func (s *Store) decideApproval(ctx context.Context, r *runTx, cmd *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		ApprovalRequestID string `json:"approval_request_id"`
		ActionHash        string `json:"action_hash"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || !validID(p.ApprovalRequestID) || p.ActionHash == "" {
		return "approval_request_id_and_action_hash_required", nil
	}
	var status, intentHash, stepID, intentID string
	var required int
	var expires time.Time
	err := r.tx.QueryRow(ctx, `SELECT a.status, i.action_hash, i.step_id, i.id, a.required_approvals, a.expires_at
		FROM approval_requests a JOIN action_intents i ON i.id = a.action_intent_id
		WHERE a.id=$1 AND a.tenant_id=$2 AND i.run_id=$3 FOR UPDATE OF a`,
		p.ApprovalRequestID, r.run.TenantID, r.run.ID).Scan(&status, &intentHash, &stepID, &intentID, &required, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return "approval_not_found", nil
	}
	if err != nil {
		return "", err
	}
	if status != "PENDING" {
		return "approval_not_pending", nil
	}
	if !expires.After(r.now) {
		return "approval_expired", nil
	}
	if p.ActionHash != intentHash {
		return "action_hash_mismatch", nil // approval must bind the exact action
	}
	decision := "APPROVE"
	if req.Type == domain.CmdRejectTool {
		decision = "REJECT"
	}
	if decision == "APPROVE" && s.cfg.RequireDistinctApprover && r.run.InitiatorID != nil && *r.run.InitiatorID == req.ActorID {
		return "approver_is_initiator", nil
	}
	if _, err := r.tx.Exec(ctx, `INSERT INTO approval_decisions (tenant_id, approval_request_id, approver_id, decision,
		action_hash, command_id) VALUES ($1,$2,$3,$4,$5,$6)`, r.run.TenantID, p.ApprovalRequestID, req.ActorID,
		decision, p.ActionHash, cmd.ID); err != nil {
		if isUniqueViolation(err) {
			return "approver_already_decided", nil
		}
		return "", err
	}

	if decision == "REJECT" {
		if _, err := r.tx.Exec(ctx, `UPDATE approval_requests SET status='REJECTED', decided_at=now() WHERE id=$1`,
			p.ApprovalRequestID); err != nil {
			return "", err
		}
		if err := r.event(ctx, "approval.decided", "user", req.ActorID, &stepID, nil,
			map[string]string{"approval_request_id": p.ApprovalRequestID, "decision": "rejected"}); err != nil {
			return "", err
		}
		return "", s.skipBlockedStep(ctx, r, stepID, "rejected by approver")
	}

	var approvals int
	if err := r.tx.QueryRow(ctx, `SELECT count(*) FROM approval_decisions WHERE approval_request_id=$1 AND decision='APPROVE'`,
		p.ApprovalRequestID).Scan(&approvals); err != nil {
		return "", err
	}
	if approvals < required {
		return "", r.event(ctx, "approval.partial", "user", req.ActorID, &stepID, nil,
			map[string]any{"approval_request_id": p.ApprovalRequestID, "approvals": approvals, "required": required})
	}
	if _, err := r.tx.Exec(ctx, `UPDATE approval_requests SET status='APPROVED', decided_at=now() WHERE id=$1`,
		p.ApprovalRequestID); err != nil {
		return "", err
	}
	if _, err := r.tx.Exec(ctx, `INSERT INTO approval_grants (id, tenant_id, approval_request_id, action_hash, expires_at)
		VALUES ($1,$2,$3,$4,$5)`, newID(), r.run.TenantID, p.ApprovalRequestID, intentHash, expires); err != nil {
		return "", err
	}
	if _, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status='PENDING', updated_at=now() WHERE id=$1 AND status='WAITING_APPROVAL'`,
		stepID); err != nil {
		return "", err
	}
	if err := r.event(ctx, "approval.decided", "user", req.ActorID, &stepID, nil,
		map[string]string{"approval_request_id": p.ApprovalRequestID, "decision": "approved"}); err != nil {
		return "", err
	}
	if r.run.Status == domain.RunWaitingApproval {
		r.setStatus(domain.RunQueued)
		return "", r.settle(ctx)
	}
	return "", nil // e.g. PAUSED: resume will pick the now-pending step
}

// skipBlockedStep marks a blocked tool step SKIPPED and lets the agent re-plan.
func (s *Store) skipBlockedStep(ctx context.Context, r *runTx, stepID, reason string) error {
	if _, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status='SKIPPED', status_reason=$2, updated_at=now() WHERE id=$1`,
		stepID, reason); err != nil {
		return err
	}
	if err := r.event(ctx, "step.skipped", "system", "", &stepID, nil, map[string]string{"reason": reason}); err != nil {
		return err
	}
	next, err := r.reasonStep(ctx)
	if err != nil {
		return err
	}
	if r.run.Status == domain.RunWaitingApproval {
		return r.appendPlan(ctx, []PlannedStep{next})
	}
	// Paused (or otherwise idle): plan the step but keep the current state.
	if int(r.run.NextStepSeq) > r.stepBudget(ctx) {
		return r.fail(ctx, "step budget exhausted")
	}
	_, err = r.insertStep(ctx, next)
	return err
}

func (s *Store) resolveUnknown(ctx context.Context, r *runTx, cmd *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		StepID     string `json:"step_id"`
		Resolution string `json:"resolution"` // succeeded | failed | retry
		Note       string `json:"note"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || !validID(p.StepID) {
		return "step_id_required", nil
	}
	if r.run.Status != domain.RunNeedsHuman {
		return "invalid_state", nil
	}
	if p.Resolution != "succeeded" && p.Resolution != "failed" && p.Resolution != "retry" {
		return "invalid_resolution", nil
	}
	tag, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status=$3, status_reason=$4, updated_at=now()
		WHERE id=$1 AND run_id=$2 AND status='UNKNOWN'`, p.StepID, r.run.ID,
		map[string]string{"succeeded": "SUCCEEDED", "failed": "FAILED", "retry": "PENDING"}[p.Resolution],
		"resolved by human: "+p.Resolution+" "+p.Note)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() != 1 {
		return "step_not_unknown", nil
	}
	effect := map[string]string{"succeeded": "CONFIRMED", "failed": "COMPENSATED", "retry": "PREPARED"}[p.Resolution]
	if _, err := r.tx.Exec(ctx, `UPDATE external_tool_effects SET state=$2, last_check_at=now() WHERE step_id=$1`,
		p.StepID, effect); err != nil {
		return "", err
	}
	if err := r.event(ctx, "step.resolved", "user", req.ActorID, &p.StepID, nil,
		map[string]string{"command_id": cmd.ID, "resolution": p.Resolution, "note": p.Note}); err != nil {
		return "", err
	}
	switch p.Resolution {
	case "retry":
		r.setStatus(domain.RunQueued)
		return "", r.settle(ctx)
	default:
		// succeeded / failed: the operator established the outcome; the agent
		// observes it (a failed step is shown as not applied) and re-plans.
		next, err := r.reasonStep(ctx)
		if err != nil {
			return "", err
		}
		return "", r.appendPlan(ctx, []PlannedStep{next})
	}
}

func (s *Store) GetCommand(ctx context.Context, tenantID, runID, commandID string) (domain.Command, error) {
	if !validID(runID) || !validID(commandID) {
		return domain.Command{}, domain.ErrNotFound
	}
	return getCommand(ctx, s.pool, tenantID, commandID, runID, "")
}

func getCommand(ctx context.Context, q querier, tenantID, id, runID, idemKey string) (domain.Command, error) {
	var c domain.Command
	err := q.QueryRow(ctx, `SELECT id, tenant_id, run_id, command_type, actor_id, expected_version, status, reject_reason,
		payload, run_version_after, created_at, applied_at FROM run_commands
		WHERE tenant_id=$1 AND run_id=$2 AND (($3 <> '' AND id::text=$3) OR ($4 <> '' AND idempotency_key=$4))`,
		tenantID, runID, id, idemKey).Scan(&c.ID, &c.TenantID, &c.RunID, &c.Type, &c.ActorID, &c.ExpectedVersion,
		&c.Status, &c.RejectReason, &c.Payload, &c.RunVersionAfter, &c.CreatedAt, &c.AppliedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, domain.ErrNotFound
	}
	return c, err
}
