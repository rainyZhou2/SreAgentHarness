package pg

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
)

// ClaimStep takes the execution lease for the run's next runnable step (§14.2).
// It is the only path to execution and always runs in one short transaction:
//
//	lock run → settle desired_state → recover an expired lease → pick step →
//	approval-grant check → fence++ / lease → attempt → snapshot pin → events.
//
// Notifications are wake-ups, not facts: duplicates get ErrNoRunnableStep or
// ErrLeaseHeld. State changes made before returning those errors (e.g. a paused
// run committed at a safe point) are still committed.
func (s *Store) ClaimStep(ctx context.Context, runID, workerID string, ttl time.Duration) (domain.Lease, error) {
	var lease domain.Lease
	var outcome error
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		r, err := s.lockRun(ctx, tx, "", runID)
		if err != nil {
			return err
		}
		lease, outcome, err = s.claim(ctx, r, workerID, ttl)
		if err != nil {
			return err
		}
		return r.save(ctx)
	})
	if err != nil {
		return domain.Lease{}, err
	}
	return lease, outcome
}

func (s *Store) claim(ctx context.Context, r *runTx, workerID string, ttl time.Duration) (domain.Lease, error, error) {
	noWork := domain.ErrNoRunnableStep
	switch r.run.Status {
	case domain.RunRunning:
		if r.run.LeaseUntil != nil && r.run.LeaseUntil.After(r.now) {
			return domain.Lease{}, domain.ErrLeaseHeld, nil
		}
		if err := s.recoverExpired(ctx, r); err != nil {
			return domain.Lease{}, nil, err
		}
		if r.run.Status != domain.RunQueued {
			return domain.Lease{}, noWork, nil
		}
	case domain.RunQueued:
	default: // PENDING_ACTIVATION, PAUSED, WAITING_APPROVAL, NEEDS_HUMAN, terminal
		return domain.Lease{}, noWork, nil
	}

	switch r.run.DesiredState {
	case domain.DesiredPaused:
		r.setStatus(domain.RunPaused)
		return domain.Lease{}, noWork, r.event(ctx, "run.paused", "system", "", nil, nil, nil)
	case domain.DesiredCancelled:
		return domain.Lease{}, noWork, r.cancel(ctx, "system", "", "cancel requested")
	}
	if r.run.NotBefore != nil && r.run.NotBefore.After(r.now) {
		return domain.Lease{}, noWork, nil
	}

	step, err := scanStep(r.tx.QueryRow(ctx, `SELECT `+stepCols+` FROM agent_steps
		WHERE run_id=$1 AND status IN ('PENDING','RETRY_WAIT','WAITING_APPROVAL','WAITING_CHILDREN')
		ORDER BY seq LIMIT 1`, r.run.ID))
	if errors.Is(err, domain.ErrNotFound) {
		return domain.Lease{}, noWork, nil
	}
	if err != nil {
		return domain.Lease{}, nil, err
	}
	if step.Status == domain.StepWaitingApproval {
		r.setStatus(domain.RunWaitingApproval)
		return domain.Lease{}, noWork, nil
	}
	if step.Status == domain.StepWaitingChildren {
		ready, err := s.flowChildrenReady(ctx, r)
		if err != nil {
			return domain.Lease{}, nil, err
		}
		if !ready {
			return domain.Lease{}, noWork, nil
		}
	}
	if step.Status == domain.StepRetryWait && step.NextAttemptAt != nil && step.NextAttemptAt.After(r.now) {
		r.run.NotBefore = step.NextAttemptAt
		return domain.Lease{}, noWork, r.wake(ctx, step.NextAttemptAt)
	}

	if s.cfg.MaxRunningPerTenant > 0 {
		var running int
		if err := r.tx.QueryRow(ctx, `SELECT COUNT(*) FROM agent_runs WHERE tenant_id=$1 AND status='RUNNING'
			AND lease_until > $2 AND id <> $3`, r.run.TenantID, r.now, r.run.ID).Scan(&running); err != nil {
			return domain.Lease{}, nil, err
		}
		if running >= s.cfg.MaxRunningPerTenant {
			delay := s.cfg.TenantDeferDelay
			if delay <= 0 {
				delay = time.Second
			}
			at := r.now.Add(delay)
			r.run.NotBefore = &at
			return domain.Lease{}, noWork, r.wake(ctx, &at)
		}
	}

	attemptID := newID()
	var intent *domain.ActionIntent
	if step.Kind == domain.StepTool || step.Kind == domain.StepFlow || step.Kind == domain.StepPlanReview {
		intent, err = loadIntent(ctx, r.tx, r.run.TenantID, step.ID)
		if err != nil {
			return domain.Lease{}, nil, err
		}
		if step.Kind == domain.StepTool && intent == nil {
			return domain.Lease{}, nil, r.fail(ctx, "tool step without action intent")
		}
		if step.Kind == domain.StepPlanReview && intent == nil {
			return domain.Lease{}, nil, r.fail(ctx, "plan review step without action intent")
		}
	}
	if intent != nil {
		ok, err := s.checkApprovalGrant(ctx, r, step, intent, attemptID)
		if err != nil {
			return domain.Lease{}, nil, err
		}
		if !ok {
			return domain.Lease{}, noWork, nil
		}
	}

	// Take the lease. Fence++ invalidates every older holder; run.version only
	// changes because the visible status changes (QUEUED -> RUNNING).
	r.run.FenceToken++
	until := r.now.Add(ttl)
	r.run.LeaseOwner, r.run.LeaseUntil, r.run.NotBefore = &workerID, &until, nil
	r.setStatus(domain.RunRunning)

	actionKey := step.InputHash
	if intent != nil {
		actionKey = intent.ActionHash
	}
	idemKey := hashStrings(r.run.TenantID, r.run.ID, step.ID, actionKey)
	attemptNo := step.CurrentAttempt + 1
	if _, err := r.tx.Exec(ctx, `INSERT INTO step_attempts
		(id, tenant_id, run_id, step_id, attempt_no, worker_id, fence_token, status, idempotency_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'RUNNING',$8)`,
		attemptID, r.run.TenantID, r.run.ID, step.ID, attemptNo, workerID, r.run.FenceToken, idemKey); err != nil {
		return domain.Lease{}, nil, err
	}

	snapshotID := ""
	if step.ContextSnapshotID != nil {
		snapshotID = *step.ContextSnapshotID // retries reuse the first attempt's snapshot
	} else if snapshotID, err = s.pinSnapshot(ctx, r, step); err != nil {
		return domain.Lease{}, nil, err
	}
	if _, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status='RUNNING', current_attempt=$2, context_snapshot_id=$3,
		next_attempt_at=NULL, updated_at=now() WHERE id=$1`, step.ID, attemptNo, snapshotID); err != nil {
		return domain.Lease{}, nil, err
	}
	r.run.ContextSnapshotID = &snapshotID

	if attemptNo == 1 {
		if err := r.event(ctx, "step.started", "worker", workerID, &step.ID, nil, map[string]any{"seq": step.Seq, "kind": step.Kind}); err != nil {
			return domain.Lease{}, nil, err
		}
	}
	if err := r.event(ctx, "attempt.started", "worker", workerID, &step.ID, &attemptID, map[string]any{
		"attempt_no": attemptNo, "fence_token": r.run.FenceToken, "context_snapshot_id": snapshotID,
	}); err != nil {
		return domain.Lease{}, nil, err
	}

	return domain.Lease{
		TenantID: r.run.TenantID, RunID: r.run.ID, StepID: step.ID, StepSeq: step.Seq, AttemptID: attemptID,
		AttemptNo: attemptNo, WorkerID: workerID, FenceToken: r.run.FenceToken, Kind: step.Kind,
		ExecutorName: step.ExecutorName, FlowNodeID: step.FlowNodeID, InputRef: step.InputRef, InputHash: step.InputHash,
		GoalRef: r.run.GoalRef, ContextSnapshotID: snapshotID, IdempotencyKey: idemKey, Intent: intent,
		ProfileID: r.run.ProfileID, ProfileVersion: r.run.ProfileVersion,
		ParentRunID: deref(r.run.ParentRunID), ParentStepID: deref(r.run.ParentStepID), FlowDepth: r.run.FlowDepth,
	}, nil, nil
}

// flowChildrenReady reports whether every child run of the coordinator has
// reached a terminal flow_node_runs status. Child completion wakes the parent,
// so a false result only means "do not claim yet".
func (s *Store) flowChildrenReady(ctx context.Context, r *runTx) (bool, error) {
	var pending int
	err := r.tx.QueryRow(ctx, `SELECT count(*) FROM flow_node_runs n
		JOIN flow_runs f ON f.id = n.flow_run_id AND f.tenant_id = n.tenant_id AND n.generation = f.generation
		WHERE f.tenant_id=$1 AND f.run_id=$2 AND n.status IN ('PENDING','RUNNING')`,
		r.run.TenantID, r.run.ID).Scan(&pending)
	return pending == 0, err
}

// checkApprovalGrant enforces one-time approval consumption for tool steps
// whose intent ever required approval (§13.6). Returns false when the step
// must (again) wait for a human.
func (s *Store) checkApprovalGrant(ctx context.Context, r *runTx, step domain.Step, intent *domain.ActionIntent, attemptID string) (bool, error) {
	var needs bool
	if err := r.tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM approval_requests WHERE action_intent_id=$1)`,
		intent.ID).Scan(&needs); err != nil {
		return false, err
	}
	if !needs {
		return true, nil
	}
	if step.CurrentAttempt == 0 {
		var reqID string
		err := r.tx.QueryRow(ctx, `UPDATE approval_grants g SET used_count = used_count + 1, consumed_by_attempt_id = $3
			FROM approval_requests a
			WHERE g.approval_request_id = a.id AND a.action_intent_id = $1 AND a.status = 'APPROVED'
			  AND g.action_hash = $2 AND g.used_count < g.max_uses AND g.expires_at > now()
			RETURNING a.id`, intent.ID, intent.ActionHash, attemptID).Scan(&reqID)
		if err == nil {
			_, err = r.tx.Exec(ctx, `UPDATE approval_requests SET status='CONSUMED' WHERE id=$1`, reqID)
			if err == nil {
				err = r.event(ctx, "approval.consumed", "system", "", &step.ID, nil, map[string]string{"approval_request_id": reqID})
			}
			return err == nil, err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return false, err
		}
	} else {
		// Retry of an already-approved action with the same action_hash.
		var ok bool
		if err := r.tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM approval_grants g JOIN approval_requests a
			ON a.id = g.approval_request_id WHERE a.action_intent_id=$1 AND a.status='CONSUMED'
			AND g.action_hash=$2 AND g.used_count >= 1)`, intent.ID, intent.ActionHash).Scan(&ok); err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	// No valid grant: back to waiting, with a fresh request if none is pending.
	if _, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status='WAITING_APPROVAL', updated_at=now() WHERE id=$1`, step.ID); err != nil {
		return false, err
	}
	var pending bool
	if err := r.tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM approval_requests WHERE action_intent_id=$1 AND status='PENDING')`,
		intent.ID).Scan(&pending); err != nil {
		return false, err
	}
	if !pending {
		// Keep the approval count the intent was planned with (profile
		// approval_rules may require more than one approver).
		var required int
		if err := r.tx.QueryRow(ctx, `SELECT COALESCE(MAX(required_approvals), 1) FROM approval_requests WHERE action_intent_id=$1`,
			intent.ID).Scan(&required); err != nil {
			return false, err
		}
		if err := r.requestApproval(ctx, step.ID, intent.ID, intent, required); err != nil {
			return false, err
		}
	}
	r.setStatus(domain.RunWaitingApproval)
	return false, r.event(ctx, "run.waiting_approval", "system", "", &step.ID, nil, nil)
}

// pinSnapshot is phase ① of the snapshot lifecycle (§10.2): it records only
// version numbers, never reads content or calls retrieval.
func (s *Store) pinSnapshot(ctx context.Context, r *runTx, step domain.Step) (string, error) {
	kv := map[string]int64{}
	rows, err := r.tx.Query(ctx, `SELECT key, version FROM agent_kv WHERE tenant_id=$1 AND scope='run' AND owner_id=$2
		AND NOT tombstone AND (expires_at IS NULL OR expires_at > now())`, r.run.TenantID, r.run.ID)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var k string
		var v int64
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			return "", err
		}
		kv[k] = v
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", err
	}
	var incidentVersion *int64
	if r.run.IncidentID != nil {
		var v int64
		if err := r.tx.QueryRow(ctx, `SELECT memory_version FROM incidents WHERE id=$1`, *r.run.IncidentID).Scan(&v); err != nil {
			return "", err
		}
		incidentVersion = &v
	}
	id := newID()
	_, err = r.tx.Exec(ctx, `INSERT INTO context_snapshots (id, tenant_id, run_id, step_id, profile_id, profile_version,
		incident_memory_version, kv_versions, refs) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		id, r.run.TenantID, r.run.ID, step.ID, r.run.ProfileID, r.run.ProfileVersion, incidentVersion, jsonb(kv),
		jsonb(map[string]any{"history_before_seq": step.Seq, "goal_ref": r.run.GoalRef}))
	return id, err
}

// recoverExpired handles a RUNNING run whose lease expired (worker crash,
// partition). The attempt becomes LOST; the step is retried only if retrying
// cannot duplicate an external effect (§5.3, §14.3).
func (s *Store) recoverExpired(ctx context.Context, r *runTx) error {
	var attemptID, stepID string
	var kind domain.StepKind
	var attemptNo int
	var idemKey string
	err := r.tx.QueryRow(ctx, `SELECT a.id, a.step_id, s.kind, a.attempt_no, a.idempotency_key FROM step_attempts a
		JOIN agent_steps s ON s.id = a.step_id WHERE a.run_id=$1 AND a.status='RUNNING'
		ORDER BY a.started_at DESC LIMIT 1`, r.run.ID).Scan(&attemptID, &stepID, &kind, &attemptNo, &idemKey)
	r.releaseLease()
	if errors.Is(err, pgx.ErrNoRows) {
		r.setStatus(domain.RunQueued)
		return r.event(ctx, "lease.expired", "system", "", nil, nil, nil)
	}
	if err != nil {
		return err
	}
	if _, err := r.tx.Exec(ctx, `UPDATE step_attempts SET status='LOST', ended_at=now(), error_code='LEASE_EXPIRED'
		WHERE id=$1`, attemptID); err != nil {
		return err
	}
	if err := r.event(ctx, "attempt.lost", "system", "", &stepID, &attemptID, map[string]any{"attempt_no": attemptNo}); err != nil {
		return err
	}

	unsafe := false
	if kind == domain.StepTool {
		var state string
		err := r.tx.QueryRow(ctx, `SELECT state FROM external_tool_effects WHERE tenant_id=$1 AND idempotency_key=$2
			FOR UPDATE`, r.run.TenantID, idemKey).Scan(&state)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return err
		case state == "DISPATCHED" || state == "UNKNOWN":
			unsafe = true
			if _, err := r.tx.Exec(ctx, `UPDATE external_tool_effects SET state='UNKNOWN', last_check_at=now()
				WHERE tenant_id=$1 AND idempotency_key=$2`, r.run.TenantID, idemKey); err != nil {
				return err
			}
		}
	}
	if unsafe {
		if _, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status='UNKNOWN', updated_at=now() WHERE id=$1`, stepID); err != nil {
			return err
		}
		r.setStatus(domain.RunNeedsHuman)
		return r.event(ctx, "step.unknown", "system", "", &stepID, &attemptID,
			map[string]string{"reason": "worker lost after dispatching a side-effecting tool"})
	}
	if attemptNo >= s.cfg.MaxAttempts {
		if _, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status='FAILED', updated_at=now() WHERE id=$1`, stepID); err != nil {
			return err
		}
		return r.fail(ctx, "attempts exhausted after lost leases")
	}
	if _, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status='PENDING', updated_at=now() WHERE id=$1`, stepID); err != nil {
		return err
	}
	r.setStatus(domain.RunQueued)
	return nil
}

// RenewLease extends a live lease without touching run.version (§14.2). It
// returns the desired state so the worker can cooperatively cancel.
func (s *Store) RenewLease(ctx context.Context, lease domain.Lease, ttl time.Duration) (domain.DesiredState, error) {
	var desired domain.DesiredState
	err := s.pool.QueryRow(ctx, `UPDATE agent_runs SET lease_until = now() + $4 * interval '1 millisecond'
		WHERE id=$1 AND fence_token=$2 AND lease_owner=$3 AND status='RUNNING' AND lease_until > now()
		RETURNING desired_state`, lease.RunID, lease.FenceToken, lease.WorkerID, ttl.Milliseconds()).Scan(&desired)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", domain.ErrLeaseLost
	}
	return desired, err
}
