package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
)

// CommitSuccess is the fenced result commit (§4.4, §15.2): attempt/step result,
// cognitive artifact, run-private KV, effect confirmation, events, next steps,
// lease release and wake-up — one transaction. A stale worker gets ErrLeaseLost.
func (s *Store) CommitSuccess(ctx context.Context, lease domain.Lease, res StepSuccess) error {
	return s.inKernelTx(ctx, func(tx pgx.Tx) error {
		r, err := s.lockLeased(ctx, tx, lease)
		if err != nil {
			return err
		}
		if err := s.finishAttempt(ctx, r, lease, domain.AttemptSucceeded, "", ""); err != nil {
			return err
		}
		stepStatus := domain.StepSucceeded
		switch res.Wait {
		case domain.WaitChildren:
			stepStatus = domain.StepWaitingChildren
		case domain.WaitInput:
			stepStatus = domain.StepWaitingInput
		}
		if _, err := tx.Exec(ctx, `UPDATE agent_steps SET status=$2, output_ref=$3, output_hash=$4,
			updated_at=now() WHERE id=$1`, lease.StepID, stepStatus, nullable(res.OutputRef), nullable(res.OutputHash)); err != nil {
			return err
		}
		if res.ArtifactKind != "" {
			if _, err := tx.Exec(ctx, `INSERT INTO cognitive_artifacts (id, tenant_id, run_id, step_id, kind, schema_version,
				artifact_ref, artifact_hash, context_snapshot_id) VALUES ($1,$2,$3,$4,$5,1,$6,$7,$8)`,
				newID(), lease.TenantID, lease.RunID, lease.StepID, res.ArtifactKind, res.OutputRef, res.OutputHash,
				lease.ContextSnapshotID); err != nil {
				return err
			}
		}
		if res.ContextHash != "" {
			if _, err := tx.Exec(ctx, `UPDATE context_snapshots SET context_hash=$2 WHERE id=$1 AND context_hash IS NULL`,
				lease.ContextSnapshotID, res.ContextHash); err != nil {
				return err
			}
		}
		if res.SideEffect {
			if _, err := tx.Exec(ctx, `UPDATE external_tool_effects SET state='CONFIRMED', result_ref=$3, last_check_at=now()
				WHERE tenant_id=$1 AND idempotency_key=$2`, lease.TenantID, lease.IdempotencyKey, res.OutputRef); err != nil {
				return err
			}
		}
		for _, w := range res.KVWrites {
			if w.Key.Scope != "run" || w.Key.OwnerID != lease.RunID || w.Key.TenantID != lease.TenantID {
				return errors.New("step commits may only write run-private KV")
			}
			if err := r.event(ctx, "kv.written", "worker", lease.WorkerID, &lease.StepID, &lease.AttemptID,
				map[string]any{"key": w.Key.Name}); err != nil {
				return err
			}
			if _, err := kvWrite(ctx, tx, w, kvProvenance{RunID: lease.RunID, StepID: lease.StepID,
				ActorType: "worker", ActorID: lease.WorkerID}); err != nil {
				return err
			}
		}
		if err := r.event(ctx, "step.succeeded", "worker", lease.WorkerID, &lease.StepID, &lease.AttemptID,
			res.Summary); err != nil {
			return err
		}
		for _, e := range res.Events {
			if e.Type == "" {
				continue
			}
			if err := r.event(ctx, e.Type, "system", "", &lease.StepID, &lease.AttemptID, e.Payload); err != nil {
				return err
			}
		}
		if err := insertThoughts(ctx, tx, lease.TenantID, lease.RunID, lease.StepID, res.Thoughts); err != nil {
			return err
		}
		if err := insertCapabilityRefs(ctx, tx, lease.TenantID, lease.RunID, lease.StepID, res.Capabilities); err != nil {
			return err
		}

		for _, h := range res.MemoryHits {
			if _, err := tx.Exec(ctx, `INSERT INTO step_memory_hits (tenant_id, step_id, memory_id, memory_version, score, rank)
				VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, lease.TenantID, lease.StepID, h.MemoryID, h.Version,
				h.Score, h.Rank); err != nil {
				return err
			}
		}
		if res.Compaction != nil {
			if _, err := tx.Exec(ctx, `UPDATE agent_steps SET summary=$2 WHERE id=$1`, lease.StepID,
				jsonb(res.Compaction)); err != nil {
				return err
			}
			r.run.CompactionEpoch++
			if err := r.event(ctx, "context.compacted", "worker", lease.WorkerID, &lease.StepID, &lease.AttemptID,
				map[string]any{"compaction_epoch": r.run.CompactionEpoch, "compacted_before": res.Compaction.CompactedBefore,
					"summary_ref": res.Compaction.SummaryRef}); err != nil {
				return err
			}
		}

		if res.Flow != nil {
			if err := s.applyFlowCommit(ctx, r, lease, res.Flow); err != nil {
				return err
			}
		}

		r.releaseLease()
		r.run.Version++ // step progress is control-plane visible
		if res.CheckpointRef != "" {
			if err := r.checkpoint(ctx, lease, res); err != nil {
				return err
			}
		}
		if res.Complete {
			r.run.ResultRef = &res.ResultRef
			r.setStatus(domain.RunCompleted)
			if err := r.event(ctx, "run.completed", "system", "", nil, nil, map[string]string{"result_ref": res.ResultRef}); err != nil {
				return err
			}
			if lease.ParentRunID != "" {
				if err := s.flowChildFinished(ctx, r, lease, "SUCCEEDED", res.ResultRef, ""); err != nil {
					return err
				}
			}
		} else if res.Wait != domain.WaitNone {
			switch res.Wait {
			case domain.WaitChildren:
				r.setStatus(domain.RunWaitingChildren)
				if err := r.event(ctx, "flow.waiting_children", "system", "", &lease.StepID, nil,
					map[string]string{"flow_node": lease.FlowNodeID}); err != nil {
					return err
				}
			case domain.WaitInput:
				r.setStatus(domain.RunWaitingInput)
				if err := r.event(ctx, "flow.waiting_input", "system", "", &lease.StepID, nil,
					map[string]string{"flow_node": lease.FlowNodeID}); err != nil {
					return err
				}
			}
		} else if err := r.appendPlan(ctx, res.Plan); err != nil {
			return err
		}
		return r.save(ctx)
	})
}

// applyFlowCommit persists the flow state and creates every child run in the
// same transaction as the parent step. Child creation is idempotent on the
// (flow run, node, attempt) key.
func (s *Store) applyFlowCommit(ctx context.Context, r *runTx, lease domain.Lease, fc *domain.FlowCommit) error {
	if fc == nil {
		return nil
	}
	flowRunID := fc.FlowRunID
	if flowRunID == "" {
		flowRunID = newID()
	}
	status := fc.Status
	if status == "" {
		status = "RUNNING"
	}
	state := fc.State
	if len(state) == 0 {
		state = []byte("{}")
	}
	var actualID string
	if err := r.tx.QueryRow(ctx, `INSERT INTO flow_runs
		(id, tenant_id, run_id, compiled_ref, compiled_hash, state, status, current_node, depth, child_runs, generation)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (tenant_id, run_id) DO UPDATE SET
		  compiled_ref=EXCLUDED.compiled_ref, compiled_hash=EXCLUDED.compiled_hash,
		  state=EXCLUDED.state, status=EXCLUDED.status, current_node=EXCLUDED.current_node,
		  child_runs=GREATEST(flow_runs.child_runs, EXCLUDED.child_runs),
		  generation=EXCLUDED.generation, updated_at=now()
		WHERE flow_runs.generation <= EXCLUDED.generation
		RETURNING id`,
		flowRunID, r.run.TenantID, r.run.ID, fc.CompiledRef, fc.CompiledHash, jsonb(state), status,
		fc.CurrentNode, r.run.FlowDepth, fc.ChildRuns, fc.Generation).Scan(&actualID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("flow commit for generation %d is older than the stored flow", fc.Generation)
		}
		return err
	}
	for _, child := range fc.Children {
		parentID := r.run.ID
		stepID := lease.StepID
		attempt := child.Attempt
		if attempt < 1 {
			attempt = 1
		}
		key := child.IdempotencyKey
		if key == "" {
			key = "flow:" + actualID + ":" + child.NodeID + ":" + itoa(attempt)
		}
		cr, isNew, err := s.createRunTx(ctx, r.tx, CreateRunRequest{
			TenantID: r.run.TenantID, ProfileID: child.ProfileID, ProfileVersion: child.ProfileVersion,
			GoalRef: child.GoalRef, GoalHash: child.GoalHash, IdempotencyKey: key,
			InitiatorType: "system", ParentRunID: &parentID, ParentStepID: &stepID,
			FlowNodeID: child.NodeID, FlowDepth: r.run.FlowDepth + 1, InitialSteps: child.InitialSteps,
		})
		if err != nil {
			return err
		}
		if isNew {
			if err := cr.save(ctx); err != nil {
				return err
			}
		}
		if _, err := r.tx.Exec(ctx, `INSERT INTO flow_node_runs
			(id, tenant_id, flow_run_id, node_id, node_type, child_run_id, status, attempt, generation)
			VALUES ($1,$2,$3,$4,$5,$6,'RUNNING',$7,$8)
			ON CONFLICT (tenant_id, flow_run_id, generation, node_id, attempt) DO UPDATE SET
			  child_run_id=EXCLUDED.child_run_id, status='RUNNING', updated_at=now()`,
			newID(), r.run.TenantID, actualID, child.NodeID, child.NodeType, cr.run.ID, attempt, fc.Generation); err != nil {
			return err
		}
	}
	return nil
}

// flowChildFinished records a child result and wakes the coordinator. The
// parent has no lease, so this is a plain row update plus an outbox message.
func (s *Store) flowChildFinished(ctx context.Context, r *runTx, lease domain.Lease, status, resultRef, errMsg string) error {
	if lease.ParentRunID == "" {
		return nil
	}
	if _, err := r.tx.Exec(ctx, `UPDATE flow_node_runs SET status=$3, output_ref=$4, error=$5, updated_at=now()
		WHERE tenant_id=$1 AND child_run_id=$2`, r.run.TenantID, lease.RunID, status,
		nullable(resultRef), nullable(errMsg)); err != nil {
		return err
	}
	var parentID string
	err := r.tx.QueryRow(ctx, `UPDATE agent_runs SET status='QUEUED', not_before=now(), version=version+1, updated_at=now()
		WHERE tenant_id=$1 AND id=$2 AND status='WAITING_CHILDREN' AND desired_state='RUNNING'
		RETURNING id`, r.run.TenantID, lease.ParentRunID).Scan(&parentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // paused or cancelled: the command path settles it
	}
	if err != nil {
		return err
	}
	_, err = r.tx.Exec(ctx, `INSERT INTO outbox_messages (id, tenant_id, aggregate_id, subject, payload, next_retry_at)
		VALUES ($1,$2,$3,$4,$5,now())`, newID(), r.run.TenantID, parentID, WakeSubject,
		jsonb(map[string]string{"run_id": parentID, "tenant_id": r.run.TenantID}))
	return err
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}

func (s *Store) finishAttempt(ctx context.Context, r *runTx, lease domain.Lease, st domain.AttemptStatus, code, msg string) error {
	tag, err := r.tx.Exec(ctx, `UPDATE step_attempts SET status=$2, ended_at=now(), error_code=$3, error_message=$4
		WHERE id=$1 AND status='RUNNING' AND fence_token=$5`, lease.AttemptID, st, nullable(code), nullable(msg), lease.FenceToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrLeaseLost
	}
	return r.event(ctx, "attempt."+lowerStatus(st), "worker", lease.WorkerID, &lease.StepID, &lease.AttemptID,
		map[string]any{"attempt_no": lease.AttemptNo, "error_code": code})
}

func lowerStatus(st domain.AttemptStatus) string {
	switch st {
	case domain.AttemptSucceeded:
		return "succeeded"
	case domain.AttemptFailed:
		return "failed"
	case domain.AttemptTimedOut:
		return "timed_out"
	case domain.AttemptCancelled:
		return "cancelled"
	case domain.AttemptUnknown:
		return "unknown"
	}
	return "lost"
}

// CommitFailure records a failed attempt with the correct recovery semantics.
func (s *Store) CommitFailure(ctx context.Context, lease domain.Lease, f StepFailure) error {
	return s.inKernelTx(ctx, func(tx pgx.Tx) error {
		r, err := s.lockLeased(ctx, tx, lease)
		if err != nil {
			return err
		}
		// Locals only: inKernelTx may re-run this function.
		class, msg := f.Class, f.Message
		if class == FailRetryable && lease.AttemptNo >= s.cfg.MaxAttempts {
			class = FailPermanent
			msg = "attempts exhausted: " + msg
		}
		attemptStatus := domain.AttemptFailed
		switch {
		case class == FailCancelled:
			attemptStatus = domain.AttemptCancelled
		case f.TimedOut:
			attemptStatus = domain.AttemptTimedOut
		case class == FailUnknownEffect:
			attemptStatus = domain.AttemptUnknown
		}
		if err := s.finishAttempt(ctx, r, lease, attemptStatus, f.Code, msg); err != nil {
			return err
		}
		r.releaseLease()
		effectState := ""
		switch class {
		case FailRetryable:
			delay := f.RetryAfter
			if delay <= 0 {
				delay = s.cfg.DefaultRetryDelay * time.Duration(1<<(lease.AttemptNo-1))
			}
			at := r.now.Add(delay)
			if _, err := tx.Exec(ctx, `UPDATE agent_steps SET status='RETRY_WAIT', next_attempt_at=$2, updated_at=now()
				WHERE id=$1`, lease.StepID, at); err != nil {
				return err
			}
			effectState = "PREPARED" // the tool asserted nothing was dispatched
			r.run.NotBefore = &at
			r.setStatus(domain.RunQueued)
			if err := r.event(ctx, "step.retry_scheduled", "worker", lease.WorkerID, &lease.StepID, &lease.AttemptID,
				map[string]any{"not_before": at, "error_code": f.Code}); err != nil {
				return err
			}
			if err := r.settle(ctx); err != nil {
				return err
			}
		case FailPermanent:
			if _, err := tx.Exec(ctx, `UPDATE agent_steps SET status='FAILED', updated_at=now() WHERE id=$1`, lease.StepID); err != nil {
				return err
			}
			if err := r.event(ctx, "step.failed", "worker", lease.WorkerID, &lease.StepID, &lease.AttemptID,
				map[string]string{"error_code": f.Code, "message": msg}); err != nil {
				return err
			}
			if err := r.fail(ctx, f.Code+": "+msg); err != nil {
				return err
			}
		case FailUnknownEffect:
			if _, err := tx.Exec(ctx, `UPDATE agent_steps SET status='UNKNOWN', updated_at=now() WHERE id=$1`, lease.StepID); err != nil {
				return err
			}
			effectState = "UNKNOWN"
			r.setStatus(domain.RunNeedsHuman)
			if err := r.event(ctx, "step.unknown", "worker", lease.WorkerID, &lease.StepID, &lease.AttemptID,
				map[string]string{"error_code": f.Code, "message": msg}); err != nil {
				return err
			}
		case FailCancelled:
			if _, err := tx.Exec(ctx, `UPDATE agent_steps SET status='CANCELLED', updated_at=now() WHERE id=$1`, lease.StepID); err != nil {
				return err
			}
			if err := r.cancel(ctx, "worker", lease.WorkerID, "cancelled during execution"); err != nil {
				return err
			}
		}
		if effectState != "" {
			if _, err := tx.Exec(ctx, `UPDATE external_tool_effects SET state=$3, last_check_at=now()
				WHERE tenant_id=$1 AND idempotency_key=$2 AND state <> 'CONFIRMED'`,
				lease.TenantID, lease.IdempotencyKey, effectState); err != nil {
				return err
			}
		}
		if lease.Kind == domain.StepFlow {
			switch class {
			case FailPermanent, FailUnknownEffect:
				if _, err := tx.Exec(ctx, `UPDATE flow_runs SET status='FAILED', updated_at=now()
					WHERE tenant_id=$1 AND run_id=$2`, lease.TenantID, lease.RunID); err != nil {
					return err
				}
			case FailCancelled:
				if _, err := tx.Exec(ctx, `UPDATE flow_runs SET status='CANCELLED', updated_at=now()
					WHERE tenant_id=$1 AND run_id=$2`, lease.TenantID, lease.RunID); err != nil {
					return err
				}
			}
		}
		if lease.ParentRunID != "" && class == FailUnknownEffect {
			if err := s.flowChildFinished(ctx, r, lease, "FAILED", "", msg); err != nil {
				return err
			}
		}
		return r.save(ctx)
	})
}

// PrepareEffect registers a side-effecting call in the effect ledger before
// dispatch (§14.3). If a previous attempt may already have dispatched it,
// ErrUnknownEffect is returned and the tool must not be called.
func (s *Store) PrepareEffect(ctx context.Context, lease domain.Lease, target string) (EffectState, string, error) {
	var state EffectState
	var resultRef *string
	err := s.inKernelTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.lockLeased(ctx, tx, lease); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO external_tool_effects (id, tenant_id, run_id, step_id, idempotency_key,
			target, state) VALUES ($1,$2,$3,$4,$5,$6,'PREPARED') ON CONFLICT (tenant_id, idempotency_key, target) DO NOTHING`,
			newID(), lease.TenantID, lease.RunID, lease.StepID, lease.IdempotencyKey, target); err != nil {
			return err
		}
		var cur string
		if err := tx.QueryRow(ctx, `SELECT state, result_ref FROM external_tool_effects
			WHERE tenant_id=$1 AND idempotency_key=$2 AND target=$3 FOR UPDATE`,
			lease.TenantID, lease.IdempotencyKey, target).Scan(&cur, &resultRef); err != nil {
			return err
		}
		switch cur {
		case "PREPARED":
			state = EffectDispatched
			_, err := tx.Exec(ctx, `UPDATE external_tool_effects SET state='DISPATCHED', last_check_at=now()
				WHERE tenant_id=$1 AND idempotency_key=$2 AND target=$3`, lease.TenantID, lease.IdempotencyKey, target)
			return err
		case "CONFIRMED":
			state = EffectConfirmed
			return nil
		default:
			return domain.ErrUnknownEffect
		}
	})
	ref := ""
	if resultRef != nil {
		ref = *resultRef
	}
	return state, ref, err
}

// RecordEffectResult confirms a dispatched side effect as soon as the tool
// returned and its observation is stored, ahead of the step commit. If that
// commit is then lost (database failover, worker crash), recovery finds the
// effect CONFIRMED and the retried step reuses resultRef instead of parking
// the run for a human. Fenced like every other worker write.
func (s *Store) RecordEffectResult(ctx context.Context, lease domain.Lease, target, resultRef string) error {
	return s.inKernelTx(ctx, func(tx pgx.Tx) error {
		r, err := s.lockLeased(ctx, tx, lease)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE external_tool_effects SET state='CONFIRMED', result_ref=$4, last_check_at=now()
			WHERE tenant_id=$1 AND idempotency_key=$2 AND target=$3 AND state='DISPATCHED'`,
			lease.TenantID, lease.IdempotencyKey, target, resultRef)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("effect %s is not awaiting a result", lease.IdempotencyKey)
		}
		if err := r.event(ctx, "effect.confirmed", "worker", lease.WorkerID, &lease.StepID, &lease.AttemptID,
			map[string]string{"target": target, "result_ref": resultRef}); err != nil {
			return err
		}
		return r.save(ctx)
	})
}

// AttemptStatus reports the status of the lease's attempt, provided it still
// carries the lease's fence. A worker whose commit failed ambiguously uses it
// to learn whether that commit landed: only the fence holder can move its
// attempt out of RUNNING other than to LOST.
func (s *Store) AttemptStatus(ctx context.Context, lease domain.Lease) (domain.AttemptStatus, error) {
	var st domain.AttemptStatus
	err := s.pool.QueryRow(ctx, `SELECT status FROM step_attempts WHERE id=$1 AND tenant_id=$2 AND fence_token=$3`,
		lease.AttemptID, lease.TenantID, lease.FenceToken).Scan(&st)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", domain.ErrNotFound
	}
	return st, err
}

// checkpoint records a resumable boundary after a committed step (§29.4).
// The manifest artifact was uploaded by the worker before this transaction.
func (r *runTx) checkpoint(ctx context.Context, lease domain.Lease, res StepSuccess) error {
	var cursor *int64
	if r.run.TurnID != nil {
		_ = r.tx.QueryRow(ctx, `SELECT input_message_cursor FROM conversation_turns WHERE id=$1`, *r.run.TurnID).Scan(&cursor)
	}
	id := newID()
	tag, err := r.tx.Exec(ctx, `INSERT INTO run_checkpoints (id, tenant_id, run_id, last_committed_step_seq, last_event_seq,
		profile_id, profile_version, context_snapshot_id, message_cursor, compaction_epoch, manifest_ref, manifest_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (run_id, last_committed_step_seq) DO NOTHING`,
		id, r.run.TenantID, r.run.ID, lease.StepSeq, r.run.LastEventSeq+1, r.run.ProfileID, r.run.ProfileVersion,
		nullable(lease.ContextSnapshotID), cursor, r.run.CompactionEpoch, res.CheckpointRef, res.CheckpointHash)
	if err != nil || tag.RowsAffected() == 0 {
		return err
	}
	return r.event(ctx, "checkpoint.created", "worker", lease.WorkerID, &lease.StepID, nil,
		map[string]any{"checkpoint_id": id, "step_seq": lease.StepSeq})
}
