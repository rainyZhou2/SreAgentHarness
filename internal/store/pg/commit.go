package pg

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
)

// CommitSuccess is the fenced result commit (§4.4, §15.2): attempt/step result,
// cognitive artifact, run-private KV, effect confirmation, events, next steps,
// lease release and wake-up — one transaction. A stale worker gets ErrLeaseLost.
func (s *Store) CommitSuccess(ctx context.Context, lease domain.Lease, res StepSuccess) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		r, err := s.lockLeased(ctx, tx, lease)
		if err != nil {
			return err
		}
		if err := s.finishAttempt(ctx, r, lease, domain.AttemptSucceeded, "", ""); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE agent_steps SET status='SUCCEEDED', output_ref=$2, output_hash=$3,
			updated_at=now() WHERE id=$1`, lease.StepID, res.OutputRef, res.OutputHash); err != nil {
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
		} else if err := r.appendPlan(ctx, res.Plan); err != nil {
			return err
		}
		return r.save(ctx)
	})
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
	return s.inTx(ctx, func(tx pgx.Tx) error {
		r, err := s.lockLeased(ctx, tx, lease)
		if err != nil {
			return err
		}
		class := f.Class
		if class == FailRetryable && lease.AttemptNo >= s.cfg.MaxAttempts {
			class = FailPermanent
			f.Message = "attempts exhausted: " + f.Message
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
		if err := s.finishAttempt(ctx, r, lease, attemptStatus, f.Code, f.Message); err != nil {
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
				map[string]string{"error_code": f.Code, "message": f.Message}); err != nil {
				return err
			}
			if err := r.fail(ctx, f.Code+": "+f.Message); err != nil {
				return err
			}
		case FailUnknownEffect:
			if _, err := tx.Exec(ctx, `UPDATE agent_steps SET status='UNKNOWN', updated_at=now() WHERE id=$1`, lease.StepID); err != nil {
				return err
			}
			effectState = "UNKNOWN"
			r.setStatus(domain.RunNeedsHuman)
			if err := r.event(ctx, "step.unknown", "worker", lease.WorkerID, &lease.StepID, &lease.AttemptID,
				map[string]string{"error_code": f.Code, "message": f.Message}); err != nil {
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
		return r.save(ctx)
	})
}

// PrepareEffect registers a side-effecting call in the effect ledger before
// dispatch (§14.3). If a previous attempt may already have dispatched it,
// ErrUnknownEffect is returned and the tool must not be called.
func (s *Store) PrepareEffect(ctx context.Context, lease domain.Lease, target string) (EffectState, string, error) {
	var state EffectState
	var resultRef *string
	err := s.inTx(ctx, func(tx pgx.Tx) error {
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
