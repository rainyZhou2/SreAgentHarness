package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
)

type Checkpoint struct {
	ID                   string    `json:"id"`
	RunID                string    `json:"run_id"`
	LastCommittedStepSeq int64     `json:"last_committed_step_seq"`
	LastEventSeq         int64     `json:"last_event_seq"`
	ProfileID            string    `json:"profile_id"`
	ProfileVersion       int64     `json:"profile_version"`
	ContextSnapshotID    *string   `json:"context_snapshot_id,omitempty"`
	MessageCursor        *int64    `json:"message_cursor,omitempty"`
	CompactionEpoch      int64     `json:"compaction_epoch"`
	ManifestRef          string    `json:"manifest_ref"`
	ManifestHash         string    `json:"manifest_hash"`
	Replayable           bool      `json:"replayable"`
	CreatedAt            time.Time `json:"created_at"`
}

const checkpointCols = `id, run_id, last_committed_step_seq, last_event_seq, profile_id, profile_version,
	context_snapshot_id, message_cursor, compaction_epoch, manifest_ref, manifest_hash, replayable, created_at`

func scanCheckpoint(row pgx.Row) (Checkpoint, error) {
	var c Checkpoint
	err := row.Scan(&c.ID, &c.RunID, &c.LastCommittedStepSeq, &c.LastEventSeq, &c.ProfileID, &c.ProfileVersion,
		&c.ContextSnapshotID, &c.MessageCursor, &c.CompactionEpoch, &c.ManifestRef, &c.ManifestHash, &c.Replayable, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, domain.ErrNotFound
	}
	return c, err
}

func (s *Store) ListCheckpoints(ctx context.Context, tenantID, runID string) ([]Checkpoint, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+checkpointCols+` FROM run_checkpoints WHERE tenant_id=$1 AND run_id=$2
		ORDER BY last_committed_step_seq`, tenantID, runID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Checkpoint, error) { return scanCheckpoint(row) })
}

func (s *Store) GetCheckpoint(ctx context.Context, tenantID, id string) (Checkpoint, error) {
	if !validID(id) {
		return Checkpoint{}, domain.ErrNotFound
	}
	return scanCheckpoint(s.pool.QueryRow(ctx, `SELECT `+checkpointCols+` FROM run_checkpoints WHERE tenant_id=$1 AND id=$2`,
		tenantID, id))
}

// ---------- fork (§29.4) ----------

type ForkRunRequest struct {
	TenantID       string
	SourceRunID    string
	CheckpointID   string // empty = latest checkpoint of the source run
	IdempotencyKey string
	ActorID        string
	ProfileVersion int64 // 0 = checkpoint's version
}

// ForkRun creates a new run that continues from a committed checkpoint of
// another run. The source run is not affected. The new run's history is the
// source's committed steps up to the checkpoint plus its own steps.
func (s *Store) ForkRun(ctx context.Context, req ForkRunRequest) (domain.Run, bool, error) {
	src, err := s.GetRun(ctx, req.TenantID, req.SourceRunID)
	if err != nil {
		return domain.Run{}, false, err
	}
	var cp Checkpoint
	if req.CheckpointID == "" {
		cp, err = scanCheckpoint(s.pool.QueryRow(ctx, `SELECT `+checkpointCols+` FROM run_checkpoints
			WHERE tenant_id=$1 AND run_id=$2 ORDER BY last_committed_step_seq DESC LIMIT 1`, req.TenantID, src.ID))
	} else {
		cp, err = s.GetCheckpoint(ctx, req.TenantID, req.CheckpointID)
	}
	if err != nil {
		return domain.Run{}, false, err
	}
	if cp.RunID != src.ID || !cp.Replayable {
		return domain.Run{}, false, &domain.RejectedError{Reason: "checkpoint_not_forkable"}
	}
	var goalHash string
	if err := s.pool.QueryRow(ctx, `SELECT input_hash FROM agent_steps WHERE run_id=$1 ORDER BY seq LIMIT 1`, src.ID).
		Scan(&goalHash); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return domain.Run{}, false, err
	}
	if goalHash == "" {
		goalHash = hashStrings(src.GoalRef)
	}
	version := req.ProfileVersion
	if version == 0 {
		version = cp.ProfileVersion
	}
	var out domain.Run
	var created bool
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		r, c, err := s.createRunTx(ctx, tx, CreateRunRequest{TenantID: req.TenantID, IncidentID: src.IncidentID,
			ProfileID: src.ProfileID, ProfileVersion: version, GoalRef: src.GoalRef, GoalHash: goalHash,
			IdempotencyKey: req.IdempotencyKey, InitiatorType: "user", InitiatorID: req.ActorID, ForkedFrom: &cp.ID,
			StepSeqStart: cp.LastCommittedStepSeq + 1})
		if err != nil {
			return err
		}
		created = c
		out = r.run
		if !c {
			return nil
		}
		if err := r.event(ctx, "run.forked", "user", req.ActorID, nil, nil, map[string]any{"source_run_id": src.ID,
			"checkpoint_id": cp.ID, "step_seq": cp.LastCommittedStepSeq}); err != nil {
			return err
		}
		if err := r.save(ctx); err != nil {
			return err
		}
		out = r.run
		return nil
	})
	return out, created, err
}

// History returns the committed steps before seq; they are immutable, so the
// result is the same no matter when a retry reads it. A forked run sees its
// ancestors' steps up to the fork checkpoint first.
func (s *Store) History(ctx context.Context, tenantID, runID string, beforeSeq int64) ([]domain.Step, error) {
	var forked *string
	if err := s.pool.QueryRow(ctx, `SELECT forked_from_checkpoint_id FROM agent_runs WHERE id=$1 AND tenant_id=$2`,
		runID, tenantID).Scan(&forked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	var prefix []domain.Step
	if forked != nil {
		cp, err := s.GetCheckpoint(ctx, tenantID, *forked)
		if err != nil {
			return nil, err
		}
		prefix, err = s.History(ctx, tenantID, cp.RunID, cp.LastCommittedStepSeq+1)
		if err != nil {
			return nil, err
		}
	}
	own, err := s.listSteps(ctx, s.pool, tenantID, runID, beforeSeq)
	if err != nil {
		return nil, err
	}
	return append(prefix, own...), nil
}

// ---------- snapshot-pinned context extras ----------

var _ kernel.ContextExtras = (*Store)(nil)

// SnapshotContext returns the memory hits pinned for this snapshot and the
// latest compaction summary committed before the step (§11.4, §33.4).
func (s *Store) SnapshotContext(ctx context.Context, tenantID, runID, snapshotID string, beforeSeq int64) (kernel.SnapshotContext, error) {
	var out kernel.SnapshotContext
	var summary []byte
	var epoch int64
	err := s.pool.QueryRow(ctx, `SELECT st.summary, r.compaction_epoch FROM agent_runs r
		LEFT JOIN LATERAL (SELECT summary FROM agent_steps WHERE run_id=r.id AND kind='compact' AND status='SUCCEEDED'
			AND seq < $3 AND summary IS NOT NULL ORDER BY seq DESC LIMIT 1) st ON true
		WHERE r.id=$1 AND r.tenant_id=$2`, runID, tenantID, beforeSeq).Scan(&summary, &epoch)
	if err != nil {
		return out, err
	}
	out.CompactionEpoch = epoch
	if len(summary) > 0 {
		var c domain.Compaction
		if err := json.Unmarshal(summary, &c); err != nil {
			return out, fmt.Errorf("compaction summary: %w", err)
		}
		var doc struct {
			Text string `json:"text"`
		}
		_ = json.Unmarshal(c.Summary, &doc)
		out.Summary, out.CompactedBefore = doc.Text, c.CompactedBefore
	}
	mems, err := s.pinnedMemories(ctx, tenantID, runID, snapshotID, beforeSeq)
	if err != nil {
		return out, err
	}
	out.Memories = mems
	return out, nil
}
