package localstore

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/store/pg"
)

// checkpointRec is the local projection of run_checkpoints. Checkpoints are
// immutable once written; ForkRun reads them and never rewrites the source run.
type checkpointRec = pg.Checkpoint

type compactRec struct {
	StepID     string            `json:"step_id"`
	RunID      string            `json:"run_id"`
	Seq        int64             `json:"seq"`
	Compaction domain.Compaction `json:"compaction"`
	CreatedAt  time.Time         `json:"created_at"`
}

type memoryHitRec struct {
	MemoryID string  `json:"memory_id"`
	Version  int64   `json:"version"`
	Score    float64 `json:"score"`
	Rank     int     `json:"rank"`
	StepSeq  int64   `json:"step_seq"`
}

func (s *Store) saveCheckpoint(cp checkpointRec) {
	s.checkpoints[cp.ID] = cp
	s.put("checkpoint", cp.ID, cp.RunID, cp.LastCommittedStepSeq, cp)
}

// createCheckpointLocked records a resumable boundary after a committed step.
// The manifest was uploaded by the worker before this commit.
func (s *Store) createCheckpointLocked(r domain.Run, lease domain.Lease, res domain.StepSuccess) {
	for _, cp := range s.checkpoints {
		if cp.RunID == r.ID && cp.LastCommittedStepSeq == lease.StepSeq {
			return // unique(run_id, last_committed_step_seq)
		}
	}
	var cursor *int64
	if r.TurnID != nil {
		if turn, ok := s.turns[*r.TurnID]; ok && turn.InputMessageCursor != nil {
			v := *turn.InputMessageCursor
			cursor = &v
		}
	}
	lastEventSeq := s.runs[r.ID].LastEventSeq
	if s.buf != nil {
		// Kernel transactions defer event emission until object commit. The
		// checkpoint still pins the logical boundary after this Step's events.
		for _, event := range s.buf.events {
			if event.runID == r.ID {
				lastEventSeq++
			}
		}
	}
	cp := checkpointRec{ID: newID(), RunID: r.ID, LastCommittedStepSeq: lease.StepSeq,
		LastEventSeq: lastEventSeq, ProfileID: r.ProfileID, ProfileVersion: r.ProfileVersion,
		CompactionEpoch: r.CompactionEpoch, ManifestRef: res.CheckpointRef, ManifestHash: res.CheckpointHash,
		Replayable: true, CreatedAt: now()}
	if lease.ContextSnapshotID != "" {
		id := lease.ContextSnapshotID
		cp.ContextSnapshotID = &id
	}
	cp.MessageCursor = cursor
	s.saveCheckpoint(cp)
	s.appendEvent(r.TenantID, r.ID, lease.StepID, "", "checkpoint.created", "worker", lease.WorkerID,
		map[string]any{"checkpoint_id": cp.ID, "step_seq": lease.StepSeq})
}

func (s *Store) ListCheckpoints(ctx context.Context, tenantID, runID string) ([]pg.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok || (tenantID != "" && r.TenantID != tenantID) {
		return nil, domain.ErrNotFound
	}
	var out []pg.Checkpoint
	for _, cp := range s.checkpoints {
		if cp.RunID == runID {
			out = append(out, cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastCommittedStepSeq < out[j].LastCommittedStepSeq })
	return out, nil
}

func (s *Store) latestCheckpointLocked(tenantID, runID string) (pg.Checkpoint, bool) {
	var best pg.Checkpoint
	found := false
	for _, cp := range s.checkpoints {
		if cp.RunID != runID {
			continue
		}
		r, ok := s.runs[runID]
		if !ok || r.TenantID != tenantID {
			continue
		}
		if !found || cp.LastCommittedStepSeq > best.LastCommittedStepSeq {
			best, found = cp, true
		}
	}
	return best, found
}

// ForkRun starts a new run that continues from a committed checkpoint. Flow
// forks are rejected: copying a compiled graph and its node attempts is not
// the same as replaying the step history.
func (s *Store) ForkRun(ctx context.Context, req pg.ForkRunRequest) (domain.Run, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var run domain.Run
	var created bool
	err := s.withStateTxLocked(ctx, func() error {
		var err error
		run, created, err = s.forkRunLocked(ctx, req)
		return err
	})
	if err != nil {
		return domain.Run{}, false, err
	}
	return s.runs[run.ID], created, nil
}

func (s *Store) forkRunLocked(ctx context.Context, req pg.ForkRunRequest) (domain.Run, bool, error) {
	src, ok := s.runs[req.SourceRunID]
	if !ok || src.TenantID != req.TenantID {
		return domain.Run{}, false, domain.ErrNotFound
	}
	if _, ok := s.flows[src.ID]; ok {
		return domain.Run{}, false, &domain.RejectedError{Reason: "flow_fork_not_supported"}
	}
	var cp pg.Checkpoint
	if req.CheckpointID == "" {
		var found bool
		cp, found = s.latestCheckpointLocked(req.TenantID, src.ID)
		if !found {
			return domain.Run{}, false, domain.ErrNotFound
		}
	} else {
		cp, ok = s.checkpoints[req.CheckpointID]
		if !ok || cp.RunID != src.ID {
			return domain.Run{}, false, domain.ErrNotFound
		}
	}
	if !cp.Replayable {
		return domain.Run{}, false, &domain.RejectedError{Reason: "checkpoint_not_forkable"}
	}
	var goalHash string
	for _, st := range sortedSteps(s.steps, src.ID) {
		if st.InputRef == src.GoalRef {
			goalHash = st.InputHash
			break
		}
	}
	version := req.ProfileVersion
	if version == 0 {
		version = cp.ProfileVersion
	}
	run, created, err := s.createRunLocked(ctx, domain.CreateRunRequest{
		TenantID: req.TenantID, IncidentID: src.IncidentID, ProfileID: src.ProfileID, ProfileVersion: version,
		GoalRef: src.GoalRef, GoalHash: goalHash, IdempotencyKey: req.IdempotencyKey, InitiatorType: "user",
		InitiatorID: req.ActorID, ForkedFrom: &cp.ID, StepSeqStart: cp.LastCommittedStepSeq + 1})
	if err != nil || !created {
		return run, created, err
	}
	s.appendEvent(run.TenantID, run.ID, "", "", "run.forked", "user", req.ActorID, map[string]any{
		"source_run_id": src.ID, "checkpoint_id": cp.ID, "step_seq": cp.LastCommittedStepSeq})
	return s.runs[run.ID], true, nil
}

func (s *Store) StepOutputRevisions(ctx context.Context, tenantID, stepID string) ([]pg.OutputRevision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.steps[stepID]
	if !ok || (tenantID != "" && st.TenantID != tenantID) {
		return nil, domain.ErrNotFound
	}
	out := append([]pg.OutputRevision(nil), s.outputRevs[stepID]...)
	sort.Slice(out, func(i, j int) bool { return out[i].Revision < out[j].Revision })
	return out, nil
}

// SnapshotContext returns compaction and pinned memory that is still valid.
func (s *Store) SnapshotContext(ctx context.Context, tenantID, runID, snapshotID string, beforeSeq int64) (kernel.SnapshotContext, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok || tenantID == "" || r.TenantID != tenantID {
		return kernel.SnapshotContext{}, domain.ErrNotFound
	}
	out := kernel.SnapshotContext{CompactionEpoch: r.CompactionEpoch}
	var latest compactRec
	found := false
	for _, c := range s.compactions {
		if c.RunID != runID || (beforeSeq > 0 && c.Seq >= beforeSeq) {
			continue
		}
		if !found || c.Seq > latest.Seq {
			latest, found = c, true
		}
	}
	if found {
		var doc struct {
			Text string `json:"text"`
		}
		_ = json.Unmarshal(latest.Compaction.Summary, &doc)
		out.Summary, out.CompactedBefore = doc.Text, latest.Compaction.CompactedBefore
	}
	// The latest successful retrieve_memory step before this snapshot pins the
	// memories the following reason steps see.
	var hits []memoryHitRec
	for _, c := range sortedSteps(s.steps, runID) {
		if c.Kind == domain.StepRetrieve && c.Status == domain.StepSucceeded &&
			(beforeSeq == 0 || c.Seq < beforeSeq) {
			hits = s.memHits[c.ID]
		}
	}
	env, _ := memory.EnvFrom(ctx)
	at := time.Now().UTC()
	for _, h := range hits {
		m, ok := s.memories[h.MemoryID]
		if !ok || m.Mem.Version != h.Version {
			continue
		}
		if m.Mem.Scope == "user" {
			if r.InitiatorType != "user" || r.InitiatorID == nil || m.Mem.ScopeID != *r.InitiatorID {
				continue
			}
		} else if m.Mem.Sensitivity != "public" && m.Mem.Sensitivity != "internal" {
			continue
		}
		if valid, _ := m.Mem.Recallable(tenantID, env, at); !valid {
			continue
		}
		out.Memories = append(out.Memories, cognitive.MemoryRef{ID: h.MemoryID, Version: h.Version,
			Kind: m.Mem.Type, Title: m.Mem.Title, Content: m.Mem.Content, Trust: m.Mem.Trust, Score: h.Score,
			Scope: m.Mem.Scope})
	}
	return out, nil
}
