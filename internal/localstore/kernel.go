package localstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"go-sre-harness/internal/domain"
)

// ClaimStep is the local equivalent of the PostgreSQL claim transaction. The
// store mutex is the row lock; there is one process, so leases only guard
// against duplicate workers inside it and against stale attempts after a
// restart.
func (s *Store) ClaimStep(ctx context.Context, runID, workerID string, ttl time.Duration) (domain.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return domain.Lease{}, domain.ErrNotFound
	}
	if r.Status == domain.RunRunning && r.LeaseUntil != nil && r.LeaseUntil.After(now()) {
		return domain.Lease{}, domain.ErrLeaseHeld
	}
	if r.Status == domain.RunRunning {
		s.recoverExpiredLocked(&r)
		r = s.runs[runID]
	}
	if r.Status != domain.RunQueued {
		return domain.Lease{}, domain.ErrNoRunnableStep
	}
	if r.DesiredState == domain.DesiredPaused {
		r.Status = domain.RunPaused
		r.Version++
		s.saveRun(r)
		return domain.Lease{}, domain.ErrNoRunnableStep
	}
	if r.DesiredState == domain.DesiredCancelled {
		r.Status = domain.RunCancelled
		r.Version++
		s.saveRun(r)
		return domain.Lease{}, domain.ErrNoRunnableStep
	}
	if r.NotBefore != nil && r.NotBefore.After(now()) {
		return domain.Lease{}, domain.ErrNoRunnableStep
	}

	steps := sortedSteps(s.steps, runID)
	var target *domain.Step
	for i := range steps {
		st := &steps[i]
		switch st.Status {
		case domain.StepPending:
			target = st
		case domain.StepRetryWait:
			if st.NextAttemptAt == nil || !st.NextAttemptAt.After(now()) {
				target = st
			}
		case domain.StepWaitingChildren:
			if s.flowChildrenReadyLocked(runID) {
				st.Status = domain.StepPending
				s.saveStep(*st)
				target = st
			}
		case domain.StepWaitingApproval:
			r.Status = domain.RunWaitingApproval
			s.saveRun(r)
			return domain.Lease{}, domain.ErrNoRunnableStep
		}
		if target != nil {
			break
		}
	}
	if target == nil {
		return domain.Lease{}, domain.ErrNoRunnableStep
	}

	var intent *domain.ActionIntent
	if target.Kind == domain.StepTool || target.Kind == domain.StepFlow || target.Kind == domain.StepPlanReview {
		if id := s.intentStep[target.ID]; id != "" {
			in := s.intents[id]
			intent = &in
		}
		if target.Kind == domain.StepTool && intent == nil {
			return domain.Lease{}, fmt.Errorf("tool step without action intent")
		}
		if target.Kind == domain.StepPlanReview && intent == nil {
			return domain.Lease{}, fmt.Errorf("plan review step without action intent")
		}
	}
	if intent != nil {
		ok, err := s.consumeGrantLocked(*target, *intent)
		if err != nil {
			return domain.Lease{}, err
		}
		if !ok {
			target.Status = domain.StepWaitingApproval
			s.saveStep(*target)
			r.Status = domain.RunWaitingApproval
			s.saveRun(r)
			return domain.Lease{}, domain.ErrNoRunnableStep
		}
	}

	attemptID := newID()
	snapshotID := newID()
	s.snapshots[snapshotID] = r.ID
	r.FenceToken++
	until := now().Add(ttl)
	r.LeaseOwner, r.LeaseUntil, r.NotBefore = &workerID, &until, nil
	r.Status = domain.RunRunning
	r.Version++
	readyAt := domain.StepReadyAt(*target)
	target.Status = domain.StepRunning
	target.CurrentAttempt++
	target.UpdatedAt = now()
	att := attempt{ID: attemptID, RunID: r.ID, StepID: target.ID, AttemptNo: target.CurrentAttempt,
		WorkerID: workerID, FenceToken: r.FenceToken, Status: "RUNNING", IdempotencyKey: target.InputHash,
		SnapshotID: snapshotID, StartedAt: now()}
	s.saveRun(r)
	s.saveStep(*target)
	s.attempts[attemptID] = att
	s.put("attempt", attemptID, r.ID, int64(target.CurrentAttempt), att)
	s.appendEvent(r.TenantID, r.ID, target.ID, attemptID, "step.started", "worker", workerID, map[string]any{
		"seq": target.Seq, "kind": target.Kind})
	return domain.Lease{TenantID: r.TenantID, RunID: r.ID, StepID: target.ID, StepSeq: target.Seq,
		AttemptID: attemptID, AttemptNo: target.CurrentAttempt, WorkerID: workerID, FenceToken: r.FenceToken,
		Kind: target.Kind, ExecutorName: target.ExecutorName, FlowNodeID: target.FlowNodeID,
		InputRef: target.InputRef, InputHash: target.InputHash, GoalRef: r.GoalRef,
		ContextSnapshotID: snapshotID, IdempotencyKey: att.IdempotencyKey, Deadline: until,
		Intent: intent, ProfileID: r.ProfileID, ProfileVersion: r.ProfileVersion,
		ParentRunID: deref(r.ParentRunID), ParentStepID: deref(r.ParentStepID), FlowDepth: r.FlowDepth,
		ReadyAt: readyAt}, nil
}

func (s *Store) recoverExpiredLocked(r *domain.Run) {
	var lost *attempt
	for id, a := range s.attempts {
		if a.RunID == r.ID && a.Status == "RUNNING" {
			a.Status = "LOST"
			end := now()
			a.EndedAt = &end
			a.ErrorCode = "LEASE_EXPIRED"
			s.attempts[id] = a
			s.put("attempt", id, r.ID, int64(a.AttemptNo), a)
			if a.SnapshotID != "" {
				delete(s.snapshots, a.SnapshotID)
			}
			cp := a
			lost = &cp
		}
	}
	r.LeaseOwner, r.LeaseUntil = nil, nil
	if lost == nil {
		r.Status = domain.RunQueued
		r.Version++
		s.saveRun(*r)
		s.appendEvent(r.TenantID, r.ID, "", "", "lease.expired", "system", "", nil)
		return
	}
	s.appendEvent(r.TenantID, r.ID, lost.StepID, lost.ID, "attempt.lost", "system", "",
		map[string]any{"attempt_no": lost.AttemptNo})
	st := s.steps[lost.StepID]
	// A worker that vanished after dispatching a side-effecting call may or
	// may not have applied it. Never retry silently: park the run for a human
	// resolution.
	if st.Kind == domain.StepTool {
		e, ok := s.effects[lost.IdempotencyKey]
		if ok && (e.State == "DISPATCHED" || e.State == "UNKNOWN") {
			e.State = "UNKNOWN"
			e.LastCheckAt = now()
			s.saveEffect(e)
			st.Status = domain.StepUnknown
			st.StatusReason = strptr("worker lost after dispatching a side-effecting tool")
			s.saveStep(st)
			r.Status = domain.RunNeedsHuman
			r.Version++
			s.saveRun(*r)
			s.appendEvent(r.TenantID, r.ID, st.ID, lost.ID, "step.unknown", "system", "",
				map[string]string{"reason": "worker lost after dispatching a side-effecting tool"})
			return
		}
	}
	if lost.AttemptNo >= 3 {
		st.Status = domain.StepFailed
		s.saveStep(st)
		s.failRunLocked(r, "attempts exhausted after lost leases")
		return
	}
	st.Status = domain.StepPending
	s.saveStep(st)
	r.Status = domain.RunQueued
	r.Version++
	s.saveRun(*r)
}

func (s *Store) consumeGrantLocked(st domain.Step, in domain.ActionIntent) (bool, error) {
	var reqID string
	for _, a := range s.approvals {
		if a.IntentID == in.ID {
			reqID = a.Request.ID
			break
		}
	}
	if reqID == "" {
		return true, nil
	} // intent never required approval
	a := s.approvals[reqID]
	if a.Request.Status == "APPROVED" {
		for id, g := range s.grants {
			if g.ApprovalID == reqID && g.ActionHash == in.ActionHash && g.ExpiresAt.After(now()) && g.UsedCount < g.MaxUses {
				g.UsedCount++
				g.ConsumedByAttempt = st.ID
				s.saveGrant(g)
				a.Request.Status = "CONSUMED"
				s.saveApproval(a)
				s.appendEvent(a.TenantID, a.RunID, st.ID, "", "approval.consumed", "system", "",
					map[string]string{"approval_request_id": reqID, "grant": id})
				return true, nil
			}
		}
	}
	if a.Request.Status == "CONSUMED" {
		for _, g := range s.grants {
			if g.ApprovalID == reqID && g.UsedCount >= 1 {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *Store) RenewLease(ctx context.Context, lease domain.Lease, ttl time.Duration) (domain.DesiredState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[lease.RunID]
	if !ok || r.FenceToken != lease.FenceToken || r.LeaseOwner == nil || *r.LeaseOwner != lease.WorkerID ||
		r.Status != domain.RunRunning {
		return "", domain.ErrLeaseLost
	}
	u := now().Add(ttl)
	r.LeaseUntil = &u
	s.saveRun(r)
	return r.DesiredState, nil
}

func (s *Store) CommitSuccess(ctx context.Context, lease domain.Lease, res domain.StepSuccess) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[lease.RunID]
	if !ok || r.FenceToken != lease.FenceToken {
		return domain.ErrLeaseLost
	}
	att, ok := s.attempts[lease.AttemptID]
	if !ok || att.Status != "RUNNING" || att.FenceToken != lease.FenceToken {
		return domain.ErrLeaseLost
	}
	delete(s.snapshots, lease.ContextSnapshotID)
	end := now()
	att.Status, att.EndedAt = "SUCCEEDED", &end
	s.attempts[att.ID] = att
	s.put("attempt", att.ID, r.ID, int64(att.AttemptNo), att)

	st := s.steps[lease.StepID]
	st.Status = domain.StepSucceeded
	if res.Wait == domain.WaitChildren {
		st.Status = domain.StepWaitingChildren
	}
	if res.Wait == domain.WaitInput {
		st.Status = domain.StepWaitingInput
	}
	if res.OutputRef != "" {
		st.OutputRef = &res.OutputRef
	}
	if res.OutputHash != "" {
		st.OutputHash = &res.OutputHash
	}
	st.UpdatedAt = now()
	s.saveStep(st)
	if res.SideEffect {
		eff := s.effects[lease.IdempotencyKey]
		eff.Key, eff.State, eff.ResultRef, eff.LastCheckAt = lease.IdempotencyKey, "CONFIRMED", res.OutputRef, now()
		s.saveEffect(eff)
	}
	for _, w := range res.KVWrites {
		if w.Key.Scope != "run" || w.Key.OwnerID != r.ID || w.Key.TenantID != r.TenantID {
			return errors.New("step commits may only write run-private KV")
		}
		cur := s.kv[kvKey(w.Key)]
		v := cur.Version + 1
		if w.ExpectedVersion != nil && *w.ExpectedVersion != cur.Version {
			return domain.ErrVersionConflict
		}
		cur.Key, cur.Data, cur.SchemaID, cur.Version, cur.Deleted = w.Key, append([]byte(nil), w.Data...), w.SchemaID, v, w.Delete
		if w.TTL > 0 {
			t := now().Add(w.TTL)
			cur.ExpiresAt = &t
		}
		s.saveKV(cur)
	}
	for _, th := range res.Thoughts {
		s.thoughts[st.ID] = append(s.thoughts[st.ID], th)
		s.put("thought", newID(), st.ID, int64(len(s.thoughts[st.ID])), map[string]any{"step_id": st.ID, "thought": th})
	}
	for _, cap := range res.Capabilities {
		s.caps[st.ID] = append(s.caps[st.ID], cap)
		s.put("capability", newID(), st.ID, int64(len(s.caps[st.ID])), map[string]any{"step_id": st.ID, "ref": cap})
	}
	for _, h := range res.MemoryHits {
		rec := memoryHitRec{MemoryID: h.MemoryID, Version: h.Version, Score: h.Score, Rank: h.Rank, StepSeq: st.Seq}
		s.memHits[st.ID] = append(s.memHits[st.ID], rec)
		s.put("memhit", newID(), st.ID, int64(len(s.memHits[st.ID])),
			map[string]any{"step_id": st.ID, "hit": rec})
	}
	if res.Compaction != nil {
		rec := compactRec{StepID: st.ID, RunID: r.ID, Seq: st.Seq, Compaction: *res.Compaction, CreatedAt: now()}
		s.compactions[st.ID] = rec
		s.put("compact", st.ID, r.ID, st.Seq, rec)
		r.CompactionEpoch++
		s.appendEvent(r.TenantID, r.ID, st.ID, att.ID, "context.compacted", "worker", lease.WorkerID,
			map[string]any{"compaction_epoch": r.CompactionEpoch,
				"compacted_before": res.Compaction.CompactedBefore, "summary_ref": res.Compaction.SummaryRef})
	}
	s.appendEvent(r.TenantID, r.ID, st.ID, att.ID, "step.succeeded", "worker", lease.WorkerID, res.Summary)
	for _, e := range res.Events {
		if e.Type != "" {
			s.appendEvent(r.TenantID, r.ID, st.ID, att.ID, e.Type, "system", "", e.Payload)
		}
	}
	if res.Flow != nil {
		if err := s.applyFlowLocked(&r, lease, res.Flow); err != nil {
			return err
		}
	}
	r.LeaseOwner, r.LeaseUntil = nil, nil
	r.Version++
	if res.CheckpointRef != "" {
		s.createCheckpointLocked(r, lease, res)
	}
	if res.Complete {
		r.Status = domain.RunCompleted
		r.ResultRef = &res.ResultRef
		s.appendEvent(r.TenantID, r.ID, "", "", "run.completed", "system", "", map[string]string{"result_ref": res.ResultRef})
		s.finishParentLocked(r, "SUCCEEDED", res.ResultRef, "")
		s.finalizeTurnLocked(r)
	} else if res.Wait != domain.WaitNone {
		if res.Wait == domain.WaitChildren {
			r.Status = domain.RunWaitingChildren
		}
		if res.Wait == domain.WaitInput {
			r.Status = domain.RunWaitingInput
		}
		s.appendEvent(r.TenantID, r.ID, st.ID, "", "flow."+string(res.Wait), "system", "", map[string]string{"flow_node": lease.FlowNodeID})
	} else {
		r.Status = domain.RunQueued
		waitingApproval, waitingChildren, waitingInput := false, false, false
		for _, p := range res.Plan {
			if _, err := s.insertStepLocked(&r, p); err != nil {
				return err
			}
			switch p.Status {
			case domain.StepWaitingApproval:
				waitingApproval = true
			case domain.StepWaitingChildren:
				waitingChildren = true
			case domain.StepWaitingInput:
				waitingInput = true
			}
		}
		switch {
		case waitingApproval:
			r.Status = domain.RunWaitingApproval
		case waitingChildren:
			r.Status = domain.RunWaitingChildren
		case waitingInput:
			r.Status = domain.RunWaitingInput
		}
	}
	// Pause/cancel requests made while the step was executing settle now that
	// the lease is released. A completed run is never retroactively cancelled.
	if !res.Complete {
		switch r.DesiredState {
		case domain.DesiredCancelled:
			s.cancelRunLocked(&r, "system", "", "cancel requested")
			return nil
		case domain.DesiredPaused:
			if r.Status == domain.RunQueued || r.Status == domain.RunWaitingApproval ||
				r.Status == domain.RunWaitingInput || r.Status == domain.RunWaitingChildren {
				r.Status = domain.RunPaused
				s.appendEvent(r.TenantID, r.ID, "", "", "run.paused", "system", "", nil)
			}
		}
	}
	s.saveRun(r)
	return nil
}

func (s *Store) CommitFailure(ctx context.Context, lease domain.Lease, f domain.StepFailure) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[lease.RunID]
	if !ok || r.FenceToken != lease.FenceToken {
		return domain.ErrLeaseLost
	}
	att, ok := s.attempts[lease.AttemptID]
	if !ok || att.Status != "RUNNING" || att.FenceToken != lease.FenceToken {
		return domain.ErrLeaseLost
	}
	delete(s.snapshots, lease.ContextSnapshotID)
	class := f.Class
	if class == domain.FailRetryable && lease.AttemptNo >= 3 {
		class = domain.FailPermanent
		f.Message = "attempts exhausted: " + f.Message
	}
	end := now()
	att.Status, att.EndedAt, att.ErrorCode, att.ErrorMessage = "FAILED", &end, f.Code, f.Message
	if f.TimedOut {
		att.Status = "TIMED_OUT"
	}
	s.attempts[att.ID] = att
	s.put("attempt", att.ID, r.ID, int64(att.AttemptNo), att)
	st := s.steps[lease.StepID]
	st.UpdatedAt = now()
	r.LeaseOwner, r.LeaseUntil = nil, nil
	r.Version++
	switch class {
	case domain.FailRetryable:
		st.Status = domain.StepRetryWait
		next := now().Add(max(f.RetryAfter, 500*time.Millisecond*time.Duration(1<<(lease.AttemptNo-1))))
		st.NextAttemptAt = &next
		s.saveStep(st)
		r.Status = domain.RunQueued
		r.NotBefore = &next
		if e, ok := s.effects[lease.IdempotencyKey]; ok && e.State != "CONFIRMED" {
			e.State, e.LastCheckAt = "PREPARED", now()
			s.saveEffect(e)
		}
		s.appendEvent(r.TenantID, r.ID, st.ID, att.ID, "step.retry_scheduled", "worker", lease.WorkerID,
			map[string]any{"not_before": next, "error_code": f.Code})
	case domain.FailCancelled:
		st.Status = domain.StepCancelled
		s.saveStep(st)
		s.cancelRunLocked(&r, "worker", lease.WorkerID, "cancelled during execution")
		return nil
	case domain.FailUnknownEffect:
		st.Status = domain.StepUnknown
		s.saveStep(st)
		if e, ok := s.effects[lease.IdempotencyKey]; ok && e.State != "CONFIRMED" {
			e.State, e.LastCheckAt = "UNKNOWN", now()
			s.saveEffect(e)
		}
		r.Status = domain.RunNeedsHuman
		r.FailureReason = &f.Message
		s.saveRun(r)
		s.appendEvent(r.TenantID, r.ID, st.ID, att.ID, "step.unknown", "worker", lease.WorkerID,
			map[string]string{"error_code": f.Code, "message": f.Message})
		return nil
	default:
		st.Status = domain.StepFailed
		s.saveStep(st)
		s.failRunLocked(&r, f.Code+": "+f.Message)
		return nil
	}
	s.saveRun(r)
	return nil
}

func (s *Store) PrepareEffect(ctx context.Context, lease domain.Lease, target string) (domain.EffectState, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.runs[lease.RunID]; !ok || r.FenceToken != lease.FenceToken {
		return "", "", domain.ErrLeaseLost
	}
	e, ok := s.effects[lease.IdempotencyKey]
	if !ok {
		e = effectRec{Key: lease.IdempotencyKey, State: "PREPARED", LastCheckAt: now()}
	}
	switch e.State {
	case "PREPARED":
		e.State = "DISPATCHED"
		e.LastCheckAt = now()
		s.saveEffect(e)
		return domain.EffectDispatched, "", nil
	case "CONFIRMED":
		return domain.EffectConfirmed, e.ResultRef, nil
	case "DISPATCHED", "UNKNOWN":
		// The previous attempt may have reached the target. The kernel turns
		// this into UNKNOWN instead of calling the tool twice.
		return "", "", domain.ErrUnknownEffect
	default:
		return "", "", domain.ErrUnknownEffect
	}
}

// RecordEffectResult confirms a dispatched side effect ahead of the step
// commit, so a lost commit is retried by reusing the result (see store/pg).
func (s *Store) RecordEffectResult(ctx context.Context, lease domain.Lease, target, resultRef string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[lease.RunID]
	if !ok || r.Status != domain.RunRunning || r.FenceToken != lease.FenceToken || r.LeaseOwner == nil ||
		*r.LeaseOwner != lease.WorkerID || r.LeaseUntil == nil || !r.LeaseUntil.After(now()) {
		return domain.ErrLeaseLost
	}
	e, ok := s.effects[lease.IdempotencyKey]
	if !ok || e.State != "DISPATCHED" {
		return fmt.Errorf("effect %s is not awaiting a result", lease.IdempotencyKey)
	}
	e.State, e.ResultRef, e.LastCheckAt = "CONFIRMED", resultRef, now()
	s.saveEffect(e)
	s.appendEvent(r.TenantID, r.ID, lease.StepID, lease.AttemptID, "effect.confirmed", "worker", lease.WorkerID,
		map[string]string{"target": target, "result_ref": resultRef})
	return nil
}

// AttemptStatus reports the lease's attempt status while it carries the
// lease's fence (see store/pg).
func (s *Store) AttemptStatus(ctx context.Context, lease domain.Lease) (domain.AttemptStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.attempts[lease.AttemptID]
	if !ok || a.FenceToken != lease.FenceToken {
		return "", domain.ErrNotFound
	}
	return domain.AttemptStatus(a.Status), nil
}

func (s *Store) History(ctx context.Context, tenantID, runID string, beforeSeq int64) ([]domain.Step, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.historyLocked(tenantID, runID, beforeSeq, map[string]bool{})
}

func (s *Store) historyLocked(tenantID, runID string, beforeSeq int64, seen map[string]bool) ([]domain.Step, error) {
	if seen[runID] {
		return nil, nil
	}
	seen[runID] = true
	r, ok := s.runs[runID]
	if !ok || (tenantID != "" && r.TenantID != tenantID) {
		return nil, domain.ErrNotFound
	}
	var prefix []domain.Step
	if r.ForkedFrom != nil {
		if cp, ok := s.checkpoints[*r.ForkedFrom]; ok {
			var err error
			prefix, err = s.historyLocked(tenantID, cp.RunID, cp.LastCommittedStepSeq+1, seen)
			if err != nil {
				return nil, err
			}
		}
	}
	var own []domain.Step
	for _, st := range sortedSteps(s.steps, runID) {
		if beforeSeq == 0 || st.Seq < beforeSeq {
			own = append(own, st)
		}
	}
	return append(prefix, own...), nil
}

func (s *Store) SnapshotKVVersions(ctx context.Context, tenantID, snapshotID string) (map[string]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int64{}
	runID := s.snapshots[snapshotID]
	for k, v := range s.kv {
		if v.Key.TenantID != tenantID || v.Key.Scope != "run" {
			continue
		}
		if runID != "" && v.Key.OwnerID != runID {
			continue
		}
		out[k] = v.Version
	}
	return out, nil
}

func (s *Store) KVDataAt(ctx context.Context, key domain.KVKey, version int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := kvKey(key) + "\x00" + fmt.Sprint(version)
	var data []byte
	if err := s.db.QueryRow(`SELECT data FROM objects WHERE kind='kvver' AND id=?`, id).Scan(&data); err == nil {
		var rec kvRec
		if json.Unmarshal(data, &rec) == nil {
			if rec.Deleted {
				return nil, domain.ErrNotFound
			}
			return append([]byte(nil), rec.Data...), nil
		}
	}
	v, ok := s.kv[kvKey(key)]
	if !ok || v.Deleted || v.Version < version {
		return nil, domain.ErrNotFound
	}
	return append([]byte(nil), v.Data...), nil
}

func (s *Store) RunConversation(ctx context.Context, tenantID, runID string) ([]domain.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok || r.BranchID == nil {
		return nil, nil
	}
	var out []domain.Message
	for _, m := range sortedMessages(s.messages, *r.BranchID) {
		if r.TurnID != nil {
			t, ok := s.turns[*r.TurnID]
			if ok && m.TurnID == t.ID {
				continue
			}
		}
		out = append(out, m)
	}
	return out, nil
}

func (s *Store) LoadFlow(ctx context.Context, tenantID, runID string) (domain.FlowSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.flows[runID]
	if !ok {
		return domain.FlowSnapshot{}, domain.ErrNotFound
	}
	return v, nil
}

func (s *Store) FlowInput(ctx context.Context, tenantID, stepID string) (json.RawMessage, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.steps[stepID]
	if !ok {
		return nil, false, domain.ErrNotFound
	}
	in, ok := s.flowInputs[flowInputKey(st.RunID, st.FlowNodeID)]
	if !ok {
		return nil, false, nil
	}
	return in.Value, true, nil
}

func (s *Store) flowChildrenReadyLocked(runID string) bool {
	for _, n := range s.flows[runID].Nodes {
		if n.Status == "PENDING" || n.Status == "RUNNING" {
			return false
		}
	}
	return true
}

func (s *Store) finishParentLocked(child domain.Run, status, resultRef, errMsg string) {
	if child.ParentRunID == nil {
		return
	}
	parent, ok := s.runs[*child.ParentRunID]
	if !ok {
		return
	}
	fl := s.flows[parent.ID]
	for i := range fl.Nodes {
		if fl.Nodes[i].ChildRunID == child.ID {
			fl.Nodes[i].Status = status
			fl.Nodes[i].OutputRef = resultRef
			fl.Nodes[i].Error = errMsg
		}
	}
	s.saveFlow(fl)
	if parent.Status == domain.RunWaitingChildren {
		parent.Status = domain.RunQueued
		parent.Version++
		s.saveRun(parent)
	}
}

func max(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

var _ = sort.Strings
