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
		s.recoverExpiredLocked(r)
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
	r.FenceToken++
	until := now().Add(ttl)
	r.LeaseOwner, r.LeaseUntil, r.NotBefore = &workerID, &until, nil
	r.Status = domain.RunRunning
	r.Version++
	target.Status = domain.StepRunning
	target.CurrentAttempt++
	target.UpdatedAt = now()
	att := attempt{ID: attemptID, RunID: r.ID, StepID: target.ID, AttemptNo: target.CurrentAttempt,
		WorkerID: workerID, FenceToken: r.FenceToken, Status: "RUNNING", IdempotencyKey: target.InputHash,
		StartedAt: now()}
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
		ContextSnapshotID: newID(), IdempotencyKey: att.IdempotencyKey, Deadline: until,
		Intent: intent, ProfileID: r.ProfileID, ProfileVersion: r.ProfileVersion,
		ParentRunID: deref(r.ParentRunID), ParentStepID: deref(r.ParentStepID), FlowDepth: r.FlowDepth}, nil
}

func (s *Store) recoverExpiredLocked(r domain.Run) {
	for id, a := range s.attempts {
		if a.RunID == r.ID && a.Status == "RUNNING" {
			a.Status = "LOST"
			end := now()
			a.EndedAt = &end
			s.attempts[id] = a
			s.put("attempt", id, r.ID, int64(a.AttemptNo), a)
			st := s.steps[a.StepID]
			st.Status = domain.StepPending
			s.saveStep(st)
		}
	}
	r.LeaseOwner, r.LeaseUntil = nil, nil
	r.Status = domain.RunQueued
	r.Version++
	s.saveRun(r)
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
	if !ok || att.Status != "RUNNING" {
		return domain.ErrLeaseLost
	}
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
		for _, p := range res.Plan {
			if _, err := s.insertStepLocked(&r, p); err != nil {
				return err
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
	if !ok || att.Status != "RUNNING" {
		return domain.ErrLeaseLost
	}
	end := now()
	att.Status, att.EndedAt, att.ErrorCode, att.ErrorMessage = "FAILED", &end, f.Code, f.Message
	s.attempts[att.ID] = att
	s.put("attempt", att.ID, r.ID, int64(att.AttemptNo), att)
	st := s.steps[lease.StepID]
	st.UpdatedAt = now()
	r.LeaseOwner, r.LeaseUntil = nil, nil
	r.Version++
	switch f.Class {
	case domain.FailRetryable:
		if lease.AttemptNo < 3 {
			st.Status = domain.StepRetryWait
			next := now().Add(max(f.RetryAfter, 500*time.Millisecond))
			st.NextAttemptAt = &next
			r.Status = domain.RunQueued
			r.NotBefore = &next
		} else {
			st.Status = domain.StepFailed
			r.Status = domain.RunFailed
			r.FailureReason = &f.Message
			s.finishParentLocked(r, "FAILED", "", f.Message)
		}
	case domain.FailCancelled:
		st.Status = domain.StepCancelled
		r.Status = domain.RunCancelled
		s.finishParentLocked(r, "CANCELLED", "", f.Message)
	case domain.FailUnknownEffect:
		st.Status = domain.StepUnknown
		r.Status = domain.RunNeedsHuman
		r.FailureReason = &f.Message
	default:
		st.Status = domain.StepFailed
		r.Status = domain.RunFailed
		r.FailureReason = &f.Message
		s.finishParentLocked(r, "FAILED", "", f.Message)
	}
	s.saveStep(st)
	s.appendEvent(r.TenantID, r.ID, st.ID, att.ID, "step.failed", "worker", lease.WorkerID, map[string]string{"code": f.Code, "message": f.Message})
	if r.Status == domain.RunFailed || r.Status == domain.RunCancelled || r.Status == domain.RunNeedsHuman {
		s.appendEvent(r.TenantID, r.ID, "", "", "run."+string(r.Status), "system", "", map[string]string{"reason": f.Message})
		s.finalizeTurnLocked(r)
	}
	s.saveRun(r)
	return nil
}

func (s *Store) PrepareEffect(ctx context.Context, lease domain.Lease, target string) (domain.EffectState, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.effects[lease.IdempotencyKey]
	if ok && e.State == "CONFIRMED" {
		return domain.EffectConfirmed, e.ResultRef, nil
	}
	s.saveEffect(effectRec{Key: lease.IdempotencyKey, State: "DISPATCHED", LastCheckAt: now()})
	return domain.EffectDispatched, "", nil
}

func (s *Store) History(ctx context.Context, tenantID, runID string, beforeSeq int64) ([]domain.Step, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Step
	for _, st := range sortedSteps(s.steps, runID) {
		if beforeSeq == 0 || st.Seq < beforeSeq {
			out = append(out, st)
		}
	}
	return out, nil
}

func (s *Store) SnapshotKVVersions(ctx context.Context, tenantID, snapshotID string) (map[string]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int64{}
	for k, v := range s.kv {
		if v.Key.TenantID == tenantID && v.Key.Scope == "run" {
			out[k] = v.Version
		}
	}
	return out, nil
}

func (s *Store) KVDataAt(ctx context.Context, key domain.KVKey, version int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
