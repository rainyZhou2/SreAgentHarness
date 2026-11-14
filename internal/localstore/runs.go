package localstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
)

func (s *Store) CreateRun(ctx context.Context, req domain.CreateRunRequest) (domain.Run, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var run domain.Run
	var created bool
	err := s.withStateTxLocked(ctx, func() error {
		var err error
		run, created, err = s.createRunLocked(ctx, req)
		return err
	})
	if err != nil {
		return domain.Run{}, false, err
	}
	return copyWorkRun(s.runs[run.ID]), created, nil
}

func (s *Store) createRunLocked(ctx context.Context, req domain.CreateRunRequest) (domain.Run, bool, error) {
	if err := domain.ValidateWorkRunReservation(req); err != nil {
		return domain.Run{}, false, err
	}
	if req.InitiatorType == "" {
		req.InitiatorType = "user"
	}
	if req.IdempotencyKey != "" {
		if id, ok := s.runByKey[req.TenantID+"/"+req.IdempotencyKey]; ok {
			if !domain.SameWorkRunOwner(s.runs[id].WorkOwner, req.WorkOwner) {
				return domain.Run{}, false, &domain.RejectedError{Reason: "work_run_owner_mismatch"}
			}
			return copyWorkRun(s.runs[id]), false, nil
		}
	}
	if req.ProfileVersion > 0 {
		p, ok := s.profiles[profileKey(req.TenantID, req.ProfileID, req.ProfileVersion)]
		if !ok {
			return domain.Run{}, false, &domain.RejectedError{Reason: "unknown_profile_version"}
		}
		if p.Status != "ACTIVE" {
			return domain.Run{}, false, &domain.RejectedError{Reason: "profile_version_not_runnable"}
		}
	}
	status := domain.RunQueued
	if req.PendingActivation {
		status = domain.RunPendingActivation
	}
	r := domain.Run{WorkOwner: copyWorkOwner(req.WorkOwner), ID: newID(), TenantID: req.TenantID, IncidentID: req.IncidentID, ProfileID: req.ProfileID,
		ProfileVersion: req.ProfileVersion, Status: status, DesiredState: domain.DesiredRunning, Version: 1,
		NextStepSeq: req.StepSeqStart, GoalRef: req.GoalRef, InitiatorType: domain.StoredInitiatorType(req.InitiatorType),
		InitiatorID: strptr(req.InitiatorID), SessionID: req.SessionID, BranchID: req.BranchID, TurnID: req.TurnID,
		ParentRunID: req.ParentRunID, ParentStepID: req.ParentStepID, FlowNodeID: req.FlowNodeID,
		FlowDepth: req.FlowDepth, ForkedFrom: req.ForkedFrom, RolloutID: req.RolloutID,
		CreatedAt: now(), UpdatedAt: now()}
	steps := req.InitialSteps
	if len(steps) == 0 {
		strategy := ""
		if p, ok := s.profiles[profileKey(req.TenantID, req.ProfileID, req.ProfileVersion)]; ok {
			strategy = p.Strategy
		}
		if req.FlowNodeID == "" && strategy == "flow" {
			steps = []domain.PlannedStep{{Kind: domain.StepFlow, Executor: "flow", InputRef: req.GoalRef,
				InputHash: req.GoalHash, Status: domain.StepPending}}
		} else {
			steps = []domain.PlannedStep{
				{Kind: domain.StepRetrieve, Executor: "memory", InputRef: req.GoalRef,
					InputHash: req.GoalHash, Status: domain.StepPending},
				{Kind: domain.StepReason, Executor: "reasoner", InputRef: req.GoalRef,
					InputHash: req.GoalHash, Status: domain.StepPending},
			}
		}
	}
	s.saveRun(r)
	if req.IdempotencyKey != "" {
		key := req.TenantID + "/" + req.IdempotencyKey
		s.runByKey[key] = r.ID
		s.put("runkey", key, req.TenantID, 0, map[string]string{"run_id": r.ID})
	}
	s.appendEvent(r.TenantID, r.ID, "", "", "run.created", req.InitiatorType, req.InitiatorID, map[string]any{
		"profile_id": req.ProfileID, "profile_version": req.ProfileVersion, "pending_activation": req.PendingActivation})
	for _, p := range steps {
		if _, err := s.insertStepLocked(&r, p); err != nil {
			return domain.Run{}, false, err
		}
	}
	return s.runs[r.ID], true, nil
}

func (s *Store) insertStepLocked(r *domain.Run, p domain.PlannedStep) (string, error) {
	id := newID()
	seq := r.NextStepSeq
	r.NextStepSeq++
	st := domain.Step{ID: id, TenantID: r.TenantID, RunID: r.ID, Seq: seq, Kind: p.Kind, ExecutorName: p.Executor,
		FlowNodeID: p.FlowNodeID, Status: p.Status, InputRef: p.InputRef, InputHash: p.InputHash,
		CreatedAt: now(), UpdatedAt: now()}
	if p.StatusReason != "" {
		st.StatusReason = strptr(p.StatusReason)
	}
	if st.Status == "" {
		st.Status = domain.StepPending
	}
	s.saveRun(*r)
	s.saveStep(st)
	s.appendEvent(r.TenantID, r.ID, id, "", "step.planned", "system", "", map[string]any{
		"seq": seq, "kind": p.Kind, "status": st.Status})
	if p.Intent != nil {
		in := *p.Intent
		in.ID = newID()
		s.saveIntent(in)
		s.intentStep[id] = in.ID
		s.put("intentstep", id, r.ID, 0, map[string]string{"step_id": id, "intent_id": in.ID})
		if p.Status == domain.StepWaitingApproval {
			req := domain.ApprovalRequest{ID: newID(), ActionIntentID: in.ID, StepID: id, ActionHash: in.ActionHash,
				RiskClass: in.RiskClass, ToolName: in.ToolName, Target: in.Target, RequiredApprovals: p.RequiredApprovals,
				Status: "PENDING", ExpiresAt: now().Add(30 * time.Minute), CreatedAt: now()}
			if req.RequiredApprovals < 1 {
				req.RequiredApprovals = 1
			}
			s.saveApproval(approvalRec{Request: req, RunID: r.ID, StepID: id, IntentID: in.ID, TenantID: r.TenantID})
			s.appendEvent(r.TenantID, r.ID, id, "", "approval.requested", "system", "", map[string]any{
				"approval_request_id": req.ID, "action_hash": in.ActionHash, "risk_class": in.RiskClass,
				"tool": in.ToolName, "target": in.Target, "required_approvals": req.RequiredApprovals})
		}
	}
	return id, nil
}

func (s *Store) GetRun(ctx context.Context, tenantID, runID string) (domain.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok || r.TenantID != tenantID {
		return domain.Run{}, domain.ErrNotFound
	}
	return copyWorkRun(r), nil
}

func (s *Store) ListRuns(ctx context.Context, tenantID string, status domain.RunStatus, limit int) ([]domain.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Run
	for _, r := range s.runs {
		if r.TenantID != tenantID {
			continue
		}
		if status != "" && r.Status != status {
			continue
		}
		out = append(out, copyWorkRun(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) ListSteps(ctx context.Context, tenantID, runID string) ([]domain.Step, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.runs[runID]; !ok || r.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return sortedSteps(s.steps, runID), nil
}

func (s *Store) ListEvents(ctx context.Context, tenantID, runID string, afterSeq int64, limit int) ([]domain.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.runs[runID]; !ok || r.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	return s.listEventsLocked(runID, afterSeq, limit)
}

func (s *Store) ListApprovals(ctx context.Context, tenantID, runID string) ([]domain.ApprovalRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.runs[runID]; !ok || r.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	var out []domain.ApprovalRequest
	for _, a := range s.approvals {
		if a.RunID == runID {
			out = append(out, a.Request)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) RunTrace(ctx context.Context, tenantID, runID string) (map[string][]domain.Thought, map[string][]domain.CapabilityRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.runs[runID]; !ok || r.TenantID != tenantID {
		return nil, nil, domain.ErrNotFound
	}
	trace := map[string][]domain.Thought{}
	caps := map[string][]domain.CapabilityRef{}
	for _, st := range sortedSteps(s.steps, runID) {
		if v := s.thoughts[st.ID]; len(v) > 0 {
			trace[st.ID] = v
		}
		caps[st.ID] = s.caps[st.ID]
	}
	return trace, caps, nil
}

func (s *Store) GetCommand(ctx context.Context, tenantID, runID, commandID string) (domain.Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.commands[commandID]
	if !ok || c.RunID != runID || c.TenantID != tenantID {
		return domain.Command{}, domain.ErrNotFound
	}
	return c, nil
}

func (s *Store) ApplyCommand(ctx context.Context, tenantID, runID string, req domain.CommandRequest) (domain.Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.runs[runID]; !ok || r.TenantID != tenantID {
		return domain.Command{}, domain.ErrNotFound
	}
	if req.IdempotencyKey != "" {
		if id, ok := s.cmdKey[s.commandKey(tenantID, runID, req.IdempotencyKey)]; ok {
			if c, ok := s.commands[id]; ok {
				if c.Status == domain.CommandRejected && c.RejectReason != nil {
					if *c.RejectReason == "version_conflict" {
						return c, fmt.Errorf("%w: %w", domain.ErrVersionConflict, &domain.RejectedError{Reason: *c.RejectReason, Cmd: c})
					}
					return c, &domain.RejectedError{Reason: *c.RejectReason, Cmd: c}
				}
				return c, nil
			}
		}
	}
	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	c := domain.Command{ID: newID(), TenantID: tenantID, RunID: runID, Type: req.Type, ActorID: req.ActorID,
		ExpectedVersion: req.ExpectedVersion, Status: domain.CommandApplied, Payload: payload, CreatedAt: now()}

	// Every command runs against a private buffer. A rejection or an error
	// discards the buffer and restores the in-memory snapshot, so the run is
	// never left half-mutated.
	snap := s.snapshotStateLocked()
	buf := newStateBuf()
	s.buf = buf
	defer func() {
		if s.buf == buf {
			s.buf = nil // a panic must not leave the next call in buffered mode
		}
	}()
	s.appendEvent(tenantID, runID, "", "", "command.received", "user", req.ActorID,
		map[string]any{"command_id": c.ID, "type": req.Type, "reason": req.Reason})
	reason, err := s.applyCommandLocked(&c, req)
	if err != nil {
		s.buf = nil
		s.restoreStateLocked(snap)
		return domain.Command{}, err
	}
	applied := now()
	c.AppliedAt = &applied
	if reason != "" {
		s.buf = nil
		s.restoreStateLocked(snap)
		c.Status = domain.CommandRejected
		c.RejectReason = &reason
		v := s.runs[runID].Version
		c.RunVersionAfter = &v
		s.persistCommandKey(tenantID, runID, req.IdempotencyKey, c.ID)
		s.saveCommand(c)
		s.appendEvent(tenantID, runID, "", "", "command.received", "user", req.ActorID,
			map[string]any{"command_id": c.ID, "type": req.Type, "reason": req.Reason})
		s.appendEvent(tenantID, runID, "", "", "command.rejected", "user", req.ActorID,
			map[string]string{"command_id": c.ID, "reason": reason})
		if reason == "version_conflict" {
			return c, fmt.Errorf("%w: %w", domain.ErrVersionConflict, &domain.RejectedError{Reason: reason, Cmd: c})
		}
		return c, &domain.RejectedError{Reason: reason, Cmd: c}
	}
	r := s.runs[runID]
	v := r.Version
	c.RunVersionAfter = &v
	s.saveCommand(c)
	s.persistCommandKey(tenantID, runID, req.IdempotencyKey, c.ID)
	s.appendEvent(tenantID, runID, "", "", "command.applied", "user", req.ActorID,
		map[string]string{"command_id": c.ID})
	if err := s.commitBufferLocked(ctx, buf); err != nil {
		s.restoreStateLocked(snap)
		return domain.Command{}, err
	}
	return c, nil
}

func (s *Store) persistCommandKey(tenantID, runID, key, commandID string) {
	if key == "" {
		return
	}
	k := s.commandKey(tenantID, runID, key)
	s.cmdKey[k] = commandID
	s.put("cmdkey", k, runID, 0, map[string]string{"command_id": commandID})
}

func (s *Store) applyCommandLocked(c *domain.Command, req domain.CommandRequest) (string, error) {
	r, ok := s.runs[c.RunID]
	if !ok {
		return "run_not_found", nil
	}
	needsVersion := map[domain.CommandType]bool{domain.CmdResume: true, domain.CmdApproveTool: true,
		domain.CmdRejectTool: true, domain.CmdResolveUnknown: true, domain.CmdOverrideResult: true,
		domain.CmdReplacePendingInput: true, domain.CmdRetry: true, domain.CmdMigrateProfile: true,
		domain.CmdProvideInput: true}
	if needsVersion[req.Type] && req.ExpectedVersion == nil {
		return "expected_version_required", nil
	}
	if req.ExpectedVersion != nil && *req.ExpectedVersion != r.Version {
		return "version_conflict", nil
	}
	if r.Status.Terminal() {
		return "run_terminal", nil
	}
	switch req.Type {
	case domain.CmdPause:
		if r.Status == domain.RunPaused {
			return "", nil
		}
		if r.Status == domain.RunPendingActivation {
			// A queued conversation turn must retain its activation barrier.
			if r.DesiredState != domain.DesiredPaused {
				r.DesiredState = domain.DesiredPaused
				r.Version++
				s.saveRun(r)
			}
			s.appendEvent(r.TenantID, r.ID, "", "", "run.pause_requested", "user", req.ActorID, map[string]string{"command_id": c.ID})
			return "", nil
		}
		if r.Status == domain.RunNeedsHuman {
			return "invalid_state", nil
		}
		r.DesiredState = domain.DesiredPaused
		if r.Status == domain.RunQueued || r.Status == domain.RunWaitingApproval ||
			r.Status == domain.RunWaitingInput || r.Status == domain.RunWaitingChildren {
			r.Status = domain.RunPaused
		}
		r.Version++
		s.saveRun(r)
		s.appendEvent(r.TenantID, r.ID, "", "", "run.pause_requested", "user", req.ActorID, map[string]string{"command_id": c.ID})
		return "", nil
	case domain.CmdResume:
		if r.Status == domain.RunPendingActivation && r.DesiredState == domain.DesiredPaused {
			r.DesiredState = domain.DesiredRunning
			r.Version++
			s.saveRun(r)
			s.appendEvent(r.TenantID, r.ID, "", "", "run.pause_withdrawn", "user", req.ActorID, nil)
			return "", nil
		}
		if r.Status != domain.RunPaused {
			return "invalid_state", nil
		}
		r.DesiredState = domain.DesiredRunning
		r.Status = domain.RunQueued
		r.Version++
		s.saveRun(r)
		s.appendEvent(r.TenantID, r.ID, "", "", "run.resumed", "user", req.ActorID, map[string]string{"command_id": c.ID})
		return "", nil
	case domain.CmdCancel:
		if r.Status.Terminal() {
			return "", nil
		}
		if r.Status == domain.RunRunning && r.LeaseUntil != nil && r.LeaseUntil.After(now()) {
			// A worker is executing. Record the intent; the commit/failure
			// path settles the run so the in-flight attempt is accounted for.
			if r.DesiredState != domain.DesiredCancelled {
				r.DesiredState = domain.DesiredCancelled
				r.Version++
				s.saveRun(r)
				s.appendEvent(r.TenantID, r.ID, "", "", "run.cancel_requested", "user", req.ActorID,
					map[string]string{"command_id": c.ID, "reason": req.Reason})
			}
			return "", nil
		}
		if r.Status == domain.RunRunning {
			s.recoverExpiredLocked(&r)
			r = s.runs[c.RunID]
			if r.Status.Terminal() {
				return "", nil
			}
		}
		s.cancelRunLocked(&r, "user", req.ActorID, req.Reason)
		return "", nil
	case domain.CmdApproveTool, domain.CmdRejectTool:
		return s.decideApprovalLocked(c, req)
	case domain.CmdProvideInput:
		return s.provideInputLocked(c, req)
	case domain.CmdResolveUnknown:
		return s.resolveUnknownLocked(c, req)
	case domain.CmdRetry:
		return s.retryStepLocked(c, req)
	case domain.CmdOverrideResult:
		return s.overrideResultLocked(c, req)
	case domain.CmdReplacePendingInput:
		return s.replaceInputLocked(c, req)
	case domain.CmdInjectEvidence:
		return s.injectEvidenceLocked(c, req)
	case domain.CmdMigrateProfile:
		return s.migrateProfileLocked(c, req)
	default:
		return "unsupported_command", nil
	}
}

func (s *Store) decideApprovalLocked(c *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		ApprovalRequestID string `json:"approval_request_id"`
		ActionHash        string `json:"action_hash"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || p.ApprovalRequestID == "" || p.ActionHash == "" {
		return "approval_request_id_and_action_hash_required", nil
	}
	a, ok := s.approvals[p.ApprovalRequestID]
	if !ok || a.RunID != c.RunID {
		return "approval_not_found", nil
	}
	if a.Request.Status != "PENDING" {
		return "approval_not_pending", nil
	}
	if !a.Request.ExpiresAt.After(now()) {
		return "approval_expired", nil
	}
	if p.ActionHash != a.Request.ActionHash {
		return "action_hash_mismatch", nil
	}
	decision := "APPROVE"
	if c.Type == domain.CmdRejectTool {
		decision = "REJECT"
	}
	// Local mode is single-user: the operator who started the run is also the
	// approver. The server's distinct-approver policy is configurable and is
	// not applied here.
	for _, d := range s.decisions[a.Request.ID] {
		if d.ApproverID == c.ActorID {
			return "approver_already_decided", nil
		}
	}
	s.decisions[a.Request.ID] = append(s.decisions[a.Request.ID], decisionRec{ID: newID(), ApprovalID: a.Request.ID,
		ApproverID: c.ActorID, Decision: decision, ActionHash: p.ActionHash, CommandID: c.ID})
	s.put("decision", newID(), a.Request.ID, int64(len(s.decisions[a.Request.ID])), s.decisions[a.Request.ID][len(s.decisions[a.Request.ID])-1])
	if decision == "REJECT" {
		a.Request.Status = "REJECTED"
		s.saveApproval(a)
		s.appendEvent(a.TenantID, a.RunID, a.StepID, "", "approval.decided", "user", c.ActorID,
			map[string]string{"approval_request_id": a.Request.ID, "decision": "rejected"})
		r := s.runs[a.RunID]
		return "", s.skipBlockedStepLocked(&r, a.StepID, "rejected by approver")
	}
	approvals := 0
	for _, d := range s.decisions[a.Request.ID] {
		if d.Decision == "APPROVE" {
			approvals++
		}
	}
	if approvals < a.Request.RequiredApprovals {
		s.appendEvent(a.TenantID, a.RunID, a.StepID, "", "approval.partial", "user", c.ActorID,
			map[string]any{"approval_request_id": a.Request.ID, "approvals": approvals, "required": a.Request.RequiredApprovals})
		return "", nil
	}
	a.Request.Status = "APPROVED"
	s.saveApproval(a)
	s.saveGrant(grantRec{ID: newID(), ApprovalID: a.Request.ID, ActionHash: a.Request.ActionHash,
		MaxUses: 1, ExpiresAt: a.Request.ExpiresAt})
	st := s.steps[a.StepID]
	if st.Status == domain.StepWaitingApproval {
		st.Status = domain.StepPending
	}
	s.saveStep(st)
	r := s.runs[a.RunID]
	if r.Status == domain.RunWaitingApproval {
		r.Status = domain.RunQueued
	}
	r.Version++
	s.saveRun(r)
	s.appendEvent(a.TenantID, a.RunID, a.StepID, "", "approval.decided", "user", c.ActorID,
		map[string]string{"approval_request_id": a.Request.ID, "decision": "approved"})
	return "", nil
}

func (s *Store) provideInputLocked(c *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		Value json.RawMessage `json:"value"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || len(p.Value) == 0 || !json.Valid(p.Value) {
		return "payload.value_required", nil
	}
	var target domain.Step
	found := false
	for _, st := range sortedSteps(s.steps, c.RunID) {
		if st.Status == domain.StepWaitingInput {
			target, found = st, true
			break
		}
	}
	if !found {
		return "no_pending_input", nil
	}
	if _, ok := s.flows[c.RunID]; !ok {
		return "flow_not_found", nil
	}
	s.flowInputs[flowInputKey(c.RunID, target.FlowNodeID)] = flowInput{RunID: c.RunID, NodeID: target.FlowNodeID,
		StepID: target.ID, ActorID: c.ActorID, Value: p.Value}
	s.put("flowinput", flowInputKey(c.RunID, target.FlowNodeID), c.RunID, 0, s.flowInputs[flowInputKey(c.RunID, target.FlowNodeID)])
	target.Status = domain.StepPending
	s.saveStep(target)
	r := s.runs[c.RunID]
	if r.Status == domain.RunWaitingInput {
		r.Status = domain.RunQueued
	}
	r.Version++
	s.saveRun(r)
	s.appendEvent(r.TenantID, r.ID, target.ID, "", "flow.input_provided", "user", c.ActorID, map[string]string{"flow_node": target.FlowNodeID})
	return "", nil
}

func (s *Store) ListPendingApprovals(ctx context.Context, tenantID string, limit int) ([]pg.PendingApproval, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []pg.PendingApproval
	for _, a := range s.approvals {
		if a.TenantID != tenantID || a.Request.Status != "PENDING" || !a.Request.ExpiresAt.After(now()) {
			continue
		}
		r := s.runs[a.RunID]
		p := pg.PendingApproval{ApprovalRequest: a.Request, RunID: a.RunID, InitiatorID: r.InitiatorID}
		if r.SessionID != nil {
			p.SessionID = r.SessionID
			if sess, ok := s.sessions[*r.SessionID]; ok {
				p.SessionTitle = sess.Title
			}
		} else if r.TurnID != nil {
			if turn, ok := s.turns[*r.TurnID]; ok {
				p.SessionID = &turn.SessionID
				if sess, ok := s.sessions[turn.SessionID]; ok {
					p.SessionTitle = sess.Title
				}
			}
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// RunnableRuns returns run ids that the local scheduler should try to claim.
func (s *Store) RunnableRuns() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for id, r := range s.runs {
		if r.Status == domain.RunQueued {
			out = append(out, id)
			continue
		}
		if r.Status == domain.RunWaitingChildren {
			ready := true
			for _, n := range s.flows[id].Nodes {
				if n.Status == "PENDING" || n.Status == "RUNNING" {
					ready = false
					break
				}
			}
			if ready {
				out = append(out, id)
			}
		}
	}
	sort.Strings(out)
	return out
}

func (s *Store) FailRun(ctx context.Context, tenantID, runID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return domain.ErrNotFound
	}
	r.Status = domain.RunFailed
	r.FailureReason = &reason
	r.Version++
	s.saveRun(r)
	s.appendEvent(r.TenantID, r.ID, "", "", "run.failed", "system", "", map[string]string{"reason": reason})
	return nil
}

func (s *Store) ExpireApprovals(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, a := range s.approvals {
		if a.Request.Status != "PENDING" || a.Request.ExpiresAt.After(now()) {
			continue
		}
		a.Request.Status = "EXPIRED"
		s.saveApproval(a)
		s.appendEvent(a.TenantID, a.RunID, a.StepID, "", "approval.expired", "system", "", map[string]string{"approval_request_id": id})
		r := s.runs[a.RunID]
		if st, ok := s.steps[a.StepID]; ok && st.Status == domain.StepWaitingApproval && !r.Status.Terminal() {
			// The blocked step is skipped and the agent re-plans; a single
			// unanswered approval must not strand the whole run.
			if err := s.skipBlockedStepLocked(&r, a.StepID, "approval expired"); err != nil {
				return n, err
			}
		}
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	return n, nil
}

func (s *Store) RecoverExpiredLeases(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, r := range s.runs {
		if r.Status != domain.RunRunning || r.LeaseUntil == nil || r.LeaseUntil.After(now()) {
			continue
		}
		s.recoverExpiredLocked(&r)
		cur := s.runs[id]
		switch cur.DesiredState {
		case domain.DesiredCancelled:
			if !cur.Status.Terminal() {
				s.cancelRunLocked(&cur, "system", "", "cancel requested")
			}
		case domain.DesiredPaused:
			if cur.Status == domain.RunQueued {
				cur.Status = domain.RunPaused
				cur.Version++
				s.saveRun(cur)
				s.appendEvent(cur.TenantID, cur.ID, "", "", "run.paused", "system", "", nil)
			}
		}
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	return n, nil
}

func (s *Store) RewakeStaleQueued(ctx context.Context, idle time.Duration, limit int) (int, error) {
	// The local scheduler polls RunnableRuns every 100ms; a queued run is
	// never waiting on a broker wake-up.
	return 0, nil
}

func (s *Store) ActivateStalledBranches(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, br := range s.branches {
		if br.ActiveTurnID != nil {
			continue
		}
		var next *domain.Turn
		for _, t := range s.turns {
			if t.BranchID != id || t.Status != domain.TurnQueued {
				continue
			}
			if next == nil || t.Seq < next.Seq {
				cp := t
				next = &cp
			}
		}
		if next == nil {
			continue
		}
		sess, ok := s.sessions[next.SessionID]
		if !ok {
			continue
		}
		if err := s.activateTurnLocked(sess, id, next.ID, deref(next.RunID)); err != nil {
			return n, err
		}
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	return n, nil
}

func (s *Store) ClaimOutbox(ctx context.Context, limit int, claimFor time.Duration) ([]domain.OutboxMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimOutbox(limit), nil
}
func (s *Store) MarkPublished(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteOutbox(id)
	return nil
}
func (s *Store) MarkPublishFailed(ctx context.Context, id string, backoff time.Duration) error {
	return nil
}

func (s *Store) ListStreamSegments(ctx context.Context, tenantID, runID string) ([]pg.SegmentView, error) {
	return nil, nil
}
func (s *Store) EventSinkStatuses(ctx context.Context) ([]pg.EventSinkStatus, error) { return nil, nil }

// ListRunsFor is ListRuns limited to what userID may see: runs no user
// started and the runs that user started.
func (s *Store) ListRunsFor(ctx context.Context, tenantID string, status domain.RunStatus, limit int, userID string) ([]domain.Run, error) {
	all, err := s.ListRuns(ctx, tenantID, status, 0)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, r := range all {
		if domain.SharedInitiator(r.InitiatorType) || (r.InitiatorID != nil && *r.InitiatorID == userID) {
			out = append(out, r)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
