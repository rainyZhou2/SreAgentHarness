package localstore

import (
	"encoding/json"
	"fmt"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
)

func (s *Store) findStepLocked(runID, stepID string) (domain.Step, bool) {
	st, ok := s.steps[stepID]
	if !ok || st.RunID != runID {
		return domain.Step{}, false
	}
	return st, true
}

func (s *Store) stepIntentLocked(stepID string) *domain.ActionIntent {
	if id := s.intentStep[stepID]; id != "" {
		in := s.intents[id]
		return &in
	}
	return nil
}

func (s *Store) reasonStepLocked(r *domain.Run) domain.PlannedStep {
	hash := ""
	for _, st := range sortedSteps(s.steps, r.ID) {
		if st.InputRef == r.GoalRef {
			hash = st.InputHash
			break
		}
	}
	return domain.PlannedStep{Kind: domain.StepReason, Executor: "reasoner", InputRef: r.GoalRef,
		InputHash: hash, Status: domain.StepPending}
}

func (s *Store) appendReasonLocked(r *domain.Run) error {
	_, err := s.insertStepLocked(r, s.reasonStepLocked(r))
	return err
}

func (s *Store) supersedeApprovalsLocked(runID string) {
	for id, a := range s.approvals {
		if a.RunID == runID && a.Request.Status == "PENDING" {
			a.Request.Status = "SUPERSEDED"
			s.saveApproval(a)
			_ = id
		}
	}
}

func (s *Store) requestApprovalLocked(r *domain.Run, stepID string, in domain.ActionIntent, required int) {
	if required < 1 {
		required = 1
	}
	if in.ID == "" {
		in.ID = newID()
	}
	s.saveIntent(in)
	s.intentStep[stepID] = in.ID
	s.put("intentstep", stepID, r.ID, 0, map[string]string{"step_id": stepID, "intent_id": in.ID})
	req := domain.ApprovalRequest{ID: newID(), ActionIntentID: in.ID, StepID: stepID, ActionHash: in.ActionHash,
		RiskClass: in.RiskClass, ToolName: in.ToolName, Target: in.Target, RequiredApprovals: required,
		Status: "PENDING", ExpiresAt: now().Add(30 * time.Minute), CreatedAt: now()}
	s.saveApproval(approvalRec{Request: req, RunID: r.ID, StepID: stepID, IntentID: in.ID, TenantID: r.TenantID})
	s.appendEvent(r.TenantID, r.ID, stepID, "", "approval.requested", "system", "", map[string]any{
		"approval_request_id": req.ID, "action_hash": in.ActionHash, "risk_class": in.RiskClass,
		"tool": in.ToolName, "target": in.Target, "required_approvals": required})
}

func (s *Store) resolveUnknownLocked(c *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		StepID     string `json:"step_id"`
		Resolution string `json:"resolution"`
		Note       string `json:"note"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || p.StepID == "" {
		return "step_id_required", nil
	}
	r := s.runs[c.RunID]
	if r.Status != domain.RunNeedsHuman {
		return "invalid_state", nil
	}
	if p.Resolution != "succeeded" && p.Resolution != "failed" && p.Resolution != "retry" {
		return "invalid_resolution", nil
	}
	st, ok := s.findStepLocked(c.RunID, p.StepID)
	if !ok {
		return "step_not_found", nil
	}
	if st.Status != domain.StepUnknown {
		return "step_not_unknown", nil
	}
	switch p.Resolution {
	case "succeeded":
		st.Status = domain.StepSucceeded
	case "failed":
		st.Status = domain.StepFailed
	default:
		st.Status = domain.StepPending
	}
	reason := "resolved by human: " + p.Resolution + " " + p.Note
	st.StatusReason = &reason
	s.saveStep(st)
	effect := map[string]string{"succeeded": "CONFIRMED", "failed": "COMPENSATED", "retry": "PREPARED"}[p.Resolution]
	for _, a := range s.attempts {
		if a.StepID == p.StepID {
			if e, ok := s.effects[a.IdempotencyKey]; ok {
				e.State, e.LastCheckAt = effect, now()
				s.saveEffect(e)
			}
		}
	}
	s.appendEvent(r.TenantID, r.ID, p.StepID, "", "step.resolved", "user", req.ActorID,
		map[string]string{"command_id": c.ID, "resolution": p.Resolution, "note": p.Note})
	if p.Resolution == "retry" {
		r.Status = domain.RunQueued
		r.Version++
		s.saveRun(r)
		return "", nil
	}
	if err := s.appendReasonLocked(&r); err != nil {
		return "", err
	}
	r.Status = domain.RunQueued
	r.Version++
	s.saveRun(r)
	return "", nil
}

func (s *Store) retryStepLocked(c *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		StepID string `json:"step_id"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || p.StepID == "" {
		return "step_id_required", nil
	}
	r := s.runs[c.RunID]
	if r.Status != domain.RunNeedsHuman && r.Status != domain.RunPaused {
		return "invalid_state", nil
	}
	st, ok := s.findStepLocked(c.RunID, p.StepID)
	if !ok {
		return "step_not_found", nil
	}
	if st.Status != domain.StepFailed && st.Status != domain.StepCancelled {
		return "step_not_failed", nil
	}
	in := s.stepIntentLocked(st.ID)
	if in != nil && in.RiskClass.HasSideEffects() {
		return "side_effecting_step_requires_resolve_unknown", nil
	}
	s.appendEvent(r.TenantID, r.ID, st.ID, "", "step.retry_requested", "user", req.ActorID,
		map[string]string{"command_id": c.ID, "reason": req.Reason})
	plan := domain.PlannedStep{Kind: st.Kind, Executor: st.ExecutorName, InputRef: st.InputRef,
		InputHash: st.InputHash, Status: domain.StepPending, Intent: in}
	if r.Status == domain.RunPaused {
		_, err := s.insertStepLocked(&r, plan)
		return "", err
	}
	if _, err := s.insertStepLocked(&r, plan); err != nil {
		return "", err
	}
	r.Status = domain.RunQueued
	r.Version++
	s.saveRun(r)
	return "", nil
}

func (s *Store) overrideResultLocked(c *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		StepID              string `json:"step_id"`
		SideEffectsReverted bool   `json:"side_effects_reverted"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || p.StepID == "" {
		return "step_id_required", nil
	}
	if req.Internal == nil || req.Internal.OutputRef == "" {
		return "output_required", nil
	}
	if req.Reason == "" {
		return "reason_required", nil
	}
	r := s.runs[c.RunID]
	switch r.Status {
	case domain.RunPaused, domain.RunNeedsHuman, domain.RunWaitingApproval, domain.RunQueued:
	default:
		return "run_must_be_paused_or_waiting", nil
	}
	st, ok := s.findStepLocked(c.RunID, p.StepID)
	if !ok {
		return "step_not_found", nil
	}
	if st.Kind != domain.StepTool || (st.Status != domain.StepSucceeded && st.Status != domain.StepFailed) {
		return "only_finished_tool_steps_can_be_overridden", nil
	}
	if len(s.outputRevs[st.ID]) == 0 && st.OutputRef != nil {
		s.outputRevs[st.ID] = append(s.outputRevs[st.ID], pg.OutputRevision{Revision: 0,
			OutputRef: *st.OutputRef, Source: "EXECUTION"})
	}
	rev := len(s.outputRevs[st.ID])
	reason := req.Reason
	s.outputRevs[st.ID] = append(s.outputRevs[st.ID], pg.OutputRevision{Revision: rev,
		OutputRef: req.Internal.OutputRef, Source: "HUMAN_OVERRIDE", CommandID: &c.ID, Reason: &reason,
		SideEffectsReverted: p.SideEffectsReverted})
	s.put("outputrev", fmt.Sprintf("%s:%d", st.ID, rev), st.ID, int64(rev),
		map[string]any{"step_id": st.ID, "revision": s.outputRevs[st.ID][len(s.outputRevs[st.ID])-1]})
	st.Status = domain.StepSucceeded
	st.OutputRef = &req.Internal.OutputRef
	st.OutputHash = &req.Internal.OutputHash
	st.OutputRevision = rev
	note := "output overridden by " + req.ActorID + ": " + req.Reason
	st.StatusReason = &note
	s.saveStep(st)
	r.Version++
	s.saveRun(r)
	s.appendEvent(r.TenantID, r.ID, st.ID, "", "step.output_overridden", "user", req.ActorID,
		map[string]any{"command_id": c.ID, "revision": rev, "reason": req.Reason,
			"side_effects_reverted": p.SideEffectsReverted})
	return "", nil
}

func (s *Store) replaceInputLocked(c *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		StepID string `json:"step_id"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || p.StepID == "" {
		return "step_id_required", nil
	}
	in := req.Internal
	if in == nil || in.InputRef == "" {
		return "input_required", nil
	}
	r := s.runs[c.RunID]
	if r.Status != domain.RunWaitingApproval && r.Status != domain.RunPaused {
		return "invalid_state", nil
	}
	st, ok := s.findStepLocked(c.RunID, p.StepID)
	if !ok {
		return "step_not_found", nil
	}
	if st.Status != domain.StepWaitingApproval {
		return "step_not_waiting_approval", nil
	}
	st.InputRef, st.InputHash = in.InputRef, in.InputHash
	s.saveStep(st)
	s.supersedeApprovalsLocked(r.ID)
	s.appendEvent(r.TenantID, r.ID, st.ID, "", "step.input_replaced", "user", req.ActorID,
		map[string]any{"command_id": c.ID, "input_hash": in.InputHash, "reason": req.Reason,
			"policy": in.PolicyDecision})
	r.Version++
	if in.Denied != "" || in.Intent == nil {
		reason := in.Denied
		if reason == "" {
			reason = "replacement input has no valid intent"
		}
		st.Status = domain.StepSkipped
		st.StatusReason = &reason
		s.saveStep(st)
		if err := s.appendReasonLocked(&r); err != nil {
			return "", err
		}
		r.Status = domain.RunQueued
		s.saveRun(r)
		return "", nil
	}
	if in.RequiredApprovals == 0 {
		st.Status = domain.StepPending
		s.saveStep(st)
		if r.Status == domain.RunWaitingApproval {
			r.Status = domain.RunQueued
		}
		s.saveRun(r)
		return "", nil
	}
	s.requestApprovalLocked(&r, st.ID, *in.Intent, in.RequiredApprovals)
	r.Status = domain.RunWaitingApproval
	s.saveRun(r)
	return "", nil
}

func (s *Store) injectEvidenceLocked(c *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		Content string `json:"content"`
		Source  string `json:"source"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || p.Content == "" || p.Source == "" {
		return "evidence_requires_content_and_source", nil
	}
	r := s.runs[c.RunID]
	data, _ := json.Marshal(domain.Evidence{Content: p.Content, Source: p.Source, ActorID: req.ActorID, CreatedAt: now()})
	key := domain.KVKey{TenantID: r.TenantID, Scope: "run", OwnerID: r.ID, Name: "evidence/" + c.ID}
	cur := s.kv[kvKey(key)]
	cur.Key, cur.Data, cur.SchemaID, cur.Version = key, data, "evidence/v1", cur.Version+1
	s.saveKV(cur)
	s.appendEvent(r.TenantID, r.ID, "", "", "evidence.injected", "user", req.ActorID,
		map[string]string{"command_id": c.ID, "kv_key": key.Name, "source": p.Source})
	return "", nil
}

func (s *Store) migrateProfileLocked(c *domain.Command, req domain.CommandRequest) (string, error) {
	var p struct {
		ProfileVersion int64 `json:"profile_version"`
	}
	if json.Unmarshal(req.Payload, &p) != nil || p.ProfileVersion <= 0 {
		return "profile_version_required", nil
	}
	r := s.runs[c.RunID]
	if r.Status != domain.RunPaused && r.Status != domain.RunQueued && r.Status != domain.RunNeedsHuman {
		return "invalid_state", nil
	}
	for _, a := range s.approvals {
		if a.RunID == r.ID && a.Request.Status == "PENDING" {
			return "pending_approvals_must_be_resolved", nil
		}
	}
	prof, ok := s.profiles[profileKey(r.TenantID, r.ProfileID, p.ProfileVersion)]
	if !ok {
		return "unknown_profile_version", nil
	}
	if prof.Status != "ACTIVE" {
		return "profile_version_not_runnable", nil
	}
	if p.ProfileVersion == r.ProfileVersion {
		return "", nil
	}
	from := r.ProfileVersion
	r.ProfileVersion = p.ProfileVersion
	r.Version++
	s.saveRun(r)
	s.appendEvent(r.TenantID, r.ID, "", "", "run.profile_migrated", "user", req.ActorID, map[string]any{
		"command_id": c.ID, "from_version": from, "to_version": p.ProfileVersion, "reason": req.Reason})
	return "", nil
}
