package localstore

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
)

func (s *Store) CreateSession(ctx context.Context, req domain.CreateSessionRequest) (domain.Session, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key := req.TenantID + "/" + req.IdempotencyKey; req.IdempotencyKey != "" {
		if id, ok := s.sessionKey[key]; ok {
			return s.sessions[id], false, nil
		}
	}
	sess := domain.Session{ID: newID(), TenantID: req.TenantID, CreatedBy: req.ActorID, Title: strptr(req.Title),
		ProfileID: req.ProfileID, ProfileVersion: req.ProfileVersion, Status: "ACTIVE",
		DefaultBranchID: newID(), IncidentID: req.IncidentID, CreatedAt: now()}
	br := domain.Branch{ID: sess.DefaultBranchID, SessionID: sess.ID, Version: 1}
	s.saveSession(sess)
	s.saveBranch(br)
	if req.IdempotencyKey != "" {
		s.sessionKey[req.TenantID+"/"+req.IdempotencyKey] = sess.ID
	}
	return sess, true, nil
}

func (s *Store) GetSession(ctx context.Context, tenantID, sessionID string) (domain.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok || sess.TenantID != tenantID {
		return domain.Session{}, domain.ErrNotFound
	}
	return sess, nil
}

func (s *Store) ListSessions(ctx context.Context, tenantID string, f pg.SessionFilter) ([]pg.SessionSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.Status == "" {
		f.Status = "ACTIVE"
	}
	if f.Limit <= 0 {
		f.Limit = 50
	}
	var out []pg.SessionSummary
	for _, sess := range s.sessions {
		if sess.TenantID != tenantID || sess.Status != f.Status || (f.CreatedBy != "" && sess.CreatedBy != f.CreatedBy) {
			continue
		}
		if f.Query != "" && (sess.Title == nil || !strings.Contains(strings.ToLower(*sess.Title), strings.ToLower(f.Query))) {
			continue
		}
		out = append(out, s.sessionSummary(sess))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastActivityAt.After(out[j].LastActivityAt) })
	if len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

func (s *Store) UpdateSession(ctx context.Context, tenantID, id string, title, status *string) (domain.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[id]
	if !ok || sess.TenantID != tenantID {
		return domain.Session{}, domain.ErrNotFound
	}
	if title != nil {
		sess.Title = title
	}
	if status != nil {
		switch *status {
		case "ACTIVE", "ARCHIVED":
			sess.Status = *status
		case "DELETED", "DELETE":
			delete(s.sessions, id)
			s.del("session", id)
			return domain.Session{}, nil
		default:
			return domain.Session{}, domain.ErrNotFound
		}
	}
	s.saveSession(sess)
	return sess, nil
}

func (s *Store) ResumeSession(ctx context.Context, tenantID, sessionID string) (domain.SessionView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok || sess.TenantID != tenantID {
		return domain.SessionView{}, domain.ErrNotFound
	}
	view := domain.SessionView{Session: sess}
	for _, br := range s.branches {
		if br.SessionID == sessionID {
			view.Branches = append(view.Branches, br)
		}
	}
	sort.Slice(view.Branches, func(i, j int) bool { return view.Branches[i].ID < view.Branches[j].ID })
	for _, t := range s.turns {
		if t.SessionID != sessionID {
			continue
		}
		if t.Status == domain.TurnRunning {
			view.ActiveTurn = &t
		}
		if t.Status == domain.TurnQueued {
			view.Queued = append(view.Queued, t)
		}
	}
	sort.Slice(view.Queued, func(i, j int) bool { return view.Queued[i].Seq < view.Queued[j].Seq })
	view.Messages = sortedMessages(s.messages, sess.DefaultBranchID)
	return view, nil
}

func (s *Store) SubmitTurn(ctx context.Context, req domain.SubmitTurnRequest, policy domain.BranchPolicy, maxPending int) (domain.SubmittedTurn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[req.SessionID]
	if !ok || sess.TenantID != req.TenantID || sess.Status != "ACTIVE" {
		return domain.SubmittedTurn{}, domain.ErrNotFound
	}
	branchID := req.BranchID
	if branchID == "" {
		branchID = sess.DefaultBranchID
	}
	br, ok := s.branches[branchID]
	if !ok {
		return domain.SubmittedTurn{}, domain.ErrNotFound
	}
	for _, t := range s.turns {
		if t.BranchID == branchID && t.ClientRequestID == req.ClientRequestID {
			return domain.SubmittedTurn{TurnID: t.ID, RunID: deref(t.RunID), BranchID: branchID, Seq: t.Seq,
				BranchVersion: br.Version, Status: t.Status, Queued: t.Status == domain.TurnQueued}, nil
		}
	}
	if req.ExpectedVersion != nil && *req.ExpectedVersion != br.Version {
		return domain.SubmittedTurn{}, domain.ErrVersionConflict
	}
	pending := 0
	for _, t := range s.turns {
		if t.BranchID == branchID && t.Status == domain.TurnQueued {
			pending++
		}
	}
	busy := br.ActiveTurnID != nil
	if busy && policy == domain.BranchReject {
		return domain.SubmittedTurn{}, domain.ErrBranchBusy
	}
	if (busy || pending > 0) && pending >= maxPending {
		return domain.SubmittedTurn{}, domain.ErrTurnQueueFull
	}
	queued := busy || pending > 0
	turn := domain.Turn{ID: newID(), SessionID: sess.ID, BranchID: branchID, Seq: br.LastTurnSeq + 1,
		InputRef: req.InputRef, ClientRequestID: req.ClientRequestID, Status: domain.TurnQueued, CreatedAt: now()}
	s.saveTurn(turn)
	profileID, version := sess.ProfileID, sess.ProfileVersion
	if req.ProfileID != "" {
		profileID, version = req.ProfileID, req.ProfileVersion
	}
	run, _, err := s.createRunLocked(ctx, domain.CreateRunRequest{TenantID: req.TenantID, IncidentID: sess.IncidentID,
		ProfileID: profileID, ProfileVersion: version, GoalRef: req.InputRef, GoalHash: req.InputHash,
		IdempotencyKey: "turn:" + turn.ID, InitiatorType: "user", InitiatorID: req.ActorID,
		SessionID: &sess.ID, BranchID: &branchID, TurnID: &turn.ID, PendingActivation: true,
		RolloutID: req.RolloutID, InitialSteps: req.InitialSteps})
	if err != nil {
		return domain.SubmittedTurn{}, err
	}
	turn.RunID = &run.ID
	s.saveTurn(turn)
	br.LastTurnSeq = turn.Seq
	br.Version++
	s.saveBranch(br)
	status := domain.TurnQueued
	if !queued {
		if err := s.activateTurnLocked(sess, branchID, turn.ID, run.ID); err != nil {
			return domain.SubmittedTurn{}, err
		}
		status = domain.TurnRunning
	}
	br = s.branches[branchID]
	return domain.SubmittedTurn{TurnID: turn.ID, RunID: run.ID, BranchID: branchID, Seq: turn.Seq,
		BranchVersion: br.Version, Status: status, Queued: queued, Created: true}, nil
}

func (s *Store) activateTurnLocked(sess domain.Session, branchID, turnID, runID string) error {
	turn := s.turns[turnID]
	if turn.Status != domain.TurnQueued {
		return nil
	}
	br := s.branches[branchID]
	br.LastMessageSeq++
	br.ActiveTurnID = &turnID
	br.Version++
	s.saveBranch(br)
	msg := domain.Message{ID: newID(), BranchID: branchID, TurnID: turnID, Seq: br.LastMessageSeq, Role: "user",
		ContentRef: turn.InputRef, ContentHash: turn.InputRef, Kind: "text", CreatedAt: now()}
	s.saveMessage(msg)
	turn.Status = domain.TurnRunning
	turn.InputMessageCursor = &br.LastMessageSeq
	t := now()
	turn.ActivatedAt = &t
	s.saveTurn(turn)
	r := s.runs[runID]
	r.Status = domain.RunQueued
	r.Version++
	s.saveRun(r)
	s.appendEvent(r.TenantID, r.ID, "", "", "run.activated", "system", "", map[string]any{"turn_id": turnID,
		"message_cursor": br.LastMessageSeq})
	return nil
}

func (s *Store) finalizeTurnLocked(r domain.Run) {
	if r.TurnID == nil {
		return
	}
	turn, ok := s.turns[*r.TurnID]
	if !ok || (turn.Status != domain.TurnRunning && turn.Status != domain.TurnQueued) {
		return
	}
	br := s.branches[turn.BranchID]
	final := map[domain.RunStatus]domain.TurnStatus{domain.RunCompleted: domain.TurnCompleted,
		domain.RunFailed: domain.TurnFailed, domain.RunCancelled: domain.TurnCancelled}[r.Status]
	if turn.Status == domain.TurnQueued {
		turn.Status = final
		s.saveTurn(turn)
		return
	}
	ref, kind := "", "text"
	if r.Status == domain.RunCompleted && r.ResultRef != nil {
		ref = *r.ResultRef
	} else {
		kind = "status"
		ref = "status:" + string(r.Status)
	}
	br.LastMessageSeq++
	br.ActiveTurnID = nil
	br.Version++
	s.saveBranch(br)
	msg := domain.Message{ID: newID(), BranchID: turn.BranchID, TurnID: turn.ID, Seq: br.LastMessageSeq,
		Role: "assistant", ContentRef: ref, ContentHash: ref, Kind: kind, CreatedAt: now()}
	s.saveMessage(msg)
	turn.Status = final
	s.saveTurn(turn)
	s.appendEvent(r.TenantID, r.ID, "", "", "turn.finalized", "system", "", map[string]any{"turn_id": turn.ID,
		"status": final, "message_seq": br.LastMessageSeq})
	// Activate the next queued turn on this branch.
	for _, next := range s.turns {
		if next.BranchID == turn.BranchID && next.Status == domain.TurnQueued && next.Seq > turn.Seq {
			_ = s.activateTurnLocked(s.sessions[turn.SessionID], turn.BranchID, next.ID, deref(next.RunID))
			break
		}
	}
}

func (s *Store) ListTurns(ctx context.Context, tenantID, sessionID, branchID string) ([]domain.Turn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Turn
	for _, t := range s.turns {
		if t.SessionID == sessionID && (branchID == "" || t.BranchID == branchID) {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func (s *Store) GetTurn(ctx context.Context, tenantID, turnID string) (domain.Turn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.turns[turnID]
	if !ok {
		return domain.Turn{}, domain.ErrNotFound
	}
	return t, nil
}

func (s *Store) ForkBranch(ctx context.Context, req domain.ForkBranchRequest) (domain.Branch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[req.SessionID]
	if !ok || sess.TenantID != req.TenantID {
		return domain.Branch{}, domain.ErrNotFound
	}
	parent, ok := s.branches[req.ParentBranchID]
	if !ok || parent.SessionID != req.SessionID {
		return domain.Branch{}, domain.ErrNotFound
	}
	cursor := parent.LastMessageSeq
	if req.MessageCursor != nil {
		cursor = *req.MessageCursor
	}
	br := domain.Branch{ID: newID(), SessionID: req.SessionID, ParentBranchID: &parent.ID,
		ParentMessageCursor: &cursor, Version: 1}
	s.saveBranch(br)
	for _, m := range sortedMessages(s.messages, parent.ID) {
		if m.Seq > cursor {
			break
		}
		cp := m
		cp.ID = newID()
		cp.BranchID = br.ID
		s.saveMessage(cp)
		br.LastMessageSeq = cp.Seq
	}
	br.LastTurnSeq = parent.LastTurnSeq
	s.saveBranch(br)
	return br, nil
}

func (s *Store) BranchMessages(ctx context.Context, tenantID, branchID string, upTo int64) ([]domain.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Message
	for _, m := range sortedMessages(s.messages, branchID) {
		if upTo > 0 && m.Seq > upTo {
			break
		}
		out = append(out, m)
	}
	return out, nil
}

var _ = errors.Is
var _ = time.Now
