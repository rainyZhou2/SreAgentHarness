package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-sre-harness/internal/auth"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
)

// ---------- sessions and turns (§29) ----------

const autoProfile = "auto"

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 200 {
		writeErr(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key header is required")
		return
	}
	var body struct {
		Title          string  `json:"title"`
		ProfileID      string  `json:"profile_id"`
		ProfileVersion int64   `json:"profile_version"`
		IncidentID     *string `json:"incident_id"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if body.ProfileID == "" {
		body.ProfileID = autoProfile
	}
	if body.ProfileID == autoProfile {
		body.ProfileVersion = 0
	} else if body.ProfileVersion == 0 {
		if body.ProfileID == s.DefaultProfile {
			body.ProfileVersion = s.DefaultVersion
		} else if s.ResolveProfile != nil {
			body.ProfileVersion, _ = s.ResolveProfile(r.Context(), id.tenant, body.ProfileID, key)
		}
	}
	sess, created, err := s.Store.CreateSession(r.Context(), domain.CreateSessionRequest{TenantID: id.tenant, ActorID: id.actor,
		ProfileID: body.ProfileID, ProfileVersion: body.ProfileVersion, Title: body.Title, IncidentID: body.IncidentID,
		IdempotencyKey: userScopedKey(id.actor, key)})
	if err != nil {
		s.domainErr(w, err)
		return
	}
	code := http.StatusCreated
	if !created {
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]any{"session": sess, "created": created})
}

// listSessions returns the caller's tenant sessions, most recently used first.
func (s *Server) listSessions(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	f := pg.SessionFilter{Limit: 50, Query: strings.TrimSpace(q.Get("q"))}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			writeErr(w, http.StatusBadRequest, "invalid_limit", "limit must be 1..200")
			return
		}
		f.Limit = n
	}
	switch strings.ToUpper(q.Get("status")) {
	case "", "ACTIVE":
	case "ARCHIVED":
		f.Status = "ARCHIVED"
	default:
		writeErr(w, http.StatusBadRequest, "invalid_status", "status must be active or archived")
		return
	}
	if len(f.Query) > 200 {
		writeErr(w, http.StatusBadRequest, "invalid_query", "q is at most 200 characters")
		return
	}
	if p, _ := auth.FromContext(r.Context()); !isAdmin(p) {
		f.CreatedBy = id.actor // a user lists their own chats
	}
	list, err := s.Store.ListSessions(r.Context(), id.tenant, f)
	if err != nil {
		s.domainErr(w, err)
		return
	}
	if list == nil {
		list = []pg.SessionSummary{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": list})
}

// updateSession renames a session or archives, restores or deletes it.
func (s *Server) updateSession(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var body struct {
		Title  *string `json:"title"`
		Status *string `json:"status"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	if body.Title != nil {
		t := strings.TrimSpace(*body.Title)
		if t == "" || len(t) > 200 {
			writeErr(w, http.StatusBadRequest, "invalid_title", "title must be 1..200 characters")
			return
		}
		body.Title = &t
	}
	if body.Status != nil {
		st := strings.ToUpper(*body.Status)
		if st != "ACTIVE" && st != "ARCHIVED" && st != "DELETED" {
			writeErr(w, http.StatusBadRequest, "invalid_status", "status must be ACTIVE, ARCHIVED or DELETED")
			return
		}
		body.Status = &st
	}
	if body.Title == nil && body.Status == nil {
		writeErr(w, http.StatusBadRequest, "invalid_body", "set title and/or status")
		return
	}
	sid := r.PathValue("session_id")
	sess, err := s.Store.UpdateSession(r.Context(), id.tenant, sid, body.Title, body.Status)
	if err != nil {
		s.domainErr(w, err)
		return
	}
	details := map[string]any{}
	if body.Status != nil {
		details["status"] = *body.Status
	}
	if body.Title != nil {
		details["title"] = *body.Title
	}
	s.audit(r.Context(), pg.AuditEntry{TenantID: id.tenant, ActorType: "user", ActorID: id.actor, Action: "session.update",
		ResourceType: "session", ResourceID: sid, Outcome: "applied", Details: details})
	writeJSON(w, http.StatusOK, map[string]any{"session": sess})
}

// listPendingApprovals returns the tenant's approvals waiting for a decision.
func (s *Server) listPendingApprovals(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			writeErr(w, http.StatusBadRequest, "invalid_limit", "limit must be 1..200")
			return
		}
		limit = n
	}
	list, err := s.Store.ListPendingApprovals(r.Context(), id.tenant, limit)
	if err != nil {
		s.domainErr(w, err)
		return
	}
	if p, _ := auth.FromContext(r.Context()); !p.Has(auth.Approver) {
		own := make([]pg.PendingApproval, 0, len(list))
		for _, a := range list {
			if a.InitiatorID != nil && *a.InitiatorID == id.actor {
				own = append(own, a)
			}
		}
		list = own
	}
	if list == nil {
		list = []pg.PendingApproval{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"approvals": list})
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	view, err := s.Store.ResumeSession(r.Context(), id.tenant, r.PathValue("session_id"))
	if err != nil {
		s.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

type submitTurnBody struct {
	Text            string `json:"text"`
	ClientRequestID string `json:"client_request_id"`
	BranchID        string `json:"branch_id"`
	ExpectedVersion *int64 `json:"expected_branch_version"`
}

func (s *Server) submitTurn(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var body submitTurnBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10)).Decode(&body); err != nil || strings.TrimSpace(body.Text) == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "text is required")
		return
	}
	if body.ClientRequestID == "" {
		body.ClientRequestID = r.Header.Get("Idempotency-Key")
	}
	if body.ClientRequestID == "" || len(body.ClientRequestID) > 200 {
		writeErr(w, http.StatusBadRequest, "client_request_id_required", "client_request_id (or Idempotency-Key) is required")
		return
	}
	sessionID := r.PathValue("session_id")
	// The turn input pins the time the turn arrived: prompts use it as "now".
	data, _ := json.Marshal(goalDoc{Goal: body.Text, Context: &domain.GoalContext{ReceivedAt: time.Now().UTC(), Source: "session"}})
	ref, hash, err := s.Artifacts.Put(r.Context(), id.tenant, "turn-input", data)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "artifact_store_unavailable", "retry later")
		return
	}
	sess, err := s.Store.GetSession(r.Context(), id.tenant, sessionID)
	if err != nil {
		s.domainErr(w, err)
		return
	}
	profileID, version := sess.ProfileID, sess.ProfileVersion
	var rollout *string
	var initialSteps []domain.PlannedStep
	switch {
	case profileID == "" || profileID == autoProfile:
		// Auto sessions start with a durable planning step. The planner
		// decides between ReAct and a generated flow inside the run, so the
		// model call is leased, ledgered and auditable like any other step.
		profileID, version = s.AutoProfileID, s.AutoProfileVersion
		if profileID == "" {
			profileID, version = s.DefaultProfile, s.DefaultVersion
		}
		initialSteps = []domain.PlannedStep{{Kind: domain.StepPlan, Executor: "planner",
			InputRef: ref, InputHash: hash, Status: domain.StepPending}}
	case version == 0:
		version = s.DefaultVersion
		if s.ResolveProfile != nil {
			if v, r := s.ResolveProfile(r.Context(), id.tenant, profileID, sessionID); v > 0 {
				version, rollout = v, r
			}
		}
	}
	req := domain.SubmitTurnRequest{TenantID: id.tenant, ActorID: id.actor, SessionID: sessionID, BranchID: body.BranchID,
		ClientRequestID: body.ClientRequestID, InputRef: ref, InputHash: hash, ExpectedVersion: body.ExpectedVersion,
		ProfileID: profileID, ProfileVersion: version, RolloutID: rollout, InitialSteps: initialSteps}
	policy := s.BranchPolicy
	if policy == "" {
		policy = domain.BranchQueue
	}
	maxPending := s.MaxPendingTurn
	if maxPending == 0 {
		maxPending = 8
	}
	res, err := s.Store.SubmitTurn(r.Context(), req, policy, maxPending)
	if err != nil {
		s.domainErr(w, err)
		return
	}
	code := http.StatusAccepted
	if !res.Created {
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]any{"turn": res,
		"stream_url": fmt.Sprintf("/v1/runs/%s/events/stream?include_deltas=true", res.RunID)})
}

func (s *Server) listTurns(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	turns, err := s.Store.ListTurns(r.Context(), id.tenant, r.PathValue("session_id"), r.URL.Query().Get("branch_id"))
	if err != nil {
		s.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"turns": turns})
}

func (s *Server) getTurn(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	t, err := s.Store.GetTurn(r.Context(), id.tenant, r.PathValue("turn_id"))
	if err != nil || t.SessionID != r.PathValue("session_id") {
		s.domainErr(w, domain.ErrNotFound)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) forkBranch(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var body struct {
		ParentBranchID string `json:"parent_branch_id"`
		MessageCursor  *int64 `json:"message_cursor"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&body); err != nil || body.ParentBranchID == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "parent_branch_id is required")
		return
	}
	b, err := s.Store.ForkBranch(r.Context(), domain.ForkBranchRequest{TenantID: id.tenant, ActorID: id.actor,
		SessionID: r.PathValue("session_id"), ParentBranchID: body.ParentBranchID, MessageCursor: body.MessageCursor})
	if err != nil {
		s.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, b)
}

func (s *Server) branchMessages(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	upTo, _ := strconv.ParseInt(r.URL.Query().Get("up_to"), 10, 64)
	msgs, err := s.Store.BranchMessages(r.Context(), id.tenant, r.PathValue("branch_id"), upTo)
	if err != nil {
		s.domainErr(w, err)
		return
	}
	out := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		item := map[string]any{"message": m}
		if !strings.HasPrefix(m.ContentRef, "status:") {
			if data, err := s.Artifacts.Get(r.Context(), id.tenant, m.ContentRef); err == nil {
				item["content"] = json.RawMessage(data)
			}
		}
		out = append(out, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": out})
}

// domainErr maps domain errors onto HTTP status codes.
func (s *Server) domainErr(w http.ResponseWriter, err error) {
	var rej *domain.RejectedError
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, domain.ErrBranchBusy):
		writeErr(w, http.StatusConflict, "branch_busy", err.Error())
	case errors.Is(err, domain.ErrTurnQueueFull):
		w.Header().Set("Retry-After", "5")
		writeErr(w, http.StatusTooManyRequests, "turn_queue_full", err.Error())
	case errors.Is(err, domain.ErrQuotaExceeded):
		w.Header().Set("Retry-After", "30")
		writeErr(w, http.StatusTooManyRequests, "quota_exceeded", err.Error())
	case errors.Is(err, domain.ErrVersionConflict):
		writeErr(w, http.StatusConflict, "version_conflict", err.Error())
	case errors.Is(err, domain.ErrInvalidCursor):
		writeErr(w, http.StatusUnprocessableEntity, "invalid_cursor", err.Error())
	case errors.Is(err, domain.ErrForbidden), errors.Is(err, domain.ErrApproverConflict):
		writeErr(w, http.StatusForbidden, "forbidden", err.Error())
	case errors.As(err, &rej):
		writeErr(w, http.StatusUnprocessableEntity, rej.Reason, rej.Reason)
	default:
		s.storeErr(w, err)
	}
}

// ---------- model usage and cost (§34, §36.3) ----------

func (s *Server) listModelRequests(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	reqs, err := s.Store.ListModelRequests(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		s.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"model_requests": reqs})
}

func (s *Server) runCost(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	if _, err := s.Store.GetRun(r.Context(), id.tenant, r.PathValue("run_id")); err != nil {
		s.domainErr(w, err)
		return
	}
	c, err := s.Store.RunCost(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		s.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (s *Server) listSegments(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	segs, err := s.Store.ListStreamSegments(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		s.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"segments": segs})
}

func (s *Server) tenantUsage(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	since := time.Now().UTC().Truncate(24 * time.Hour)
	if v := r.URL.Query().Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_since", "since must be RFC3339")
			return
		}
		since = t
	}
	c, err := s.Store.TenantCost(r.Context(), id.tenant, since)
	if err != nil {
		s.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"since": since, "usage": c})
}
