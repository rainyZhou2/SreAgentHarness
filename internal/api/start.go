package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
)

// StartRunRequest creates a run on behalf of an API caller or an inbound
// trigger. The goal artifact pins the run context (time, window, services).
type StartRunRequest struct {
	TenantID       string
	ActorID        string
	InitiatorType  string // user | trigger | schedule
	Goal           string
	Context        domain.GoalContext
	ProfileID      string
	ProfileVersion int64
	IncidentID     *string
	IdempotencyKey string
	WorkOwner      *domain.WorkRunOwner
}

// ErrArtifactUnavailable is returned when the goal artifact cannot be stored.
var ErrArtifactUnavailable = errors.New("artifact store unavailable")

// goalDoc is the goal artifact format shared with the kernel.
type goalDoc struct {
	Goal    string              `json:"goal"`
	Context *domain.GoalContext `json:"context,omitempty"`
}

// StartRun resolves the profile version (channel/canary), stores the goal
// artifact and creates the run idempotently.
func (s *Server) StartRun(ctx context.Context, req StartRunRequest) (domain.Run, bool, error) {
	if strings.TrimSpace(req.Goal) == "" {
		return domain.Run{}, false, &domain.RejectedError{Reason: "goal_required"}
	}
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 200 {
		return domain.Run{}, false, &domain.RejectedError{Reason: "idempotency_key_required"}
	}
	var rollout *string
	if req.ProfileID == "" {
		req.ProfileID, req.ProfileVersion = s.DefaultProfile, s.DefaultVersion
	}
	if req.ProfileVersion == 0 && s.ResolveProfile != nil {
		req.ProfileVersion, rollout = s.ResolveProfile(ctx, req.TenantID, req.ProfileID, req.IdempotencyKey)
	}
	if req.ProfileVersion == 0 {
		req.ProfileVersion = s.DefaultVersion
	}
	if req.InitiatorType == "" {
		req.InitiatorType = "user"
	}
	gc := req.Context
	if gc.ReceivedAt.IsZero() {
		gc.ReceivedAt = time.Now().UTC()
	}
	gc.Services = domain.NormalizeServices(gc.Services)
	data, _ := json.Marshal(goalDoc{Goal: req.Goal, Context: &gc})
	if s.Artifacts == nil {
		return domain.Run{}, false, ErrArtifactUnavailable
	}
	ref, hash, err := s.Artifacts.Put(ctx, req.TenantID, "goal", data)
	if err != nil {
		return domain.Run{}, false, ErrArtifactUnavailable
	}
	return s.Store.CreateRun(ctx, domain.CreateRunRequest{
		TenantID: req.TenantID, IncidentID: req.IncidentID, ProfileID: req.ProfileID, ProfileVersion: req.ProfileVersion,
		GoalRef: ref, GoalHash: hash, IdempotencyKey: req.IdempotencyKey, InitiatorType: req.InitiatorType,
		InitiatorID: req.ActorID, RolloutID: rollout, WorkOwner: req.WorkOwner, PendingActivation: req.WorkOwner != nil,
	})
}

// GetRun reads a run (used by triggers to follow up on deduplicated events).
func (s *Server) GetRun(ctx context.Context, tenantID, runID string) (domain.Run, error) {
	return s.Store.GetRun(ctx, tenantID, runID)
}

// InjectEvidence attaches operator or trigger data to a run. The command is
// idempotent per key, so a redelivered event does not duplicate evidence.
func (s *Server) InjectEvidence(ctx context.Context, tenantID, runID, actorID, source, content, key string) error {
	payload, _ := json.Marshal(map[string]string{"content": content, "source": source})
	_, err := s.Store.ApplyCommand(ctx, tenantID, runID, domain.CommandRequest{Type: domain.CmdInjectEvidence,
		IdempotencyKey: key, ActorID: actorID, Payload: payload})
	return err
}
