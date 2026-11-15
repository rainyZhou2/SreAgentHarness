package orchestration

import (
	"context"
	"fmt"
	"strings"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/work"
)

// LinkCauseRequest backfills the immutable cause of an existing Incident.
type LinkCauseRequest struct {
	TenantID        string `json:"-"`
	WorkID          string `json:"-"`
	ActorID         string `json:"-"`
	IdempotencyKey  string `json:"-"`
	ExpectedVersion int64  `json:"-"`
	IncidentID      string `json:"incident_id"`
	ActionID        string `json:"action_id"`
	Reason          string `json:"reason"`
}

// LinkIncidentCause adds a missing cause without changing Incident lifecycle state.
func (s *Service) LinkIncidentCause(ctx context.Context, req LinkCauseRequest) (work.Work, bool, error) {
	req.Reason = strings.TrimSpace(req.Reason)
	if req.TenantID == "" || req.WorkID == "" || req.ActorID == "" {
		return work.Work{}, false, fmt.Errorf("%w: tenant, work and actor are required", work.ErrInvalid)
	}
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 200 {
		return work.Work{}, false, fmt.Errorf("%w: idempotency key is required (max 200 chars)", work.ErrInvalid)
	}
	if req.ExpectedVersion <= 0 {
		return work.Work{}, false, fmt.Errorf("%w: expected Work version must be positive", work.ErrInvalid)
	}
	if strings.TrimSpace(req.IncidentID) == "" || strings.TrimSpace(req.ActionID) == "" {
		return work.Work{}, false, fmt.Errorf("%w: incident_id and action_id are required", work.ErrInvalid)
	}
	if req.Reason == "" || len(req.Reason) > 500 {
		return work.Work{}, false, fmt.Errorf("%w: reason is required (max 500 chars)", work.ErrInvalid)
	}
	reqHash := cpkit.MustHash(struct {
		WorkID, IncidentID, ActionID, Reason string
		ExpectedVersion                      int64
	}{req.WorkID, req.IncidentID, req.ActionID, req.Reason, req.ExpectedVersion})
	if prev, hash, ok, err := s.Store.CommandResult(ctx, req.TenantID, req.WorkID, req.IdempotencyKey); err != nil {
		return work.Work{}, false, err
	} else if ok {
		if hash != reqHash {
			return work.Work{}, false, work.ErrIdempotencyConflict
		}
		return prev, false, nil
	}
	target, err := s.Store.GetWork(ctx, req.TenantID, req.WorkID)
	if err != nil {
		return work.Work{}, false, err
	}
	if target.Version != req.ExpectedVersion {
		return work.Work{}, false, fmt.Errorf("%w: expected %d, current %d", work.ErrVersionConflict, req.ExpectedVersion, target.Version)
	}
	if target.Kind != work.KindIncident {
		return work.Work{}, false, fmt.Errorf("%w: only an Incident can have caused_by", work.ErrInvalid)
	}
	subjects, err := s.Store.ListSubjects(ctx, req.TenantID, req.WorkID)
	if err != nil {
		return work.Work{}, false, err
	}
	existing, err := work.CausalLinks(subjects)
	if err != nil {
		return work.Work{}, false, fmt.Errorf("stored causal links: %v", err)
	}
	if len(existing) == 2 {
		if existing[0].ID == req.IncidentID && existing[1].ID == req.ActionID {
			return target, false, nil
		}
		return work.Work{}, false, fmt.Errorf("%w: %s", work.ErrCauseConflict, target.ID)
	}
	if req.IncidentID == target.ID {
		return work.Work{}, false, fmt.Errorf("%w: an Incident cannot cause itself", work.ErrInvalid)
	}
	cause, err := s.validateCauseLinks(ctx, req.TenantID, target.Kind, target.Goal.SubjectRefs,
		CreateCause{IncidentID: req.IncidentID, ActionID: req.ActionID})
	if err != nil {
		return work.Work{}, false, err
	}
	m := work.Mutation{TenantID: req.TenantID, WorkID: target.ID, ExpectedVersion: req.ExpectedVersion,
		ActorType: "user", ActorID: req.ActorID, Now: s.Clock.Now(), Subjects: cause.Links,
		Command: &work.CommandRecord{Key: req.IdempotencyKey, Type: "link_incident_cause", RequestHash: reqHash},
		Events: []work.NewEvent{{Type: "work.incident_cause_linked", Payload: map[string]any{
			"incident_id": req.IncidentID, "action_id": req.ActionID, "mitigation_work_id": cause.Action.WorkID,
			"reason": req.Reason, "idempotency_key": req.IdempotencyKey}}}}
	updated, err := s.Store.ApplyWork(ctx, m)
	if err != nil {
		return work.Work{}, false, err
	}
	return updated, true, nil
}
