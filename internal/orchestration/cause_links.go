package orchestration

import (
	"context"
	"fmt"
	"strings"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/work"
)

type validatedCause struct {
	Links  []work.SubjectLink
	Action action.Record
	Source work.Work
}

func (s *Service) validateCauseLinks(ctx context.Context, tenantID string, kind work.Kind, subjectRefs []string, cause CreateCause) (validatedCause, error) {
	if kind != work.KindIncident {
		return validatedCause{}, fmt.Errorf("%w: only an Incident can have caused_by", work.ErrInvalid)
	}
	if strings.TrimSpace(cause.IncidentID) == "" || strings.TrimSpace(cause.ActionID) == "" {
		return validatedCause{}, fmt.Errorf("%w: caused_by incident_id and action_id are required", work.ErrInvalid)
	}
	source, err := s.Store.GetWork(ctx, tenantID, cause.IncidentID)
	if err != nil {
		return validatedCause{}, err
	}
	if source.Kind != work.KindIncident {
		return validatedCause{}, fmt.Errorf("%w: caused_by source must be an Incident", work.ErrInvalid)
	}
	act, err := s.Store.GetAction(ctx, tenantID, cause.ActionID)
	if err != nil {
		return validatedCause{}, err
	}
	if !act.MayHaveDispatched {
		return validatedCause{}, fmt.Errorf("%w: caused_by action has not dispatched", work.ErrInvalid)
	}
	children, err := s.Store.Children(ctx, tenantID, source.ID)
	if err != nil {
		return validatedCause{}, err
	}
	mitigation := false
	for _, child := range children {
		if child.ID == act.WorkID && child.ParentID == source.ID && child.Goal.Labels["child_key"] == "mitigation" {
			mitigation = true
			break
		}
	}
	if !mitigation {
		return validatedCause{}, fmt.Errorf("%w: caused_by action must belong to a direct mitigation child", work.ErrInvalid)
	}
	matchesTarget := false
	for _, ref := range subjectRefs {
		if ref == act.TargetRef {
			matchesTarget = true
			break
		}
	}
	if !matchesTarget {
		return validatedCause{}, fmt.Errorf("%w: caused_by action target must be an Incident subject", work.ErrInvalid)
	}
	return validatedCause{Links: []work.SubjectLink{
		{Kind: work.SubjectCausedByWork, ID: source.ID},
		{Kind: work.SubjectCausedByAction, ID: act.ID},
	}, Action: act, Source: source}, nil
}
