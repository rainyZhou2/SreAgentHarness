package incident

import (
	"context"
	"fmt"
	"time"

	"go-sre-harness/internal/work"
)

const RelationMitigationCaused = "mitigation_caused"

// CauseView identifies the explicit mitigation cause of an Incident.
type CauseView struct {
	IncidentID string `json:"incident_id"`
	ActionID   string `json:"action_id"`
	Relation   string `json:"relation"`
}

// SecondaryIncidentView summarizes an Incident explicitly caused by a mitigation.
type SecondaryIncidentView struct {
	WorkID           string      `json:"work_id"`
	Status           work.Status `json:"status"`
	CreatedAt        time.Time   `json:"created_at"`
	MitigationWorkID string      `json:"mitigation_work_id"`
	ActionID         string      `json:"action_id"`
	Relation         string      `json:"relation"`
}

func projectCausation(ctx context.Context, store Store, source work.Work) (*CauseView, []SecondaryIncidentView, bool, error) {
	links, err := store.ListSubjects(ctx, source.TenantID, source.ID)
	if err != nil {
		return nil, nil, false, fmt.Errorf("incident %s causal subjects: %w", source.ID, err)
	}
	causal, err := work.CausalLinks(links)
	if err != nil {
		return nil, nil, false, fmt.Errorf("incident %s causal subjects: %w", source.ID, err)
	}
	var causedBy *CauseView
	if len(causal) == 2 {
		causedBy = &CauseView{IncidentID: causal[0].ID, ActionID: causal[1].ID, Relation: RelationMitigationCaused}
	}
	records, err := store.SecondaryIncidentsFor(ctx, source.TenantID, source.ID)
	if err != nil {
		return nil, nil, false, fmt.Errorf("incident %s secondary incidents: %w", source.ID, err)
	}
	truncated := len(records) > 100
	if truncated {
		records = records[:100]
	}
	secondary := make([]SecondaryIncidentView, 0, len(records))
	for _, record := range records {
		secondary = append(secondary, SecondaryIncidentView{WorkID: record.Work.ID, Status: record.Work.Status,
			CreatedAt: record.Work.CreatedAt.UTC(), MitigationWorkID: record.MitigationWorkID,
			ActionID: record.ActionID, Relation: RelationMitigationCaused})
	}
	return causedBy, secondary, truncated, nil
}
