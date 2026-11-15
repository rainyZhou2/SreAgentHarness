// Package incident projects SRE-specific state from the existing Work ledger.
package incident

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
)

// Store is the read-only ledger needed by the projector.
type Store interface {
	Children(ctx context.Context, tenantID, parentID string) ([]work.Work, error)
	ListOutcomes(ctx context.Context, tenantID, workID string, limit int) ([]outcome.Evaluation, error)
	ListHypotheses(ctx context.Context, tenantID, workID string) ([]evidence.Hypothesis, error)
	ListEvidence(ctx context.Context, tenantID, workID string, limit int) ([]evidence.Record, error)
	PostmortemsFor(ctx context.Context, tenantID, sourceWorkID string) ([]work.Work, error)
	ListSubjects(ctx context.Context, tenantID, workID string) ([]work.SubjectLink, error)
	SecondaryIncidentsFor(ctx context.Context, tenantID, sourceWorkID string) ([]work.SecondaryIncident, error)
}

// ChildView summarizes one direct child without creating another lifecycle.
type ChildView struct {
	WorkID     string        `json:"work_id"`
	Role       string        `json:"role"`
	Status     work.Status   `json:"status"`
	Generation int           `json:"generation"`
	Outcome    *OutcomeRef   `json:"outcome,omitempty"`
	Decisions  []DecisionRef `json:"decisions,omitempty"`
}

// Projection contains independent Incident dimensions at one read time.
type Projection struct {
	WorkID                      string                  `json:"work_id"`
	Generation                  int                     `json:"generation"`
	Recovery                    RecoveryView            `json:"recovery"`
	RCA                         RCAView                 `json:"rca"`
	Postmortem                  PostmortemView          `json:"postmortem"`
	CausedBy                    *CauseView              `json:"caused_by,omitempty"`
	SecondaryIncidents          []SecondaryIncidentView `json:"secondary_incidents"`
	SecondaryIncidentsTruncated bool                    `json:"secondary_incidents_truncated"`
	Children                    []ChildView             `json:"children"`
	ProjectedAt                 time.Time               `json:"projected_at"`
}

// Project reads the current ledger without changing Work execution state.
func Project(ctx context.Context, store Store, incident work.Work, now time.Time) (Projection, error) {
	if incident.Kind != work.KindIncident || now.IsZero() {
		return Projection{}, fmt.Errorf("%w: incident and projection time are required", work.ErrInvalid)
	}
	out := Projection{WorkID: incident.ID, Generation: incident.Generation, Children: []ChildView{},
		SecondaryIncidents: []SecondaryIncidentView{}, ProjectedAt: now.UTC()}
	children, err := store.Children(ctx, incident.TenantID, incident.ID)
	if err != nil {
		return Projection{}, fmt.Errorf("incident %s children: %w", incident.ID, err)
	}
	outcomes, err := store.ListOutcomes(ctx, incident.TenantID, incident.ID, 50)
	if err != nil {
		return Projection{}, fmt.Errorf("incident %s outcomes: %w", incident.ID, err)
	}
	for _, child := range children {
		role := child.Goal.Labels["child_key"]
		if role != "mitigation" && role != "rca" {
			continue
		}
		view := ChildView{WorkID: child.ID, Role: role, Status: child.Status, Generation: child.Generation}
		evals, err := store.ListOutcomes(ctx, child.TenantID, child.ID, 50)
		if err != nil {
			return Projection{}, fmt.Errorf("child %s outcomes: %w", child.ID, err)
		}
		if ev, ok := latestOutcome(evals, child); ok {
			ref := outcomeReference(ev)
			view.Outcome = &ref
		}
		out.Children = append(out.Children, view)
	}
	sort.Slice(out.Children, func(i, j int) bool {
		if out.Children[i].Role == out.Children[j].Role {
			return out.Children[i].WorkID < out.Children[j].WorkID
		}
		return out.Children[i].Role < out.Children[j].Role
	})
	out.Recovery = projectRecovery(incident, children, outcomes, out.Children)
	out.RCA, err = projectRCA(ctx, store, children, now.UTC())
	if err != nil {
		return Projection{}, err
	}
	postmortems, err := store.PostmortemsFor(ctx, incident.TenantID, incident.ID)
	if err != nil {
		return Projection{}, fmt.Errorf("incident %s postmortems: %w", incident.ID, err)
	}
	out.Postmortem, err = projectPostmortem(incident, postmortems)
	if err != nil {
		return Projection{}, err
	}
	out.CausedBy, out.SecondaryIncidents, out.SecondaryIncidentsTruncated, err = projectCausation(ctx, store, incident)
	if err != nil {
		return Projection{}, err
	}
	for i := range out.Children {
		if out.Children[i].Role == "rca" {
			out.Children[i].Decisions = decisionsForWork(out.RCA.Decisions, out.Children[i].WorkID)
		}
	}
	return out, nil
}

func decisionsForWork(decisions []DecisionRef, workID string) []DecisionRef {
	var out []DecisionRef
	for _, d := range decisions {
		if d.WorkID == workID {
			out = append(out, d)
		}
	}
	return out
}
