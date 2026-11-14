package cpsql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/work"
)

// Both compatibility writes and the Work transaction use the same SQL path.
type decisionWriter interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) writeDecisionBatch(ctx context.Context, tx *sql.Tx, w work.Work, m work.Mutation, now time.Time) (*work.NewEvent, error) {
	if len(m.Findings) == 0 && len(m.Hypotheses) == 0 {
		return nil, nil
	}
	if m.Generation != nil && *m.Generation != w.Generation {
		return nil, fmt.Errorf("%w: a decision cannot change its Work generation", work.ErrInvalid)
	}
	if err := evidence.ValidateDecisionBatch(m.Hypotheses); err != nil {
		return nil, err
	}
	scope := evidence.DecisionScope{TenantID: w.TenantID, WorkID: w.ID, Generation: w.Generation}
	for _, f := range m.Findings {
		if f.TenantID != scope.TenantID || f.WorkID != scope.WorkID || f.Generation != scope.Generation {
			return nil, fmt.Errorf("%w: finding belongs to another Work generation", evidence.ErrInvalid)
		}
		if _, _, err := s.addFinding(ctx, tx, w.TenantID, w.ID, f); err != nil {
			return nil, err
		}
	}
	decisions := make([]evidence.DecisionVerdict, 0, len(m.Hypotheses))
	for _, h := range m.Hypotheses {
		if h.Key == "" || h.TenantID != scope.TenantID || h.WorkID != scope.WorkID || !h.Decision.CurrentFor(scope, h.Key) {
			return nil, fmt.Errorf("%w: hypothesis lacks a current adjudicated decision", evidence.ErrInvalid)
		}
		// The adjudicator's verdict, never raw reference counts or the caller's
		// hypothesis status, defines the compatibility status column.
		h.Status = h.Decision.HypothesisStatus()
		h.UpdatedAt = now
		if _, err := s.upsertHypothesis(ctx, tx, h, h.Version); err != nil {
			return nil, err
		}
		decisions = append(decisions, *h.Decision)
	}
	return &work.NewEvent{Type: "work.decision_adjudicated", Payload: map[string]any{
		"generation": w.Generation, "findings": len(m.Findings), "decisions": decisions,
	}}, nil
}
