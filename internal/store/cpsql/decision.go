package cpsql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go-sre-harness/internal/cpkit"
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
	ledger, err := s.decisionEvidence(ctx, tx, w)
	if err != nil {
		return nil, err
	}
	expectedFindings := map[string]bool{}
	for _, h := range m.Hypotheses {
		if h.Key == "" || h.TenantID != scope.TenantID || h.WorkID != scope.WorkID || h.Decision == nil || h.Decision.DecisionScope != scope {
			return nil, fmt.Errorf("%w: hypothesis lacks a current adjudicated decision", evidence.ErrInvalid)
		}
		findings, err := evidence.ValidateAdjudication(h, ledger)
		if err != nil {
			return nil, err
		}
		for _, f := range findings {
			hash, err := findingDigest(f)
			if err != nil {
				return nil, err
			}
			expectedFindings[hash] = true
		}
	}
	for _, f := range m.Findings {
		if f.TenantID != scope.TenantID || f.WorkID != scope.WorkID || f.Generation != scope.Generation {
			return nil, fmt.Errorf("%w: finding belongs to another Work generation", evidence.ErrInvalid)
		}
		hash, err := findingDigest(f)
		if err != nil || !expectedFindings[hash] {
			return nil, fmt.Errorf("%w: finding differs from adjudicated input", evidence.ErrInvalid)
		}
		delete(expectedFindings, hash)
		if _, _, err := s.addFinding(ctx, tx, w.TenantID, w.ID, f); err != nil {
			return nil, err
		}
	}
	if len(expectedFindings) != 0 {
		return nil, fmt.Errorf("%w: adjudicated findings missing from decision batch", evidence.ErrInvalid)
	}
	decisions := make([]evidence.DecisionVerdict, 0, len(m.Hypotheses))
	for _, h := range m.Hypotheses {
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

func findingDigest(f evidence.Finding) (string, error) {
	f.ID = ""
	return cpkit.Hash(f)
}

func (s *Store) decisionEvidence(ctx context.Context, tx *sql.Tx, w work.Work) ([]evidence.Record, error) {
	rows, err := tx.QueryContext(ctx, s.q(`SELECT record FROM work_evidence WHERE tenant_id=? AND work_id=?
		ORDER BY captured_at, id LIMIT 1000`), w.TenantID, w.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ledger []evidence.Record
	for rows.Next() {
		r, err := scanEvidence(rows)
		if err != nil {
			return nil, err
		}
		ledger = append(ledger, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ledger) >= 1000 {
		return nil, fmt.Errorf("%w: evidence ledger reaches 1000 records", evidence.ErrDecisionLimit)
	}
	return ledger, nil
}
