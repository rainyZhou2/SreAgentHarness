package cpsql

import (
	"context"
	"encoding/json"
	"fmt"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/evidence"
)

var _ evidence.FindingStore = (*Store)(nil)

// AddFinding records an agent finding; the same content for the same Work
// is returned instead of duplicated.
func (s *Store) AddFinding(ctx context.Context, tenantID, workID string, f evidence.Finding) (evidence.Finding, bool, error) {
	return s.addFinding(ctx, s.db, tenantID, workID, f)
}

func (s *Store) addFinding(ctx context.Context, db decisionWriter, tenantID, workID string, f evidence.Finding) (evidence.Finding, bool, error) {
	if !cpkit.ValidID(workID) || !cpkit.ValidID(f.RunID) || f.Role == "" || f.Statement == "" {
		return f, false, fmt.Errorf("finding needs a work, a run, a role and a statement")
	}
	f.TenantID, f.WorkID = tenantID, workID
	if f.ID == "" {
		f.ID = cpkit.NewID()
	}
	if f.CreatedAt.IsZero() {
		f.CreatedAt = s.now()
	}
	hash := f.PayloadHash()
	refs := append(append([]string{}, f.Supports...), f.Refutes...)
	res, err := db.ExecContext(ctx, s.q(`INSERT INTO work_findings (tenant_id, work_id, finding_id, run_id, role, hypothesis_key,
		verdict, statement, evidence_refs, record, payload_hash, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT (tenant_id, work_id, payload_hash) DO NOTHING`), tenantID, workID, f.ID, f.RunID, f.Role,
		nullStr(f.HypothesisID), f.Verdict, f.Statement, js(refs), js(f), hash, s.t(f.CreatedAt))
	if err != nil {
		return f, false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		var body jb
		if err := db.QueryRowContext(ctx, s.q(`SELECT record FROM work_findings WHERE tenant_id=? AND work_id=? AND payload_hash=?`),
			tenantID, workID, hash).Scan(&body); err != nil {
			return f, false, err
		}
		var prev evidence.Finding
		return prev, false, json.Unmarshal(body, &prev)
	}
	return f, true, nil
}

// ListFindings lists a Work's findings, oldest first.
func (s *Store) ListFindings(ctx context.Context, tenantID, workID string, limit int) ([]evidence.Finding, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT record FROM work_findings WHERE tenant_id=? AND work_id=?
		ORDER BY created_at, finding_id LIMIT ?`), tenantID, workID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []evidence.Finding
	for rows.Next() {
		var body jb
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var f evidence.Finding
		if err := json.Unmarshal(body, &f); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
