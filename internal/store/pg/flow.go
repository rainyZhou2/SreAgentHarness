package pg

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
)

// FlowView is the read model for one coordinator run.
type FlowView = domain.FlowSnapshot

// FlowNodeView is one node execution attempt.
type FlowNodeView = domain.FlowNodeRun

// LoadFlow returns the flow state and every node attempt of a run.
func (s *Store) LoadFlow(ctx context.Context, tenantID, runID string) (domain.FlowSnapshot, error) {
	var v domain.FlowSnapshot
	var state []byte
	err := s.pool.QueryRow(ctx, `SELECT id, run_id, compiled_ref, compiled_hash, state, status,
		current_node, depth, child_runs, generation FROM flow_runs WHERE tenant_id=$1 AND run_id=$2`,
		tenantID, runID).Scan(&v.ID, &v.RunID, &v.CompiledRef, &v.CompiledHash, &state,
		&v.Status, &v.CurrentNode, &v.Depth, &v.ChildRuns, &v.Generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, domain.ErrNotFound
	}
	if err != nil {
		return v, err
	}
	v.State = state
	rows, err := s.pool.Query(ctx, `SELECT node_id, node_type, attempt, COALESCE(child_run_id::text, ''),
		status, COALESCE(output_ref, ''), COALESCE(output_hash, ''), COALESCE(error, ''), created_at
		FROM flow_node_runs WHERE tenant_id=$1 AND flow_run_id=$2 AND generation=$3
		ORDER BY created_at, node_id, attempt`,
		tenantID, v.ID, v.Generation)
	if err != nil {
		return v, err
	}
	defer rows.Close()
	for rows.Next() {
		var n domain.FlowNodeRun
		if err := rows.Scan(&n.NodeID, &n.NodeType, &n.Attempt, &n.ChildRunID, &n.Status,
			&n.OutputRef, &n.OutputHash, &n.Error, &n.CreatedAt); err != nil {
			return v, err
		}
		v.Nodes = append(v.Nodes, n)
	}
	return v, rows.Err()
}

// FlowInput returns the value provided for a waiting input step.
func (s *Store) FlowInput(ctx context.Context, tenantID, stepID string) (json.RawMessage, bool, error) {
	var v []byte
	err := s.pool.QueryRow(ctx, `SELECT value FROM flow_inputs WHERE tenant_id=$1 AND step_id=$2`,
		tenantID, stepID).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}
