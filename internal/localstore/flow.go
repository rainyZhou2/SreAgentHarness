package localstore

import (
	"encoding/json"
	"fmt"
	"time"

	"go-sre-harness/internal/domain"
)

// applyFlowLocked persists the coordinator state and creates child runs. It
// mirrors the PostgreSQL flow commit but uses the local run map.
func (s *Store) applyFlowLocked(parent *domain.Run, lease domain.Lease, fc *domain.FlowCommit) error {
	if fc == nil {
		return nil
	}
	key := fc.FlowRunID
	if key == "" {
		key = parent.ID
	}
	fl, ok := s.flows[parent.ID]
	if !ok {
		fl = domain.FlowSnapshot{ID: key, RunID: parent.ID}
	}
	if fc.Generation < fl.Generation {
		return fmt.Errorf("flow commit for generation %d is older than the stored flow", fc.Generation)
	}
	if fc.Generation > fl.Generation {
		// A replan: the new graph starts with no node attempts. Child runs
		// of the replaced generation stay in the run tree for audit.
		fl.Generation, fl.Nodes = fc.Generation, nil
	}
	fl.CompiledRef, fl.CompiledHash = fc.CompiledRef, fc.CompiledHash
	if len(fc.State) > 0 {
		fl.State = append(json.RawMessage(nil), fc.State...)
	}
	if fc.Status != "" {
		fl.Status = fc.Status
	}
	fl.CurrentNode = fc.CurrentNode
	if fc.ChildRuns > fl.ChildRuns {
		fl.ChildRuns = fc.ChildRuns
	}
	fl.Depth = parent.FlowDepth
	for _, child := range fc.Children {
		attempt := child.Attempt
		if attempt < 1 {
			attempt = 1
		}
		idem := child.IdempotencyKey
		if idem == "" {
			idem = fmt.Sprintf("flow:%s:%s:%d", key, child.NodeID, attempt)
		}
		if _, exists := s.runByKey[parent.TenantID+"/"+idem]; exists {
			continue
		}
		cr := domain.Run{ID: newID(), TenantID: parent.TenantID, ProfileID: child.ProfileID,
			ProfileVersion: child.ProfileVersion, Status: domain.RunQueued, DesiredState: domain.DesiredRunning,
			Version: 1, GoalRef: child.GoalRef, InitiatorType: "system", ParentRunID: &parent.ID,
			ParentStepID: &lease.StepID, FlowNodeID: child.NodeID, FlowDepth: parent.FlowDepth + 1,
			CreatedAt: now(), UpdatedAt: now()}
		steps := child.InitialSteps
		if len(steps) == 0 {
			steps = []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", InputRef: child.GoalRef,
				InputHash: child.GoalHash, Status: domain.StepPending}}
		}
		s.saveRun(cr)
		s.runByKey[parent.TenantID+"/"+idem] = cr.ID
		s.appendEvent(cr.TenantID, cr.ID, "", "", "run.created", "system", "", map[string]any{
			"parent_run_id": parent.ID, "flow_node_id": child.NodeID, "flow_depth": cr.FlowDepth})
		for _, p := range steps {
			if _, err := s.insertStepLocked(&cr, p); err != nil {
				return err
			}
		}
		fl.Nodes = append(fl.Nodes, domain.FlowNodeRun{NodeID: child.NodeID, NodeType: child.NodeType,
			Attempt: attempt, ChildRunID: cr.ID, Status: "RUNNING", CreatedAt: now()})
	}
	s.saveFlow(fl)
	return nil
}

// flowNodeOutput returns the recorded output for a node (used by replay and
// diagnostics).
func (s *Store) flowNodeOutput(runID, nodeID string) (domain.FlowNodeRun, bool) {
	fl, ok := s.flows[runID]
	if !ok {
		return domain.FlowNodeRun{}, false
	}
	for _, n := range fl.Nodes {
		if n.NodeID == nodeID {
			return n, true
		}
	}
	return domain.FlowNodeRun{}, false
}

var _ = time.Now
