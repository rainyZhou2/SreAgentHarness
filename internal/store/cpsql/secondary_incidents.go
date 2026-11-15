package cpsql

import (
	"context"
	"fmt"

	"go-sre-harness/internal/work"
)

// SecondaryIncidentsFor returns explicit secondary Incidents of a source, newest first.
// The extra row lets the projector report a truncated result.
func (s *Store) SecondaryIncidentsFor(ctx context.Context, tenantID, sourceWorkID string) ([]work.SecondaryIncident, error) {
	if sourceWorkID == "" {
		return nil, fmt.Errorf("%w: source Work is required", work.ErrInvalid)
	}
	actionJoin := "a.id=ca.subject_id"
	if s.d == Postgres {
		actionJoin = "a.id::text=ca.subject_id"
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+workCols+`, cause_action_id, action_work_id FROM (
		SELECT w.*, ca.subject_id AS cause_action_id, a.work_id AS action_work_id
		FROM work_subject_links cw
		JOIN agent_works w ON w.tenant_id=cw.tenant_id AND w.id=cw.work_id
		LEFT JOIN work_subject_links ca ON ca.tenant_id=cw.tenant_id AND ca.work_id=cw.work_id
			AND ca.subject_kind='caused_by_action'
		LEFT JOIN work_actions a ON a.tenant_id=ca.tenant_id AND `+actionJoin+`
		WHERE cw.tenant_id=? AND cw.subject_kind='caused_by_work' AND cw.subject_id=?
		ORDER BY w.created_at DESC, w.id DESC, ca.subject_id DESC LIMIT 101
	) secondary_incidents`), tenantID, sourceWorkID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []work.SecondaryIncident
	seen := map[string]struct{}{}
	for rows.Next() {
		var actionID, mitigationWorkID ns
		w, err := scanWork(scanAppend(rows, &actionID, &mitigationWorkID))
		if err != nil {
			return nil, err
		}
		if _, ok := seen[w.ID]; ok {
			return nil, fmt.Errorf("secondary incident %s has multiple causal action links", w.ID)
		}
		seen[w.ID] = struct{}{}
		if w.Kind != work.KindIncident {
			return nil, fmt.Errorf("secondary incident %s has kind %s", w.ID, w.Kind)
		}
		if !actionID.Valid || actionID.String == "" {
			return nil, fmt.Errorf("secondary incident %s is missing its causal action link", w.ID)
		}
		if !mitigationWorkID.Valid || mitigationWorkID.String == "" {
			return nil, fmt.Errorf("secondary incident %s references missing action %s", w.ID, actionID.String)
		}
		out = append(out, work.SecondaryIncident{Work: w, ActionID: actionID.String, MitigationWorkID: mitigationWorkID.String})
	}
	return out, rows.Err()
}
