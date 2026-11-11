package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/work"
)

var _ work.WorkGraphStore = (*Store)(nil)

// treeNode is what the graph checks need from each Work of a tree.
type treeNode struct {
	id, parent string
	status     work.Status
	limits     work.Limits
	costCap    float64
}

func (s *Store) treeNodes(ctx context.Context, tx *sql.Tx, tenantID, rootID string) ([]treeNode, error) {
	rows, err := tx.QueryContext(ctx, s.q(`SELECT id, parent_work_id, status, template, goal FROM agent_works
		WHERE tenant_id=? AND (id=? OR root_work_id=?)`), tenantID, rootID, rootID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []treeNode
	for rows.Next() {
		var n treeNode
		var parent ns
		var status string
		var tpl, goal jb
		if err := rows.Scan(&n.id, &parent, &status, &tpl, &goal); err != nil {
			return nil, err
		}
		var t work.Template
		var g work.GoalSpec
		if json.Unmarshal(tpl, &t) != nil || json.Unmarshal(goal, &g) != nil {
			return nil, fmt.Errorf("decode work %s", n.id)
		}
		n.parent, n.status, n.limits, n.costCap = parent.S(), work.Status(status), t.Limits, g.Constraints.MaxCostUSD
		out = append(out, n)
	}
	return out, rows.Err()
}

// edgesOf loads the dependency edges among a set of Works as upstream →
// downstream ids. The walk is bounded: a graph larger than MaxGraphNodes is
// refused by the callers before it can get here.
func (s *Store) edgesFrom(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, tenantID string, start []string) (map[string][]string, error) {
	edges := map[string][]string{}
	seen := map[string]bool{}
	frontier := start
	for len(frontier) > 0 && len(seen) <= 4*work.MaxGraphNodes {
		var next []string
		for _, id := range frontier {
			if seen[id] {
				continue
			}
			seen[id] = true
			rows, err := q.QueryContext(ctx, s.q(`SELECT child_work_id FROM work_dependencies WHERE tenant_id=? AND parent_work_id=?`), tenantID, id)
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				var c string
				if err := rows.Scan(&c); err != nil {
					rows.Close()
					return nil, err
				}
				edges[id] = append(edges[id], c)
				next = append(next, c)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return nil, err
			}
		}
		frontier = next
	}
	return edges, nil
}

// satisfiedBy reports whether an upstream Work satisfies an edge now, and
// whether it never can (a required after_success edge of a Work that ended
// without success).
func satisfiedBy(kind work.DependencyKind, upstream work.Status) (ok, never bool) {
	switch kind {
	case work.AfterSuccess:
		return upstream == work.StatusSucceeded, upstream.Terminal() && upstream != work.StatusSucceeded
	case work.AfterTerminal:
		return upstream.Terminal(), false
	}
	// after_signal: by the signal only; an upstream that ended without
	// sending it never will.
	return false, upstream.Terminal()
}

func (s *Store) insertEdge(ctx context.Context, tx *sql.Tx, tenantID string, d work.Dependency, satisfied bool, now time.Time) error {
	var at any
	if satisfied {
		at = s.t(now)
	}
	req := 0
	if d.Required {
		req = 1
	}
	var reqArg any = req
	if s.d == Postgres {
		reqArg = d.Required
	}
	_, err := tx.ExecContext(ctx, s.q(`INSERT INTO work_dependencies (tenant_id, parent_work_id, child_work_id, dependency_kind,
		predicate_ref, required, satisfied_at, created_at) VALUES (?,?,?,?,?,?,?,?)
		ON CONFLICT (tenant_id, parent_work_id, child_work_id) DO NOTHING`), tenantID, d.ParentWorkID, d.ChildWorkID,
		string(d.Kind), nullStr(d.PredicateRef), reqArg, at, s.t(now))
	return err
}

// hold parks a Work until its dependencies are met: no wake time, and a
// reason that names what it waits for.
func (s *Store) hold(ctx context.Context, tx *sql.Tx, w *work.Work, upstream []string, now time.Time) error {
	w.Reason = "held: waiting for work " + strings.Join(upstream, ", ")
	w.WakeAt = nil
	_, err := tx.ExecContext(ctx, s.q(`UPDATE agent_works SET wake_at=NULL, reason=?, updated_at=? WHERE tenant_id=? AND id=?`),
		cpkit.Truncate(w.Reason, 2000), s.t(now), w.TenantID, w.ID)
	return err
}

// CreateChild creates a child Work under its parent, inside the parent's
// ceiling and the tree's limits, with its dependency edges — all in one
// transaction, so a refused child leaves nothing behind.
func (s *Store) CreateChild(ctx context.Context, n work.NewChild) (work.Work, bool, error) {
	nw := n.Work
	if nw.TenantID == "" || !cpkit.ValidID(n.ParentID) {
		return work.Work{}, false, fmt.Errorf("%w: tenant and parent are required", work.ErrInvalid)
	}
	now := nw.Now
	if now.IsZero() {
		now = s.now()
	}
	nw.Now = now
	var out work.Work
	created := false
	err := s.tx(ctx, func(tx *sql.Tx) error {
		created = false
		parent, err := s.lockWork(ctx, tx, nw.TenantID, n.ParentID)
		if err != nil {
			return err
		}
		// The ceiling: subjects and constraints can only narrow.
		child := nw
		if child.Goal.SubjectRefs, err = work.NarrowSubjects(parent.Goal.SubjectRefs, child.Goal.SubjectRefs); err != nil {
			return err
		}
		if child.Goal.Constraints, err = work.Narrow(parent.Goal.Constraints, child.Goal.Constraints); err != nil {
			return err
		}
		if err := child.Prepare(); err != nil {
			return err
		}
		var existingHash string
		existing, err := scanWork(scanAppend(tx.QueryRowContext(ctx, s.q(`SELECT `+workCols+`, create_request_hash FROM agent_works
			WHERE tenant_id=? AND create_idempotency_key=?`), child.TenantID, child.IdempotencyKey), &existingHash))
		switch {
		case err == nil:
			if existing.ParentID != parent.ID {
				return work.ErrIdempotencyConflict
			}
			out = existing
			return nil
		case !errors.Is(err, work.ErrNotFound):
			return err
		}
		if parent.Status.Terminal() {
			return fmt.Errorf("%w: a closed Work cannot get children", work.ErrInvalid)
		}
		rootID := parent.RootID
		root := parent
		if rootID == "" {
			rootID = parent.ID
		} else if rootID != parent.ID {
			if root, err = s.lockWork(ctx, tx, parent.TenantID, rootID); err != nil {
				return err
			}
		}
		limits := work.GraphLimits{}
		if root.Template.Graph != nil {
			limits = root.Template.Graph.Limits
		}
		limits = limits.WithDefaults()
		depth := parent.Depth + 1
		if depth > limits.MaxDepth {
			return &work.LimitError{Limit: "max_depth", Detail: fmt.Sprintf("depth %d, limit %d", depth, limits.MaxDepth)}
		}
		nodes, err := s.treeNodes(ctx, tx, parent.TenantID, rootID)
		if err != nil {
			return err
		}
		if len(nodes) >= work.MaxGraphNodes {
			return &work.LimitError{Limit: "max_graph_nodes", Detail: fmt.Sprintf("%d works", len(nodes))}
		}
		siblings, runs, actions, cost := 0, 0, 0, 0.0
		for _, x := range nodes {
			if x.parent == parent.ID {
				siblings++
			}
			if x.id != rootID {
				runs += x.limits.MaxRuns
				actions += x.limits.MaxActions
				cost += x.costCap
			}
		}
		if siblings >= limits.MaxChildren {
			return &work.LimitError{Limit: "max_children", Detail: fmt.Sprintf("%d children, limit %d", siblings, limits.MaxChildren)}
		}
		if runs+child.Template.Limits.MaxRuns > limits.MaxTotalRuns {
			return &work.LimitError{Limit: "max_total_runs", Detail: fmt.Sprintf("%d + %d runs, limit %d", runs,
				child.Template.Limits.MaxRuns, limits.MaxTotalRuns)}
		}
		if actions+child.Template.Limits.MaxActions > limits.MaxActiveActions {
			return &work.LimitError{Limit: "max_active_actions", Detail: fmt.Sprintf("%d + %d actions, limit %d", actions,
				child.Template.Limits.MaxActions, limits.MaxActiveActions)}
		}
		if limits.MaxCostUSD > 0 {
			left := limits.MaxCostUSD - cost
			if child.Goal.Constraints.MaxCostUSD == 0 || child.Goal.Constraints.MaxCostUSD > left {
				if left <= 0 {
					return &work.LimitError{Limit: "max_cost_usd", Detail: fmt.Sprintf("the tree's $%.2f is allotted", limits.MaxCostUSD)}
				}
				child.Goal.Constraints.MaxCostUSD = left
			}
		}
		if lt := limits.MaxLifetime.D(); lt > 0 {
			left := root.CreatedAt.Add(lt).Sub(now)
			if left <= 0 {
				return &work.LimitError{Limit: "max_lifetime", Detail: "the Work tree is past its lifetime"}
			}
			if child.Template.Limits.MaxDuration.D() > left {
				child.Template.Limits.MaxDuration = cpkit.Duration(left)
			}
		}
		w, err := s.createWorkTx(ctx, tx, child)
		if err != nil {
			return err
		}
		w.ParentID, w.RootID, w.Depth = parent.ID, rootID, depth
		if _, err := tx.ExecContext(ctx, s.q(`UPDATE agent_works SET parent_work_id=?, root_work_id=?, depth=? WHERE tenant_id=? AND id=?`),
			parent.ID, rootID, depth, w.TenantID, w.ID); err != nil {
			return err
		}
		if root.RootID == "" {
			if _, err := tx.ExecContext(ctx, s.q(`UPDATE agent_works SET root_work_id=? WHERE tenant_id=? AND id=? AND root_work_id IS NULL`),
				rootID, w.TenantID, rootID); err != nil {
				return err
			}
		}
		var waiting []string
		for _, d := range n.DependsOn {
			d.ChildWorkID = w.ID
			if err := d.Normalize(); err != nil {
				return err
			}
			up, err := s.lockWork(ctx, tx, w.TenantID, d.ParentWorkID)
			if errors.Is(err, work.ErrNotFound) {
				return fmt.Errorf("%w: dependency %s is not a Work of this tenant", work.ErrInvalid, d.ParentWorkID)
			}
			if err != nil {
				return err
			}
			ok, never := satisfiedBy(d.Kind, up.Status)
			if never && d.Required {
				return fmt.Errorf("%w: dependency %s is %s and can no longer be satisfied", work.ErrInvalid, up.ID, up.Status)
			}
			ok = ok || (never && !d.Required)
			if err := s.insertEdge(ctx, tx, w.TenantID, d, ok, now); err != nil {
				return err
			}
			if !ok {
				waiting = append(waiting, up.ID)
			}
		}
		if len(waiting) > 0 {
			if err := s.hold(ctx, tx, &w, waiting, now); err != nil {
				return err
			}
		}
		if err := s.appendEvents(ctx, tx, &parent, child.ActorType, child.ActorID, now, []work.NewEvent{{Type: "work.child_created",
			Payload: map[string]any{"child_id": w.ID, "key": n.Key, "kind": w.Kind, "title": w.Title, "held": len(waiting) > 0,
				"depends_on": waiting}}}); err != nil {
			return err
		}
		if err := s.saveWorkCounters(ctx, tx, parent); err != nil {
			return err
		}
		out, created = w, true
		return nil
	})
	if errors.Is(err, errRetryCreate) {
		return s.CreateChild(ctx, n)
	}
	return out, created, err
}

// AddDependency adds an edge between two Works of one tenant. The check
// for a cycle and the insert share a transaction that locks both Works, so
// two concurrent edges cannot close a cycle between them.
func (s *Store) AddDependency(ctx context.Context, tenantID string, dep work.Dependency, expectedVersion int64) error {
	if err := dep.Normalize(); err != nil {
		return err
	}
	now := s.now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		// Lock in id order: two transactions adding opposite edges wait for
		// each other instead of deadlocking.
		ids := []string{dep.ParentWorkID, dep.ChildWorkID}
		sort.Strings(ids)
		locked := map[string]work.Work{}
		for _, id := range ids {
			w, err := s.lockWork(ctx, tx, tenantID, id)
			if errors.Is(err, work.ErrNotFound) {
				// A Work of another tenant does not exist here.
				return fmt.Errorf("%w: %s is not a Work of this tenant", work.ErrNotFound, id)
			}
			if err != nil {
				return err
			}
			locked[id] = w
		}
		up, down := locked[dep.ParentWorkID], locked[dep.ChildWorkID]
		if expectedVersion != 0 && down.Version != expectedVersion {
			return work.ErrVersionConflict
		}
		if down.Status != work.StatusNew {
			return fmt.Errorf("%w: a Work that already started cannot get a new dependency", work.ErrInvalid)
		}
		edges, err := s.edgesFrom(ctx, tx, tenantID, []string{down.ID})
		if err != nil {
			return err
		}
		if work.WouldCycle(edges, up.ID, down.ID) {
			return fmt.Errorf("%w: the dependency %s → %s would close a cycle", work.ErrInvalid, up.ID, down.ID)
		}
		ok, never := satisfiedBy(dep.Kind, up.Status)
		if never && dep.Required {
			return fmt.Errorf("%w: %s is %s and can no longer satisfy the dependency", work.ErrInvalid, up.ID, up.Status)
		}
		ok = ok || (never && !dep.Required)
		if err := s.insertEdge(ctx, tx, tenantID, dep, ok, now); err != nil {
			return err
		}
		if !ok {
			if err := s.hold(ctx, tx, &down, []string{up.ID}, now); err != nil {
				return err
			}
		}
		if err := s.appendEvents(ctx, tx, &down, "system", "graph", now, []work.NewEvent{{Type: "work.dependency_added",
			Payload: map[string]any{"upstream": up.ID, "kind": dep.Kind, "required": dep.Required, "satisfied": ok}}}); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, s.q(`UPDATE agent_works SET last_event_seq=?, version=version+1, updated_at=? WHERE tenant_id=? AND id=?`),
			down.LastSeq, s.t(now), tenantID, down.ID)
		return err
	})
}

// UnmetDependencies lists the upstream Works whose required edges still
// hold a Work.
func (s *Store) UnmetDependencies(ctx context.Context, tenantID, workID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT parent_work_id FROM work_dependencies WHERE tenant_id=? AND child_work_id=?
		AND satisfied_at IS NULL ORDER BY parent_work_id`), tenantID, workID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ReadyChildren lists held children of a parent whose dependencies are met.
func (s *Store) ReadyChildren(ctx context.Context, tenantID, parentID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT w.id FROM agent_works w WHERE w.tenant_id=? AND w.parent_work_id=?
		AND w.status='NEW' AND w.wake_at IS NULL AND NOT EXISTS (SELECT 1 FROM work_dependencies d
		WHERE d.tenant_id = w.tenant_id AND d.child_work_id = w.id AND d.satisfied_at IS NULL) ORDER BY w.created_at`), tenantID, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

type edgeRow struct {
	down     string
	kind     work.DependencyKind
	pred     string
	required bool
}

func (s *Store) openEdges(ctx context.Context, tx *sql.Tx, tenantID, upstream string) ([]edgeRow, error) {
	rows, err := tx.QueryContext(ctx, s.q(`SELECT child_work_id, dependency_kind, predicate_ref, required FROM work_dependencies
		WHERE tenant_id=? AND parent_work_id=? AND satisfied_at IS NULL ORDER BY child_work_id`), tenantID, upstream)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []edgeRow
	for rows.Next() {
		var e edgeRow
		var kind string
		var pred ns
		var req any
		if err := rows.Scan(&e.down, &kind, &pred, &req); err != nil {
			return nil, err
		}
		e.kind, e.pred = work.DependencyKind(kind), pred.S()
		switch v := req.(type) {
		case bool:
			e.required = v
		case int64:
			e.required = v != 0
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// release satisfies an edge and, when it was the last one holding the
// downstream Work, makes that Work due.
func (s *Store) release(ctx context.Context, tx *sql.Tx, tenantID, upstream, down string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, s.q(`UPDATE work_dependencies SET satisfied_at=? WHERE tenant_id=? AND parent_work_id=?
		AND child_work_id=? AND satisfied_at IS NULL`), s.t(now), tenantID, upstream, down); err != nil {
		return err
	}
	var open int
	if err := tx.QueryRowContext(ctx, s.q(`SELECT count(*) FROM work_dependencies WHERE tenant_id=? AND child_work_id=?
		AND satisfied_at IS NULL`), tenantID, down).Scan(&open); err != nil || open > 0 {
		return err
	}
	w, err := s.lockWork(ctx, tx, tenantID, down)
	if err != nil || w.Status != work.StatusNew {
		return err
	}
	if err := s.appendEvents(ctx, tx, &w, "system", "graph", now, []work.NewEvent{{Type: "work.released",
		Payload: map[string]any{"by": upstream}}}); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, s.q(`UPDATE agent_works SET wake_at=?, reason=NULL, last_event_seq=?, version=version+1, updated_at=?
		WHERE tenant_id=? AND id=?`), s.t(now), w.LastSeq, s.t(now), tenantID, down)
	return err
}

// CompleteChild settles what depends on a terminal Work. Downstream Works
// are released when their edge is satisfied; a required edge that can no
// longer be satisfied cancels the held downstream Work (and, through it,
// what depends on that). The owning parent is woken so its children stage
// re-evaluates. The call is idempotent: settled edges are skipped.
func (s *Store) CompleteChild(ctx context.Context, tenantID, childID string, version int64) error {
	now := s.now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		queue := []string{childID}
		for seen := map[string]bool{}; len(queue) > 0 && len(seen) < 4*work.MaxGraphNodes; {
			id := queue[0]
			queue = queue[1:]
			if seen[id] {
				continue
			}
			seen[id] = true
			w, err := s.lockWork(ctx, tx, tenantID, id)
			if err != nil {
				return err
			}
			if !w.Status.Terminal() {
				if id == childID {
					return fmt.Errorf("%w: work %s is %s, not terminal", work.ErrInvalid, id, w.Status)
				}
				continue
			}
			if id == childID && version != 0 && w.Version != version {
				return work.ErrVersionConflict
			}
			edges, err := s.openEdges(ctx, tx, tenantID, id)
			if err != nil {
				return err
			}
			for _, e := range edges {
				ok, never := satisfiedBy(e.kind, w.Status)
				switch {
				case ok || (never && !e.required):
					if err := s.release(ctx, tx, tenantID, id, e.down, now); err != nil {
						return err
					}
				case never:
					// The downstream can never start: close it, and settle
					// what waits for it in turn.
					d, err := s.lockWork(ctx, tx, tenantID, e.down)
					if err != nil {
						return err
					}
					if d.Status != work.StatusNew {
						continue
					}
					reason := fmt.Sprintf("dependency %s ended %s", id, w.Status)
					if err := s.appendEvents(ctx, tx, &d, "system", "graph", now, []work.NewEvent{{Type: "work.status_changed",
						Payload: map[string]any{"from": d.Status, "to": work.StatusCancelled, "reason": reason}}}); err != nil {
						return err
					}
					if _, err := tx.ExecContext(ctx, s.q(`UPDATE agent_works SET status='CANCELLED', reason=?, wake_at=NULL,
						closed_at=?, last_event_seq=?, version=version+1, updated_at=? WHERE tenant_id=? AND id=?`), reason, s.t(now),
						d.LastSeq, s.t(now), tenantID, d.ID); err != nil {
						return err
					}
					queue = append(queue, d.ID)
				}
			}
			if w.ParentID == "" {
				continue
			}
			p, err := s.lockWork(ctx, tx, tenantID, w.ParentID)
			if errors.Is(err, work.ErrNotFound) || (err == nil && p.Status.Terminal()) {
				continue
			}
			if err != nil {
				return err
			}
			if err := s.appendEvents(ctx, tx, &p, "system", "graph", now, []work.NewEvent{{Type: "work.child_completed",
				Payload: map[string]any{"child_id": w.ID, "status": w.Status, "reason": cpkit.Truncate(w.Reason, 300)}}}); err != nil {
				return err
			}
			// Suspended and blocked parents keep their state; the wake only
			// matters to a parent that is waiting for its children.
			if _, err := tx.ExecContext(ctx, s.q(`UPDATE agent_works SET wake_at=CASE WHEN status IN ('SUSPENDED','BLOCKED')
				THEN wake_at ELSE `+s.tsParam()+` END, last_event_seq=?, version=version+1, updated_at=? WHERE tenant_id=? AND id=?`),
				s.t(now), p.LastSeq, s.t(now), tenantID, p.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

// tsParam is a timestamp placeholder usable inside CASE (PostgreSQL cannot
// infer the type of a bare parameter there).
func (s *Store) tsParam() string {
	if s.d == Postgres {
		return "CAST(? AS TIMESTAMPTZ)"
	}
	return "?"
}

// SignalDependents satisfies after_signal edges of a Work for a predicate
// ("stage:<id>") and releases the Works they held.
func (s *Store) SignalDependents(ctx context.Context, tenantID, workID, predicate string) (int, error) {
	now := s.now()
	n := 0
	err := s.tx(ctx, func(tx *sql.Tx) error {
		n = 0
		edges, err := s.openEdges(ctx, tx, tenantID, workID)
		if err != nil {
			return err
		}
		for _, e := range edges {
			if e.kind != work.AfterSignal || e.pred != predicate {
				continue
			}
			if err := s.release(ctx, tx, tenantID, workID, e.down, now); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

// UnsettledTerminalWorks lists terminal Works that still have open edges
// or an unnotified parent stage (the reconciler's backstop for a
// CompleteChild that was lost with its process).
func (s *Store) UnsettledTerminalWorks(ctx context.Context, limit int) ([][2]string, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT DISTINCT w.tenant_id, w.id FROM agent_works w
		JOIN work_dependencies d ON d.tenant_id = w.tenant_id AND d.parent_work_id = w.id
		WHERE w.status IN ('SUCCEEDED','FAILED','CANCELLED') AND d.satisfied_at IS NULL LIMIT ?`), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]string
	for rows.Next() {
		var t, id string
		if err := rows.Scan(&t, &id); err != nil {
			return nil, err
		}
		out = append(out, [2]string{t, id})
	}
	return out, rows.Err()
}

// Children lists the child Works of a parent, oldest first.
func (s *Store) Children(ctx context.Context, tenantID, parentID string) ([]work.Work, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+workCols+` FROM agent_works WHERE tenant_id=? AND parent_work_id=?
		ORDER BY created_at, id`), tenantID, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []work.Work
	for rows.Next() {
		w, err := scanWork(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// WorkGraph returns the tree a Work belongs to with its dependency edges.
func (s *Store) WorkGraph(ctx context.Context, tenantID, workID string) (work.Graph, error) {
	w, err := s.GetWork(ctx, tenantID, workID)
	if err != nil {
		return work.Graph{}, err
	}
	g := work.Graph{RootID: w.RootID}
	if g.RootID == "" {
		g.RootID = w.ID
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+workCols+` FROM agent_works WHERE tenant_id=? AND (id=? OR root_work_id=?)
		ORDER BY depth, created_at, id LIMIT ?`), tenantID, g.RootID, g.RootID, work.MaxGraphNodes)
	if err != nil {
		return g, err
	}
	var works []work.Work
	for rows.Next() {
		x, err := scanWork(rows)
		if err != nil {
			rows.Close()
			return g, err
		}
		works = append(works, x)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return g, err
	}
	ids := make([]string, len(works))
	for i, x := range works {
		ids[i] = x.ID
		if x.ID == g.RootID && x.Template.Graph != nil {
			g.Limits = x.Template.Graph.Limits
		}
	}
	g.Limits = g.Limits.WithDefaults()
	held := map[string]bool{}
	if len(ids) > 0 {
		erows, err := s.db.QueryContext(ctx, s.q(`SELECT parent_work_id, child_work_id, dependency_kind, predicate_ref, required,
			satisfied_at FROM work_dependencies WHERE tenant_id=? AND child_work_id IN (`+placeholders(len(ids))+`)
			ORDER BY created_at, parent_work_id`), append([]any{tenantID}, anys(ids)...)...)
		if err != nil {
			return g, err
		}
		defer erows.Close()
		for erows.Next() {
			var d work.Dependency
			var kind string
			var pred ns
			var req any
			var at ts
			if err := erows.Scan(&d.ParentWorkID, &d.ChildWorkID, &kind, &pred, &req, &at); err != nil {
				return g, err
			}
			d.Kind, d.PredicateRef, d.SatisfiedAt = work.DependencyKind(kind), pred.S(), at.ptr()
			switch v := req.(type) {
			case bool:
				d.Required = v
			case int64:
				d.Required = v != 0
			}
			if d.SatisfiedAt == nil {
				held[d.ChildWorkID] = true
			}
			g.Edges = append(g.Edges, d)
		}
		if err := erows.Err(); err != nil {
			return g, err
		}
	}
	for _, x := range works {
		n := work.GraphNode{ID: x.ID, ParentID: x.ParentID, Depth: x.Depth, Kind: x.Kind, Status: x.Status, Title: x.Title,
			Queue: x.Queue, Reason: x.Reason, CostMicros: x.CostMicros, RunsStarted: x.RunsStarted,
			Held: held[x.ID] && x.Status == work.StatusNew}
		if st, ok := x.CurrentStage(); ok {
			n.Stage = st.ID
		}
		n.WaitingOn = work.WaitingOn(x, n.Held)
		g.Nodes = append(g.Nodes, n)
	}
	return g, nil
}
