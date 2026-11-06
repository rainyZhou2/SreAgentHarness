package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
)

// Conversation persistence (§29.3). Ordering is enforced by locking the
// branch row in PostgreSQL — never by an in-process mutex — so it holds across
// API replicas. Lock order: run → branch → (next) run → turn.

const sessionCols = `id, tenant_id, created_by, title, profile_id, status, coalesce(default_branch_id::text,''), incident_id, created_at`

func scanSession(row pgx.Row) (domain.Session, error) {
	var s domain.Session
	err := row.Scan(&s.ID, &s.TenantID, &s.CreatedBy, &s.Title, &s.ProfileID, &s.Status, &s.DefaultBranchID, &s.IncidentID, &s.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, domain.ErrNotFound
	}
	return s, err
}

const branchCols = `id, session_id, parent_branch_id, parent_message_cursor, forked_from_checkpoint_id, version,
	last_turn_seq, last_message_seq, active_turn_id`

func scanBranch(row pgx.Row) (domain.Branch, error) {
	var b domain.Branch
	err := row.Scan(&b.ID, &b.SessionID, &b.ParentBranchID, &b.ParentMessageCursor, &b.ForkedFrom, &b.Version,
		&b.LastTurnSeq, &b.LastMessageSeq, &b.ActiveTurnID)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, domain.ErrNotFound
	}
	return b, err
}

const turnCols = `id, session_id, branch_id, run_id, seq, input_ref, input_message_cursor, client_request_id, status,
	created_at, activated_at`

func scanTurn(row pgx.Row) (domain.Turn, error) {
	var t domain.Turn
	err := row.Scan(&t.ID, &t.SessionID, &t.BranchID, &t.RunID, &t.Seq, &t.InputRef, &t.InputMessageCursor,
		&t.ClientRequestID, &t.Status, &t.CreatedAt, &t.ActivatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, domain.ErrNotFound
	}
	return t, err
}

// CreateSession creates a session and its main branch (idempotent).
func (s *Store) CreateSession(ctx context.Context, req domain.CreateSessionRequest) (domain.Session, bool, error) {
	var out domain.Session
	created := false
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		id, branchID := newID(), newID()
		var inserted string
		err := tx.QueryRow(ctx, `INSERT INTO conversation_sessions (id, tenant_id, created_by, title, profile_id,
			default_branch_id, incident_id, create_idempotency_key) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (tenant_id, create_idempotency_key) DO NOTHING RETURNING id`,
			id, req.TenantID, req.ActorID, nullable(req.Title), req.ProfileID, branchID, req.IncidentID,
			req.IdempotencyKey).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			out, err = scanSession(tx.QueryRow(ctx, `SELECT `+sessionCols+` FROM conversation_sessions
				WHERE tenant_id=$1 AND create_idempotency_key=$2`, req.TenantID, req.IdempotencyKey))
			return err
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO conversation_branches (id, tenant_id, session_id, created_by)
			VALUES ($1,$2,$3,$4)`, branchID, req.TenantID, id, req.ActorID); err != nil {
			return err
		}
		created = true
		out, err = scanSession(tx.QueryRow(ctx, `SELECT `+sessionCols+` FROM conversation_sessions WHERE id=$1`, id))
		return err
	})
	return out, created, err
}

func (s *Store) GetSession(ctx context.Context, tenantID, sessionID string) (domain.Session, error) {
	if !validID(sessionID) {
		return domain.Session{}, domain.ErrNotFound
	}
	return scanSession(s.pool.QueryRow(ctx, `SELECT `+sessionCols+` FROM conversation_sessions
		WHERE id=$1 AND tenant_id=$2`, sessionID, tenantID))
}

// SubmitTurn implements the "submit turn" transaction of §29.3.
func (s *Store) SubmitTurn(ctx context.Context, req domain.SubmitTurnRequest, policy domain.BranchPolicy, maxPending int) (domain.SubmittedTurn, error) {
	var out domain.SubmittedTurn
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		sess, err := scanSession(tx.QueryRow(ctx, `SELECT `+sessionCols+` FROM conversation_sessions
			WHERE id=$1 AND tenant_id=$2`, req.SessionID, req.TenantID))
		if err != nil {
			return err
		}
		if sess.Status != "ACTIVE" {
			return domain.ErrNotFound
		}
		branchID := req.BranchID
		if branchID == "" {
			branchID = sess.DefaultBranchID
		}
		if !validID(branchID) {
			return domain.ErrNotFound
		}
		br, err := scanBranch(tx.QueryRow(ctx, `SELECT `+branchCols+` FROM conversation_branches
			WHERE id=$1 AND tenant_id=$2 AND session_id=$3 FOR UPDATE`, branchID, req.TenantID, sess.ID))
		if err != nil {
			return err
		}
		// Idempotency: the same client request returns the same turn/run.
		if t, err := scanTurn(tx.QueryRow(ctx, `SELECT `+turnCols+` FROM conversation_turns
			WHERE tenant_id=$1 AND branch_id=$2 AND client_request_id=$3`, req.TenantID, branchID, req.ClientRequestID)); err == nil {
			out = domain.SubmittedTurn{TurnID: t.ID, RunID: deref(t.RunID), BranchID: branchID, Seq: t.Seq,
				BranchVersion: br.Version, Status: t.Status, Queued: t.Status == domain.TurnQueued}
			return nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		if req.ExpectedVersion != nil && *req.ExpectedVersion != br.Version {
			return domain.ErrVersionConflict
		}
		busy := br.ActiveTurnID != nil
		var pending int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM conversation_turns WHERE tenant_id=$1 AND branch_id=$2
			AND status='QUEUED'`, req.TenantID, branchID).Scan(&pending); err != nil {
			return err
		}
		if busy && policy == domain.BranchReject {
			return domain.ErrBranchBusy
		}
		if (busy || pending > 0) && pending >= maxPending {
			return domain.ErrTurnQueueFull
		}
		queued := busy || pending > 0
		turnID, seq := newID(), br.LastTurnSeq+1
		status := domain.TurnQueued
		if _, err := tx.Exec(ctx, `INSERT INTO conversation_turns (id, tenant_id, session_id, branch_id, seq, input_ref,
			input_hash, client_request_id, created_by, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			turnID, req.TenantID, sess.ID, branchID, seq, req.InputRef, req.InputHash, req.ClientRequestID,
			req.ActorID, status); err != nil {
			return err
		}
		r, _, err := s.createRunTx(ctx, tx, CreateRunRequest{TenantID: req.TenantID, IncidentID: sess.IncidentID,
			ProfileID: sess.ProfileID, ProfileVersion: req.ProfileVersion, GoalRef: req.InputRef, GoalHash: req.InputHash,
			IdempotencyKey: "turn:" + turnID, InitiatorType: "user", InitiatorID: req.ActorID, SessionID: &sess.ID,
			BranchID: &branchID, TurnID: &turnID, PendingActivation: true, RolloutID: req.RolloutID,
			InitialSteps: req.InitialSteps})
		if err != nil {
			return err
		}
		if err := r.save(ctx); err != nil {
			return err
		}
		runID := r.run.ID
		if _, err := tx.Exec(ctx, `UPDATE conversation_turns SET run_id=$2 WHERE id=$1`, turnID, runID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE conversation_branches SET last_turn_seq=$2, version=version+1 WHERE id=$1`,
			branchID, seq); err != nil {
			return err
		}
		if !queued {
			if err := s.activateTurn(ctx, tx, req.TenantID, branchID, turnID, runID); err != nil {
				return err
			}
			status = domain.TurnRunning
		}
		var ver int64
		if err := tx.QueryRow(ctx, `SELECT version FROM conversation_branches WHERE id=$1`, branchID).Scan(&ver); err != nil {
			return err
		}
		out = domain.SubmittedTurn{TurnID: turnID, RunID: runID, BranchID: branchID, Seq: seq, BranchVersion: ver,
			Status: status, Queued: queued, Created: true}
		return nil
	})
	return out, err
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// activateTurn: the branch row must already be locked by the caller. It
// appends the user message, pins the input cursor and makes the run schedulable.
func (s *Store) activateTurn(ctx context.Context, tx pgx.Tx, tenantID, branchID, turnID, runID string) error {
	r, err := s.lockRun(ctx, tx, tenantID, runID)
	if err != nil {
		return err
	}
	var status domain.TurnStatus
	var sessionID, inputRef, inputHash string
	if err := tx.QueryRow(ctx, `SELECT status, session_id, input_ref, input_hash FROM conversation_turns
		WHERE id=$1 FOR UPDATE`, turnID).Scan(&status, &sessionID, &inputRef, &inputHash); err != nil {
		return err
	}
	if status != domain.TurnQueued || r.run.Status != domain.RunPendingActivation {
		return nil // cancelled meanwhile
	}
	var seq int64
	if err := tx.QueryRow(ctx, `UPDATE conversation_branches SET last_message_seq=last_message_seq+1, active_turn_id=$2,
		version=version+1 WHERE id=$1 RETURNING last_message_seq`, branchID, turnID).Scan(&seq); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO conversation_messages (id, tenant_id, session_id, branch_id, turn_id, seq,
		role, content_ref, content_hash, message_kind) VALUES ($1,$2,$3,$4,$5,$6,'user',$7,$8,'text')`,
		newID(), tenantID, sessionID, branchID, turnID, seq, inputRef, inputHash); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE conversation_turns SET status='RUNNING', input_message_cursor=$2, activated_at=now(),
		updated_at=now() WHERE id=$1`, turnID, seq); err != nil {
		return err
	}
	if err := r.event(ctx, "run.activated", "system", "", nil, nil, map[string]any{"turn_id": turnID, "message_cursor": seq}); err != nil {
		return err
	}
	r.setStatus(domain.RunQueued)
	if err := r.settle(ctx); err != nil {
		return err
	}
	return r.save(ctx)
}

// finalizeTurn runs in the same transaction that makes a turn's run terminal:
// it commits the assistant/status message, frees the branch and activates the
// next queued turn (§29.3 "终结当前 Turn 事务").
func (r *runTx) finalizeTurn(ctx context.Context) error {
	tx := r.tx
	var status domain.TurnStatus
	var branchID, sessionID string
	err := tx.QueryRow(ctx, `SELECT status, branch_id, session_id FROM conversation_turns WHERE id=$1 FOR UPDATE`,
		*r.run.TurnID).Scan(&status, &branchID, &sessionID)
	if err != nil {
		return err
	}
	final := map[domain.RunStatus]domain.TurnStatus{domain.RunCompleted: domain.TurnCompleted,
		domain.RunFailed: domain.TurnFailed, domain.RunCancelled: domain.TurnCancelled}[r.run.Status]
	switch status {
	case domain.TurnQueued: // cancelled before activation: no messages, branch untouched
		_, err := tx.Exec(ctx, `UPDATE conversation_turns SET status=$2, updated_at=now() WHERE id=$1`, *r.run.TurnID, final)
		return err
	case domain.TurnRunning:
	default:
		return nil // already finalized
	}
	var locked string
	if err := tx.QueryRow(ctx, `SELECT id FROM conversation_branches WHERE id=$1 FOR UPDATE`, branchID).Scan(&locked); err != nil {
		return err
	}
	ref, hash, kind := "", "", "text"
	switch {
	case r.run.Status == domain.RunCompleted && r.run.ResultRef != nil:
		ref, hash = *r.run.ResultRef, hashStrings(*r.run.ResultRef)
	default:
		kind = "status"
		reason := string(r.run.Status)
		if r.run.FailureReason != nil {
			reason += ": " + *r.run.FailureReason
		}
		ref, hash = "status:"+string(r.run.Status), hashStrings(reason)
	}
	var seq int64
	if err := tx.QueryRow(ctx, `UPDATE conversation_branches SET last_message_seq=last_message_seq+1, active_turn_id=NULL,
		version=version+1 WHERE id=$1 RETURNING last_message_seq`, branchID).Scan(&seq); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO conversation_messages (id, tenant_id, session_id, branch_id, turn_id, seq,
		role, content_ref, content_hash, message_kind) VALUES ($1,$2,$3,$4,$5,$6,'assistant',$7,$8,$9)`,
		newID(), r.run.TenantID, sessionID, branchID, *r.run.TurnID, seq, ref, hash, kind); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE conversation_turns SET status=$2, updated_at=now() WHERE id=$1`, *r.run.TurnID, final); err != nil {
		return err
	}
	if err := r.event(ctx, "turn.finalized", "system", "", nil, nil, map[string]any{"turn_id": *r.run.TurnID,
		"status": final, "message_seq": seq}); err != nil {
		return err
	}
	return r.s.activateNext(ctx, tx, r.run.TenantID, branchID)
}

// activateNext activates the oldest queued turn of a locked, idle branch.
func (s *Store) activateNext(ctx context.Context, tx pgx.Tx, tenantID, branchID string) error {
	for {
		var turnID, runID string
		err := tx.QueryRow(ctx, `SELECT id, run_id FROM conversation_turns WHERE tenant_id=$1 AND branch_id=$2
			AND status='QUEUED' ORDER BY seq LIMIT 1`, tenantID, branchID).Scan(&turnID, &runID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.activateTurn(ctx, tx, tenantID, branchID, turnID, runID); err != nil {
			return err
		}
		var active *string
		if err := tx.QueryRow(ctx, `SELECT active_turn_id FROM conversation_branches WHERE id=$1`, branchID).Scan(&active); err != nil {
			return err
		}
		if active != nil {
			return nil
		}
		// The queued turn had been cancelled concurrently; try the next one.
		if _, err := tx.Exec(ctx, `UPDATE conversation_turns SET status='CANCELLED' WHERE id=$1 AND status='QUEUED'`, turnID); err != nil {
			return err
		}
	}
}

// ActivateStalledBranches is the reconciler safety net (§29.3): a branch with
// no active turn but queued turns is activated even if a wake-up was lost.
func (s *Store) ActivateStalledBranches(ctx context.Context, limit int) (int, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT b.tenant_id, b.id FROM conversation_branches b
		JOIN conversation_turns t ON t.branch_id = b.id AND t.status='QUEUED'
		WHERE b.active_turn_id IS NULL LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	type tb struct{ tenant, branch string }
	var list []tb
	for rows.Next() {
		var x tb
		if err := rows.Scan(&x.tenant, &x.branch); err != nil {
			rows.Close()
			return 0, err
		}
		list = append(list, x)
	}
	rows.Close()
	n := 0
	var errs []error
	for _, x := range list {
		err := s.inTx(ctx, func(tx pgx.Tx) error {
			var active *string
			if err := tx.QueryRow(ctx, `SELECT active_turn_id FROM conversation_branches WHERE id=$1 FOR UPDATE`, x.branch).
				Scan(&active); err != nil {
				return err
			}
			if active != nil {
				return nil
			}
			if err := s.activateNext(ctx, tx, x.tenant, x.branch); err != nil {
				return err
			}
			n++
			return nil
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("branch %s: %w", x.branch, err))
		}
	}
	return n, errors.Join(errs...)
}

// ForkBranch creates a branch whose history is the parent's messages up to a
// committed cursor (§29.2 item 4). The new branch shares no execution state.
func (s *Store) ForkBranch(ctx context.Context, req domain.ForkBranchRequest) (domain.Branch, error) {
	var out domain.Branch
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		parent, err := scanBranch(tx.QueryRow(ctx, `SELECT `+branchCols+` FROM conversation_branches
			WHERE id=$1 AND tenant_id=$2 AND session_id=$3 FOR SHARE`, req.ParentBranchID, req.TenantID, req.SessionID))
		if err != nil {
			return err
		}
		cursor := parent.LastMessageSeq
		if req.MessageCursor != nil {
			cursor = *req.MessageCursor
		}
		history, err := s.branchMessages(ctx, tx, req.TenantID, parent.ID, cursor)
		if err != nil {
			return err
		}
		if cursor > parent.LastMessageSeq || (cursor > 0 && !isAncestorCursor(history, cursor)) {
			return domain.ErrInvalidCursor
		}
		// Do not fork in the middle of a turn: the cursor must end on an assistant/status message.
		if len(history) > 0 && history[len(history)-1].Role == "user" {
			return domain.ErrInvalidCursor
		}
		id := newID()
		if _, err := tx.Exec(ctx, `INSERT INTO conversation_branches (id, tenant_id, session_id, parent_branch_id,
			parent_message_cursor, last_message_seq, created_by) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			id, req.TenantID, req.SessionID, parent.ID, cursor, cursor, req.ActorID); err != nil {
			return err
		}
		out, err = scanBranch(tx.QueryRow(ctx, `SELECT `+branchCols+` FROM conversation_branches WHERE id=$1`, id))
		return err
	})
	return out, err
}

func isAncestorCursor(history []domain.Message, cursor int64) bool {
	for _, m := range history {
		if m.Seq == cursor {
			return true
		}
	}
	return false
}

// BranchMessages returns the branch history resolved through its ancestors:
// parent messages up to parent_message_cursor, then the branch's own (§29.3).
// Message seqs on a forked branch continue after the fork cursor.
func (s *Store) BranchMessages(ctx context.Context, tenantID, branchID string, upTo int64) ([]domain.Message, error) {
	if !validID(branchID) {
		return nil, domain.ErrNotFound
	}
	return s.branchMessages(ctx, s.pool, tenantID, branchID, upTo)
}

func (s *Store) branchMessages(ctx context.Context, q querier, tenantID, branchID string, upTo int64) ([]domain.Message, error) {
	br, err := scanBranch(q.QueryRow(ctx, `SELECT `+branchCols+` FROM conversation_branches WHERE id=$1 AND tenant_id=$2`,
		branchID, tenantID))
	if err != nil {
		return nil, err
	}
	var out []domain.Message
	ownFrom := int64(0)
	if br.ParentBranchID != nil && br.ParentMessageCursor != nil {
		limit := *br.ParentMessageCursor
		if upTo < limit {
			limit = upTo
		}
		parent, err := s.branchMessages(ctx, q, tenantID, *br.ParentBranchID, limit)
		if err != nil {
			return nil, err
		}
		out = parent
		ownFrom = *br.ParentMessageCursor
	}
	rows, err := q.Query(ctx, `SELECT id, branch_id, turn_id, seq, role, content_ref, content_hash, message_kind,
		source_step_id, created_at FROM conversation_messages WHERE tenant_id=$1 AND branch_id=$2 AND seq > $3 AND seq <= $4
		ORDER BY seq`, tenantID, branchID, ownFrom, upTo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var m domain.Message
		if err := rows.Scan(&m.ID, &m.BranchID, &m.TurnID, &m.Seq, &m.Role, &m.ContentRef, &m.ContentHash, &m.Kind,
			&m.SourceStepID, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ResumeSession loads what a reconnecting client needs from PostgreSQL —
// no goroutine state is restored (§29.4).
func (s *Store) ResumeSession(ctx context.Context, tenantID, sessionID string) (domain.SessionView, error) {
	var v domain.SessionView
	sess, err := s.GetSession(ctx, tenantID, sessionID)
	if err != nil {
		return v, err
	}
	v.Session = sess
	rows, err := s.pool.Query(ctx, `SELECT `+branchCols+` FROM conversation_branches WHERE tenant_id=$1 AND session_id=$2
		ORDER BY created_at`, tenantID, sessionID)
	if err != nil {
		return v, err
	}
	for rows.Next() {
		b, err := scanBranch(rows)
		if err != nil {
			rows.Close()
			return v, err
		}
		v.Branches = append(v.Branches, b)
	}
	rows.Close()
	turns, err := s.ListTurns(ctx, tenantID, sessionID, sess.DefaultBranchID)
	if err != nil {
		return v, err
	}
	v.Queued = []domain.Turn{}
	for i := range turns {
		switch turns[i].Status {
		case domain.TurnRunning:
			t := turns[i]
			v.ActiveTurn = &t
		case domain.TurnQueued:
			v.Queued = append(v.Queued, turns[i])
		}
	}
	v.Messages, err = s.BranchMessages(ctx, tenantID, sess.DefaultBranchID, 1<<62)
	return v, err
}

func (s *Store) ListTurns(ctx context.Context, tenantID, sessionID, branchID string) ([]domain.Turn, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+turnCols+` FROM conversation_turns WHERE tenant_id=$1 AND session_id=$2
		AND ($3 = '' OR branch_id::text = $3) ORDER BY branch_id, seq`, tenantID, sessionID, branchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Turn
	for rows.Next() {
		t, err := scanTurn(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) GetTurn(ctx context.Context, tenantID, turnID string) (domain.Turn, error) {
	if !validID(turnID) {
		return domain.Turn{}, domain.ErrNotFound
	}
	return scanTurn(s.pool.QueryRow(ctx, `SELECT `+turnCols+` FROM conversation_turns WHERE id=$1 AND tenant_id=$2`,
		turnID, tenantID))
}

// afterSave runs terminal-state hooks in the same transaction as the state change.
func (r *runTx) afterSave(ctx context.Context) error {
	if !r.run.Status.Terminal() || r.startStatus.Terminal() || r.terminalHandled {
		return nil
	}
	r.terminalHandled = true
	if r.run.TurnID != nil {
		if err := r.finalizeTurn(ctx); err != nil {
			return err
		}
	}
	// Cold-path consolidation work item (§11.1); runs outside the hot path.
	if r.run.Status == domain.RunCompleted || r.run.Status == domain.RunFailed {
		if _, err := r.tx.Exec(ctx, `INSERT INTO consolidation_queue (tenant_id, run_id) VALUES ($1,$2)
			ON CONFLICT DO NOTHING`, r.run.TenantID, r.run.ID); err != nil {
			return err
		}
	}
	// finalizeTurn may have appended events; persist the counter.
	_, err := r.tx.Exec(ctx, `UPDATE agent_runs SET last_event_seq=$2 WHERE id=$1`, r.run.ID, r.run.LastEventSeq)
	return err
}

// RunConversation returns the branch history visible to a turn's run: every
// committed message before the turn's own input message (§29.3). Runs that
// are not conversation turns have no history.
func (s *Store) RunConversation(ctx context.Context, tenantID, runID string) ([]domain.Message, error) {
	var branchID *string
	var cursor *int64
	err := s.pool.QueryRow(ctx, `SELECT r.branch_id, t.input_message_cursor FROM agent_runs r
		LEFT JOIN conversation_turns t ON t.id = r.turn_id WHERE r.id=$1 AND r.tenant_id=$2`, runID, tenantID).
		Scan(&branchID, &cursor)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	if branchID == nil || cursor == nil || *cursor <= 1 {
		return nil, nil
	}
	return s.branchMessages(ctx, s.pool, tenantID, *branchID, *cursor-1)
}

// SessionSummary is a session with its latest activity, for listings.
type SessionSummary struct {
	domain.Session
	LastActivityAt time.Time `json:"last_activity_at"`
	Turns          int64     `json:"turns"`
	ActiveTurns    int64     `json:"active_turns"`
}

// SessionFilter selects sessions for ListSessions.
type SessionFilter struct {
	Status string // ACTIVE (default) or ARCHIVED
	Query  string // case-insensitive substring of the title
	Limit  int
}

// ListSessions returns the tenant's sessions in one status (ACTIVE by
// default), most recently used first, optionally filtered by title.
func (s *Store) ListSessions(ctx context.Context, tenantID string, f SessionFilter) ([]SessionSummary, error) {
	if f.Status == "" {
		f.Status = "ACTIVE"
	}
	if f.Limit <= 0 {
		f.Limit = 50
	}
	like := "%" + strings.NewReplacer(`\`, `\\`, "%", `\%`, "_", `\_`).Replace(f.Query) + "%"
	rows, err := s.pool.Query(ctx, `SELECT s.id, s.tenant_id, s.created_by, s.title, s.profile_id, s.status,
			COALESCE(s.default_branch_id::text, ''), s.incident_id::text, s.created_at,
			COALESCE(t.last_at, s.created_at), COALESCE(t.n, 0), COALESCE(t.active, 0)
		FROM conversation_sessions s
		LEFT JOIN LATERAL (SELECT max(created_at) AS last_at, count(*) AS n,
				count(*) FILTER (WHERE status IN ('ACCEPTED','QUEUED','RUNNING')) AS active
			FROM conversation_turns WHERE tenant_id=s.tenant_id AND session_id=s.id) t ON true
		WHERE s.tenant_id=$1 AND s.status=$3 AND ($4 = '%%' OR COALESCE(s.title, '') ILIKE $4)
		ORDER BY 10 DESC, s.id DESC LIMIT $2`, tenantID, f.Limit, f.Status, like)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SessionSummary
	for rows.Next() {
		var x SessionSummary
		if err := rows.Scan(&x.ID, &x.TenantID, &x.CreatedBy, &x.Title, &x.ProfileID, &x.Status, &x.DefaultBranchID,
			&x.IncidentID, &x.CreatedAt, &x.LastActivityAt, &x.Turns, &x.ActiveTurns); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

// UpdateSession renames a session and/or changes its status (ACTIVE,
// ARCHIVED or DELETED). A DELETED session is hidden from every listing;
// its turns and runs are kept for audit.
func (s *Store) UpdateSession(ctx context.Context, tenantID, id string, title, status *string) (domain.Session, error) {
	if !validID(id) {
		return domain.Session{}, domain.ErrNotFound
	}
	tag, err := s.pool.Exec(ctx, `UPDATE conversation_sessions SET title=COALESCE($3, title), status=COALESCE($4, status),
		updated_at=now() WHERE tenant_id=$1 AND id=$2 AND status <> 'DELETED'`, tenantID, id, title, status)
	if err != nil {
		return domain.Session{}, err
	}
	if tag.RowsAffected() == 0 {
		return domain.Session{}, domain.ErrNotFound
	}
	v, err := s.ResumeSession(ctx, tenantID, id)
	return v.Session, err
}

// PendingApproval is an approval waiting for a decision, with where it
// came from.
type PendingApproval struct {
	domain.ApprovalRequest
	RunID        string  `json:"run_id"`
	SessionID    *string `json:"session_id,omitempty"`
	SessionTitle *string `json:"session_title,omitempty"`
	InitiatorID  *string `json:"initiator_id,omitempty"`
}

// ListPendingApprovals returns the tenant's unexpired pending approvals,
// newest first.
func (s *Store) ListPendingApprovals(ctx context.Context, tenantID string, limit int) ([]PendingApproval, error) {
	rows, err := s.pool.Query(ctx, `SELECT a.id, a.action_intent_id, i.step_id, i.action_hash, i.risk_class, i.tool_name,
			i.target, a.required_approvals, a.status, a.expires_at, a.created_at, a.decided_at,
			i.run_id::text, t.session_id::text, cs.title, r.initiator_id
		FROM approval_requests a
		JOIN action_intents i ON i.id = a.action_intent_id
		JOIN agent_runs r ON r.id = i.run_id
		LEFT JOIN conversation_turns t ON t.tenant_id = a.tenant_id AND t.run_id = i.run_id
		LEFT JOIN conversation_sessions cs ON cs.tenant_id = a.tenant_id AND cs.id = t.session_id
		WHERE a.tenant_id=$1 AND a.status='PENDING' AND a.expires_at > now()
		ORDER BY a.created_at DESC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingApproval
	for rows.Next() {
		var a PendingApproval
		if err := rows.Scan(&a.ID, &a.ActionIntentID, &a.StepID, &a.ActionHash, &a.RiskClass, &a.ToolName, &a.Target,
			&a.RequiredApprovals, &a.Status, &a.ExpiresAt, &a.CreatedAt, &a.DecidedAt,
			&a.RunID, &a.SessionID, &a.SessionTitle, &a.InitiatorID); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
