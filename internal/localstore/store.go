// Package localstore is the single-user local store used by the harness
// runtime. State objects are persisted as JSON in SQLite; execution events are
// appended to JSONL first, and SQLite keeps only byte-range indexes into those
// files. JSONL is the execution source of truth and can rebuild the index.
// The local runtime is one process, so a mutex provides row locking.
package localstore

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
)

const tenantLocal = "local"

type Store struct {
	db *sql.DB

	mu sync.Mutex

	runs         map[string]domain.Run
	runByKey     map[string]string // tenant/idempotency -> run id
	steps        map[string]domain.Step
	attempts     map[string]attempt
	eventDir     string
	events       map[string][]eventIndex
	eventCache   map[string]domain.Event
	eventSeq     map[string]int64
	legacyEvents []domain.Event
	sessions     map[string]domain.Session
	sessionKey   map[string]string
	branches     map[string]domain.Branch
	turns        map[string]domain.Turn
	messages     map[string][]domain.Message
	profiles     map[string]profileRec
	flows        map[string]domain.FlowSnapshot
	flowInputs   map[string]flowInput
	intents      map[string]domain.ActionIntent
	intentStep   map[string]string
	approvals    map[string]approvalRec
	decisions    map[string][]decisionRec
	grants       map[string]grantRec
	kv           map[string]kvRec
	effects      map[string]effectRec
	outbox       map[string]domain.OutboxMessage
	commands     map[string]domain.Command
	cmdKey       map[string]string
	thoughts     map[string][]domain.Thought
	caps         map[string][]domain.CapabilityRef
}

type attempt struct {
	ID             string     `json:"id"`
	RunID          string     `json:"run_id"`
	StepID         string     `json:"step_id"`
	AttemptNo      int        `json:"attempt_no"`
	WorkerID       string     `json:"worker_id"`
	FenceToken     int64      `json:"fence_token"`
	Status         string     `json:"status"`
	IdempotencyKey string     `json:"idempotency_key"`
	StartedAt      time.Time  `json:"started_at"`
	EndedAt        *time.Time `json:"ended_at,omitempty"`
	ErrorCode      string     `json:"error_code,omitempty"`
	ErrorMessage   string     `json:"error_message,omitempty"`
}

type profileRec struct {
	TenantID string          `json:"tenant_id"`
	ID       string          `json:"id"`
	Version  int64           `json:"version"`
	Status   string          `json:"status"`
	Strategy string          `json:"strategy"`
	Spec     json.RawMessage `json:"spec"`
	Default  bool            `json:"default"`
}

type flowInput struct {
	RunID   string          `json:"run_id"`
	NodeID  string          `json:"node_id"`
	StepID  string          `json:"step_id"`
	ActorID string          `json:"actor_id"`
	Value   json.RawMessage `json:"value"`
}

type approvalRec struct {
	Request  domain.ApprovalRequest `json:"request"`
	RunID    string                 `json:"run_id"`
	StepID   string                 `json:"step_id"`
	IntentID string                 `json:"intent_id"`
	TenantID string                 `json:"tenant_id"`
}

type decisionRec struct {
	ID         string `json:"id"`
	ApprovalID string `json:"approval_id"`
	ApproverID string `json:"approver_id"`
	Decision   string `json:"decision"`
	ActionHash string `json:"action_hash"`
	CommandID  string `json:"command_id"`
}

type grantRec struct {
	ID                string    `json:"id"`
	ApprovalID        string    `json:"approval_id"`
	ActionHash        string    `json:"action_hash"`
	UsedCount         int       `json:"used_count"`
	MaxUses           int       `json:"max_uses"`
	ExpiresAt         time.Time `json:"expires_at"`
	ConsumedByAttempt string    `json:"consumed_by_attempt,omitempty"`
}

type kvRec struct {
	Key       domain.KVKey `json:"key"`
	Data      []byte       `json:"data"`
	SchemaID  string       `json:"schema_id"`
	Version   int64        `json:"version"`
	Deleted   bool         `json:"deleted,omitempty"`
	ExpiresAt *time.Time   `json:"expires_at,omitempty"`
}

type effectRec struct {
	Key         string    `json:"key"`
	State       string    `json:"state"`
	ResultRef   string    `json:"result_ref,omitempty"`
	LastCheckAt time.Time `json:"last_check_at"`
}

// Open opens or creates the SQLite database and loads the working set.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("local store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS objects (
		kind TEXT NOT NULL,
		id TEXT NOT NULL,
		parent TEXT NOT NULL DEFAULT '',
		seq INTEGER NOT NULL DEFAULT 0,
		data BLOB NOT NULL,
		PRIMARY KEY (kind, id)
	)`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS objects_parent ON objects(parent, kind, seq)`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS event_index (
		run_id TEXT NOT NULL, seq INTEGER NOT NULL, id TEXT NOT NULL, type TEXT NOT NULL,
		step_id TEXT NOT NULL DEFAULT '', file TEXT NOT NULL, offset INTEGER NOT NULL, length INTEGER NOT NULL,
		occurred_at TEXT NOT NULL, PRIMARY KEY (run_id, seq)
	)`); err != nil {
		db.Close()
		return nil, err
	}
	eventDir := filepath.Join(filepath.Dir(path), "events")
	if err := os.MkdirAll(eventDir, 0o700); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{
		db: db, eventDir: eventDir, runs: map[string]domain.Run{}, runByKey: map[string]string{}, steps: map[string]domain.Step{},
		attempts: map[string]attempt{}, events: map[string][]eventIndex{}, eventCache: map[string]domain.Event{},
		eventSeq: map[string]int64{}, sessions: map[string]domain.Session{},
		sessionKey: map[string]string{}, branches: map[string]domain.Branch{}, turns: map[string]domain.Turn{},
		messages: map[string][]domain.Message{}, profiles: map[string]profileRec{}, flows: map[string]domain.FlowSnapshot{},
		flowInputs: map[string]flowInput{}, intents: map[string]domain.ActionIntent{}, intentStep: map[string]string{}, approvals: map[string]approvalRec{},
		decisions: map[string][]decisionRec{}, grants: map[string]grantRec{}, kv: map[string]kvRec{},
		effects: map[string]effectRec{}, outbox: map[string]domain.OutboxMessage{}, commands: map[string]domain.Command{},
		cmdKey: map[string]string{}, thoughts: map[string][]domain.Thought{},
		caps: map[string][]domain.CapabilityRef{},
	}
	if err := s.load(); err != nil {
		db.Close()
		return nil, err
	}
	s.bootstrapEvents()
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) load() error {
	rows, err := s.db.Query(`SELECT kind, id, data FROM objects`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var kind, id string
		var data []byte
		if err := rows.Scan(&kind, &id, &data); err != nil {
			return err
		}
		if err := s.loadObject(kind, id, data); err != nil {
			return fmt.Errorf("load %s %s: %w", kind, id, err)
		}
	}
	return rows.Err()
}

func (s *Store) loadObject(kind, id string, data []byte) error {
	switch kind {
	case "run":
		var v domain.Run
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.runs[v.ID] = v
	case "step":
		var v domain.Step
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.steps[v.ID] = v
	case "attempt":
		var v attempt
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.attempts[v.ID] = v
	case "event":
		var v domain.Event
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.legacyEvents = append(s.legacyEvents, v)
	case "session":
		var v domain.Session
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.sessions[v.ID] = v
	case "branch":
		var v domain.Branch
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.branches[v.ID] = v
	case "turn":
		var v domain.Turn
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.turns[v.ID] = v
	case "message":
		var v domain.Message
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.messages[v.BranchID] = append(s.messages[v.BranchID], v)
	case "profile":
		var v profileRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.profiles[profileKey(v.TenantID, v.ID, v.Version)] = v
	case "flow":
		var v domain.FlowSnapshot
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.flows[v.RunID] = v
	case "flowinput":
		var v flowInput
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.flowInputs[flowInputKey(v.RunID, v.NodeID)] = v
	case "intent":
		var v domain.ActionIntent
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.intents[v.ID] = v
	case "intentstep":
		var v struct {
			StepID   string `json:"step_id"`
			IntentID string `json:"intent_id"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.intentStep[v.StepID] = v.IntentID
	case "approval":
		var v approvalRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.approvals[v.Request.ID] = v
	case "decision":
		var v decisionRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.decisions[v.ApprovalID] = append(s.decisions[v.ApprovalID], v)
	case "grant":
		var v grantRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.grants[v.ID] = v
	case "kv":
		var v kvRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.kv[kvKey(v.Key)] = v
	case "effect":
		var v effectRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.effects[v.Key] = v
	case "outbox":
		var v domain.OutboxMessage
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.outbox[v.ID] = v
	case "command":
		var v domain.Command
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.commands[v.ID] = v
	}
	return nil
}

func (s *Store) put(kind, id, parent string, seq int64, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	_, _ = s.db.Exec(`INSERT INTO objects(kind,id,parent,seq,data) VALUES(?,?,?,?,?)
		ON CONFLICT(kind,id) DO UPDATE SET parent=excluded.parent, seq=excluded.seq, data=excluded.data`,
		kind, id, parent, seq, data)
}

func (s *Store) del(kind, id string) {
	_, _ = s.db.Exec(`DELETE FROM objects WHERE kind=? AND id=?`, kind, id)
}

func newID() string  { return uuid.NewString() }
func now() time.Time { return time.Now().UTC() }
func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
func strptr(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
func int64ptr(v int64) *int64 { return &v }
func profileKey(tenant, id string, version int64) string {
	return tenant + "/" + id + "@" + fmt.Sprint(version)
}
func flowInputKey(runID, node string) string { return runID + "/" + node }
func kvKey(k domain.KVKey) string            { return k.TenantID + "/" + k.Scope + "/" + k.OwnerID + "/" + k.Name }

func (s *Store) saveRun(r domain.Run) {
	if cur, ok := s.runs[r.ID]; ok {
		// Callers often hold a copy from before appendEvent/insertStep updated
		// the row. Never let a stale copy rewind the monotonic counters.
		if r.LastEventSeq < cur.LastEventSeq {
			r.LastEventSeq = cur.LastEventSeq
		}
		if r.NextStepSeq < cur.NextStepSeq {
			r.NextStepSeq = cur.NextStepSeq
		}
		if r.FenceToken < cur.FenceToken {
			r.FenceToken = cur.FenceToken
		}
		if r.Version < cur.Version {
			r.Version = cur.Version
		}
	}
	s.runs[r.ID] = r
	s.put("run", r.ID, "", 0, r)
}

func (s *Store) saveStep(st domain.Step) {
	s.steps[st.ID] = st
	s.put("step", st.ID, st.RunID, st.Seq, st)
}

func (s *Store) saveSession(v domain.Session) {
	s.sessions[v.ID] = v
	s.put("session", v.ID, v.TenantID, 0, v)
}

func (s *Store) saveBranch(v domain.Branch) {
	s.branches[v.ID] = v
	s.put("branch", v.ID, v.SessionID, v.LastTurnSeq, v)
}

func (s *Store) saveTurn(v domain.Turn) {
	s.turns[v.ID] = v
	s.put("turn", v.ID, v.BranchID, v.Seq, v)
}

func (s *Store) saveMessage(v domain.Message) {
	s.messages[v.BranchID] = append(s.messages[v.BranchID], v)
	s.put("message", v.ID, v.BranchID, v.Seq, v)
}

func (s *Store) saveFlow(v domain.FlowSnapshot) {
	s.flows[v.RunID] = v
	s.put("flow", v.RunID, v.RunID, 0, v)
}

func (s *Store) saveApproval(v approvalRec) {
	s.approvals[v.Request.ID] = v
	s.put("approval", v.Request.ID, v.RunID, 0, v)
}

func (s *Store) saveIntent(v domain.ActionIntent) {
	s.intents[v.ID] = v
	s.put("intent", v.ID, "", 0, v)
}

func (s *Store) saveGrant(v grantRec) {
	s.grants[v.ID] = v
	s.put("grant", v.ID, v.ApprovalID, 0, v)
}

func (s *Store) saveKV(v kvRec) {
	s.kv[kvKey(v.Key)] = v
	s.put("kv", kvKey(v.Key), v.Key.OwnerID, v.Version, v)
}

func (s *Store) saveEffect(v effectRec) {
	s.effects[v.Key] = v
	s.put("effect", v.Key, "", 0, v)
}

func (s *Store) appendEvent(tenant, runID, stepID, attemptID, typ, actorType, actorID string, payload any) {
	var p json.RawMessage
	if payload != nil {
		p, _ = json.Marshal(payload)
	}
	r := s.runs[runID]
	seq := s.eventSeq[runID] + 1
	s.eventSeq[runID] = seq
	if r.LastEventSeq < seq {
		r.LastEventSeq = seq
	}
	s.runs[runID] = r
	ev := domain.Event{TenantID: tenant, RunID: runID, Seq: seq, ID: newID(), Type: typ,
		ActorType: actorType, Payload: p, OccurredAt: now(), CommittedAt: now()}
	if stepID != "" {
		ev.StepID = &stepID
	}
	if attemptID != "" {
		ev.AttemptID = &attemptID
	}
	if actorID != "" {
		ev.ActorID = &actorID
	}
	s.appendEventLogLocked(ev)
	s.saveRun(r)
}

func sortedSteps(steps map[string]domain.Step, runID string) []domain.Step {
	var out []domain.Step
	for _, st := range steps {
		if st.RunID == runID {
			out = append(out, st)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

func sortedMessages(msgs map[string][]domain.Message, branchID string) []domain.Message {
	out := append([]domain.Message(nil), msgs[branchID]...)
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

func (s *Store) tenantOfRun(runID string) string {
	if r, ok := s.runs[runID]; ok {
		return r.TenantID
	}
	return tenantLocal
}

func (s *Store) stepByID(id string) (domain.Step, error) {
	st, ok := s.steps[id]
	if !ok {
		return domain.Step{}, domain.ErrNotFound
	}
	return st, nil
}

func (s *Store) runByID(id string) (domain.Run, error) {
	r, ok := s.runs[id]
	if !ok {
		return domain.Run{}, domain.ErrNotFound
	}
	return r, nil
}

func (s *Store) profileSpec(tenant, id string, version int64) (json.RawMessage, error) {
	p, ok := s.profiles[profileKey(tenant, id, version)]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return p.Spec, nil
}

func (s *Store) sortedProfiles(tenant string) []profileRec {
	var out []profileRec
	for _, p := range s.profiles {
		if p.TenantID == tenant {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID == out[j].ID {
			return out[i].Version < out[j].Version
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (s *Store) sessionSummary(sess domain.Session) pg.SessionSummary {
	var last = sess.CreatedAt
	var turns, active int64
	for _, t := range s.turns {
		if t.SessionID == sess.ID {
			turns++
			if t.CreatedAt.After(last) {
				last = t.CreatedAt
			}
			if t.Status == domain.TurnQueued || t.Status == domain.TurnRunning {
				active++
			}
		}
	}
	return pg.SessionSummary{Session: sess, LastActivityAt: last, Turns: turns, ActiveTurns: active}
}

func (s *Store) commandKey(tenant, runID, key string) string {
	if key == "" {
		return ""
	}
	return tenant + "/" + runID + "/" + key
}

func (s *Store) saveCommand(c domain.Command) {
	s.commands[c.ID] = c
	s.put("command", c.ID, c.RunID, 0, c)
}

func (s *Store) claimOutbox(limit int) []domain.OutboxMessage {
	var out []domain.OutboxMessage
	for _, m := range s.outbox {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (s *Store) saveOutbox(m domain.OutboxMessage) {
	s.outbox[m.ID] = m
	s.put("outbox", m.ID, m.TenantID, 0, m)
}

func (s *Store) deleteOutbox(id string) { delete(s.outbox, id); s.del("outbox", id) }

func (s *Store) hasPrefix(s1, prefix string) bool { return strings.HasPrefix(s1, prefix) }
