// Package localstore persists local runtime state and execution events in SQLite.
// A single process owns the in-memory working set and serializes mutations.
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

	"go-sre-harness/internal/curate"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/extensions"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/store/pg"
)

type Store struct {
	db      *sql.DB
	upgrade *Upgrade

	mu  sync.Mutex
	buf *stateBuf // non-nil while one command is being applied atomically

	runs            map[string]domain.Run
	runByKey        map[string]string // tenant/idempotency -> run id
	steps           map[string]domain.Step
	attempts        map[string]attempt
	eventDir        string
	sessions        map[string]domain.Session
	sessionKey      map[string]string
	branches        map[string]domain.Branch
	turns           map[string]domain.Turn
	messages        map[string][]domain.Message
	profiles        map[string]profileRec
	flows           map[string]domain.FlowSnapshot
	flowInputs      map[string]flowInput
	intents         map[string]domain.ActionIntent
	intentStep      map[string]string
	approvals       map[string]approvalRec
	decisions       map[string][]decisionRec
	grants          map[string]grantRec
	kv              map[string]kvRec
	effects         map[string]effectRec
	outbox          map[string]domain.OutboxMessage
	commands        map[string]domain.Command
	cmdKey          map[string]string
	thoughts        map[string][]domain.Thought
	caps            map[string][]domain.CapabilityRef
	extensions      map[string]extensions.Record
	toolOff         map[string]map[string]bool
	outputRevs      map[string][]pg.OutputRevision
	modelReqs       map[string]modelReqRec
	checkpoints     map[string]checkpointRec
	memories        map[string]memoryRec
	memOutcomes     map[string]bool // memory|work outcomes already counted
	memHits         map[string][]memoryHitRec
	curations       map[string]curate.Proposal
	reasoningScores map[string]reasoningScoreRec
	consolidation   map[string]consolidationRec
	compactions     map[string]compactRec
	shards          map[string][]domain.MemoryShard
	feedback        map[string][]memory.Feedback
	experiences     map[string][]pg.ExperienceView
	incidents       map[string]incidentRec
	incidentEvents  map[string][]pg.IncidentEvent
	serviceDeps     map[string]map[string]string
	capNodes        map[string]capNodeRec
	capRels         map[string]capRelRec
	datasets        map[string]evalDatasetRec
	evalCases       map[string]evalCaseRec
	evalRuns        map[string]evalRunRec
	candidates      map[string]evalCandidateRec
	rollouts        map[string]evalRolloutRec
	snapshots       map[string]string // context snapshot id -> run id
	triggerClaims   map[string]triggerClaimRec
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
	SnapshotID     string     `json:"snapshot_id,omitempty"`
	StartedAt      time.Time  `json:"started_at"`
	EndedAt        *time.Time `json:"ended_at,omitempty"`
	ErrorCode      string     `json:"error_code,omitempty"`
	ErrorMessage   string     `json:"error_message,omitempty"`
}

type profileRec struct {
	TenantID  string          `json:"tenant_id"`
	ID        string          `json:"id"`
	Version   int64           `json:"version"`
	Status    string          `json:"status"`
	Strategy  string          `json:"strategy"`
	Spec      json.RawMessage `json:"spec"`
	Default   bool            `json:"default"`
	CreatedAt time.Time       `json:"created_at,omitempty"`
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
	up, err := ensureSchema(db, path)
	if err != nil {
		db.Close()
		return nil, err
	}
	eventDir := filepath.Join(filepath.Dir(path), "events")
	if err := os.MkdirAll(eventDir, 0o700); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{
		db: db, upgrade: up, eventDir: eventDir, runs: map[string]domain.Run{}, runByKey: map[string]string{}, steps: map[string]domain.Step{},
		attempts: map[string]attempt{}, sessions: map[string]domain.Session{},
		sessionKey: map[string]string{}, branches: map[string]domain.Branch{}, turns: map[string]domain.Turn{},
		messages: map[string][]domain.Message{}, profiles: map[string]profileRec{}, flows: map[string]domain.FlowSnapshot{},
		flowInputs: map[string]flowInput{}, intents: map[string]domain.ActionIntent{}, intentStep: map[string]string{}, approvals: map[string]approvalRec{},
		decisions: map[string][]decisionRec{}, grants: map[string]grantRec{}, kv: map[string]kvRec{},
		effects: map[string]effectRec{}, outbox: map[string]domain.OutboxMessage{}, commands: map[string]domain.Command{},
		cmdKey: map[string]string{}, thoughts: map[string][]domain.Thought{},
		caps: map[string][]domain.CapabilityRef{}, extensions: map[string]extensions.Record{},
		toolOff: map[string]map[string]bool{}, outputRevs: map[string][]pg.OutputRevision{},
		modelReqs: map[string]modelReqRec{}, checkpoints: map[string]checkpointRec{},
		memories: map[string]memoryRec{}, memHits: map[string][]memoryHitRec{}, curations: map[string]curate.Proposal{}, reasoningScores: map[string]reasoningScoreRec{}, consolidation: map[string]consolidationRec{},
		compactions: map[string]compactRec{},
		shards:      map[string][]domain.MemoryShard{}, feedback: map[string][]memory.Feedback{},
		experiences: map[string][]pg.ExperienceView{}, incidents: map[string]incidentRec{},
		incidentEvents: map[string][]pg.IncidentEvent{}, serviceDeps: map[string]map[string]string{},
		capNodes: map[string]capNodeRec{}, capRels: map[string]capRelRec{},
		datasets: map[string]evalDatasetRec{}, evalCases: map[string]evalCaseRec{},
		evalRuns: map[string]evalRunRec{}, candidates: map[string]evalCandidateRec{},
		rollouts:  map[string]evalRolloutRec{},
		snapshots: map[string]string{}, triggerClaims: map[string]triggerClaimRec{},
	}
	if err := s.load(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) load() error {
	rows, err := s.db.Query(`SELECT kind, id, data FROM objects WHERE kind != 'event'`)
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
	case "thought":
		var v struct {
			StepID  string         `json:"step_id"`
			Thought domain.Thought `json:"thought"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.thoughts[v.StepID] = append(s.thoughts[v.StepID], v.Thought)
	case "capability":
		var v struct {
			StepID string               `json:"step_id"`
			Ref    domain.CapabilityRef `json:"ref"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.caps[v.StepID] = append(s.caps[v.StepID], v.Ref)
	case "outputrev":
		var v struct {
			StepID string            `json:"step_id"`
			Rev    pg.OutputRevision `json:"revision"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.outputRevs[v.StepID] = append(s.outputRevs[v.StepID], v.Rev)
	case "extension":
		var wrapper struct {
			TenantID string            `json:"tenant_id"`
			Record   extensions.Record `json:"record"`
		}
		if err := json.Unmarshal(data, &wrapper); err != nil {
			return err
		}
		v := wrapper.Record
		v.TenantID = wrapper.TenantID
		if v.Kind == "" {
			_ = json.Unmarshal(data, &v)
		}
		if v.TenantID == "" {
			if i := strings.IndexByte(id, '/'); i > 0 {
				v.TenantID = id[:i]
			}
		}
		s.extensions[extensionKey(v.TenantID, v.Kind, v.Name)] = v
	case "tooltoggle":
		var v struct {
			TenantID string `json:"tenant_id"`
			Name     string `json:"name"`
			Enabled  bool   `json:"enabled"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		if s.toolOff[v.TenantID] == nil {
			s.toolOff[v.TenantID] = map[string]bool{}
		}
		s.toolOff[v.TenantID][v.Name] = !v.Enabled
	case "modelreq":
		var v modelReqRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.modelReqs[v.ID] = v
	case "checkpoint":
		var v checkpointRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.checkpoints[v.ID] = v
	case "memory_outcome":
		if s.memOutcomes == nil {
			s.memOutcomes = map[string]bool{}
		}
		s.memOutcomes[id] = true
	case "memory":
		var v memoryRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.memories[v.Mem.ID] = v
	case "curation":
		var v curate.Proposal
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.curations[v.ID] = v
	case "memhit":
		var v struct {
			StepID string       `json:"step_id"`
			Hit    memoryHitRec `json:"hit"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.memHits[v.StepID] = append(s.memHits[v.StepID], v.Hit)
	case "compact":
		var v compactRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.compactions[v.StepID] = v
	case "shard":
		var v []domain.MemoryShard
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.shards[id] = v
	case "feedback":
		var v memory.Feedback
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		runID := id
		if i := strings.LastIndexByte(id, '/'); i > 0 {
			runID = id[:i]
		}
		s.feedback[runID] = append(s.feedback[runID], v)
	case "experience":
		var v struct {
			TenantID string            `json:"tenant_id"`
			View     pg.ExperienceView `json:"view"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.experiences[v.TenantID+"/"+v.View.RunID] = append(s.experiences[v.TenantID+"/"+v.View.RunID], v.View)
	case "incident":
		var v incidentRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.incidents[v.ID] = v
	case "incidentevent":
		var v pg.IncidentEvent
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		incidentID := id
		if i := strings.LastIndexByte(id, '/'); i > 0 {
			incidentID = id[:i]
		}
		s.incidentEvents[incidentID] = append(s.incidentEvents[incidentID], v)
	case "servicedep":
		var v struct {
			TenantID  string `json:"tenant_id"`
			Service   string `json:"service"`
			DependsOn string `json:"depends_on"`
			Relation  string `json:"relation"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		key := v.TenantID + "/" + v.Service
		if s.serviceDeps[key] == nil {
			s.serviceDeps[key] = map[string]string{}
		}
		s.serviceDeps[key][v.DependsOn] = v.Relation
	case "capnode":
		var v capNodeRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.capNodes[id] = v
	case "caprel":
		var v capRelRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.capRels[id] = v
	case "evaldataset":
		var v evalDatasetRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.datasets[v.Dataset.ID] = v
	case "evalcase":
		var v evalCaseRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.evalCases[v.Case.ID] = v
	case "evalrun":
		var v evalRunRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.evalRuns[v.Run.ID] = v
	case "evalcandidate":
		var v evalCandidateRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.candidates[v.Candidate.ID] = v
	case "evalrollout":
		var v evalRolloutRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.rollouts[v.Rollout.ID] = v
	case "cmdkey":
		var v struct {
			CommandID string `json:"command_id"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		if v.CommandID != "" {
			s.cmdKey[id] = v.CommandID
		}
	case "runkey":
		var v struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		if v.RunID != "" {
			s.runByKey[id] = v.RunID
		}
	case "triggerclaim":
		var v triggerClaimRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.triggerClaims[id] = v
	case "reasoningscore":
		var v reasoningScoreRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.reasoningScores[id] = v
	case "consolidation":
		var v consolidationRec
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		s.consolidation[id] = v
	}
	return nil
}

func (s *Store) put(kind, id, parent string, seq int64, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		if s.buf != nil {
			s.buf.err = errors.Join(s.buf.err, fmt.Errorf("marshal %s: %w", kind, err))
			return
		}
		panic(err)
	}
	if s.buf != nil {
		key := objectKey(kind, id)
		delete(s.buf.dels, key)
		s.buf.puts[key] = putOp{kind: kind, id: id, parent: parent, seq: seq, data: data}
		return
	}
	s.execPut(putOp{kind: kind, id: id, parent: parent, seq: seq, data: data})
}

func (s *Store) del(kind, id string) {
	if s.buf != nil {
		key := objectKey(kind, id)
		delete(s.buf.puts, key)
		s.buf.dels[key] = struct{}{}
		return
	}
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
	// Keep every version so a context snapshot can read the value it pinned,
	// not whatever the key holds later.
	s.put("kvver", kvKey(v.Key)+"\x00"+fmt.Sprint(v.Version), v.Key.OwnerID, v.Version, v)
}

func (s *Store) saveEffect(v effectRec) {
	s.effects[v.Key] = v
	s.put("effect", v.Key, "", 0, v)
}

func (s *Store) appendEvent(tenant, runID, stepID, attemptID, typ, actorType, actorID string, payload any) {
	if s.buf == nil {
		panic("localstore: event mutation requires a state transaction")
	}
	var p json.RawMessage
	if payload != nil {
		var err error
		p, err = json.Marshal(payload)
		if err != nil {
			s.buf.err = errors.Join(s.buf.err, fmt.Errorf("marshal event %s: %w", typ, err))
			return
		}
	}
	s.buf.events = append(s.buf.events, pendingEvent{tenant: tenant, runID: runID, stepID: stepID,
		attemptID: attemptID, typ: typ, actorType: actorType, actorID: actorID, payload: p, at: now()})
}

func (s *Store) bufferEventLocked(e pendingEvent) error {
	r, ok := s.runs[e.runID]
	if !ok || r.TenantID != e.tenant || e.typ == "" {
		return fmt.Errorf("invalid event owner or type for run %s", e.runID)
	}
	r.LastEventSeq++
	ev := domain.Event{TenantID: e.tenant, RunID: e.runID, Seq: r.LastEventSeq, ID: newID(), Type: e.typ,
		ActorType: e.actorType, Payload: e.payload, OccurredAt: e.at, CommittedAt: now(),
		StepID: strptr(e.stepID), AttemptID: strptr(e.attemptID), ActorID: strptr(e.actorID)}
	s.put("event", eventCapKey(ev.RunID, ev.Seq), ev.RunID, ev.Seq, ev)
	s.saveRun(r)
	return nil
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

func (s *Store) deleteOutbox(id string) { delete(s.outbox, id); s.del("outbox", id) }
