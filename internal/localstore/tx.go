package localstore

import (
	"encoding/json"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/extensions"
	"go-sre-harness/internal/store/pg"
)

// The local store keeps its working set in memory and mirrors every object to
// SQLite. Commands must be atomic: a rejected command may not leave partial
// state behind, and a command that fails midway must not persist half of its
// writes. stateBuf buffers object writes and events for the duration of one
// command; the caller commits or rolls the buffer back together with the
// in-memory snapshot.

type putOp struct {
	kind   string
	id     string
	parent string
	seq    int64
	data   []byte
}

type pendingEvent struct {
	tenant    string
	runID     string
	stepID    string
	attemptID string
	typ       string
	actorType string
	actorID   string
	payload   json.RawMessage
	at        time.Time
}

type stateBuf struct {
	puts   map[string]putOp
	dels   map[string]struct{}
	events []pendingEvent
}

func objectKey(kind, id string) string { return kind + "\x00" + id }

func splitObjectKey(key string) (string, string) {
	if i := strings.IndexByte(key, 0); i >= 0 {
		return key[:i], key[i+1:]
	}
	return key, ""
}

func newStateBuf() *stateBuf {
	return &stateBuf{puts: map[string]putOp{}, dels: map[string]struct{}{}}
}

// stateSnapshot is a shallow copy of every map a command can mutate. Values
// are structs; handlers replace values instead of mutating shared pointees, so
// copying the map is enough to restore the previous state.
type stateSnapshot struct {
	runs        map[string]domain.Run
	runByKey    map[string]string
	steps       map[string]domain.Step
	attempts    map[string]attempt
	events      map[string][]eventIndex
	eventCache  map[string]domain.Event
	eventSeq    map[string]int64
	sessions    map[string]domain.Session
	sessionKey  map[string]string
	branches    map[string]domain.Branch
	turns       map[string]domain.Turn
	messages    map[string][]domain.Message
	profiles    map[string]profileRec
	flows       map[string]domain.FlowSnapshot
	flowInputs  map[string]flowInput
	intents     map[string]domain.ActionIntent
	intentStep  map[string]string
	approvals   map[string]approvalRec
	decisions   map[string][]decisionRec
	grants      map[string]grantRec
	kv          map[string]kvRec
	effects     map[string]effectRec
	outbox      map[string]domain.OutboxMessage
	commands    map[string]domain.Command
	cmdKey      map[string]string
	thoughts    map[string][]domain.Thought
	caps        map[string][]domain.CapabilityRef
	extensions  map[string]extensions.Record
	toolOff     map[string]map[string]bool
	outputRevs  map[string][]pg.OutputRevision
	modelReqs   map[string]modelReqRec
	checkpoints map[string]checkpointRec
	memories    map[string]memoryRec
	memHits     map[string][]memoryHitRec
	compactions map[string]compactRec
}

func cloneMap[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneSliceMap[K comparable, V any](m map[K][]V) map[K][]V {
	out := make(map[K][]V, len(m))
	for k, v := range m {
		out[k] = append([]V(nil), v...)
	}
	return out
}

func cloneNestedMap[K comparable, V comparable](m map[K]map[V]bool) map[K]map[V]bool {
	out := make(map[K]map[V]bool, len(m))
	for k, v := range m {
		out[k] = cloneMap(v)
	}
	return out
}

func (s *Store) snapshotStateLocked() *stateSnapshot {
	return &stateSnapshot{
		runs: cloneMap(s.runs), runByKey: cloneMap(s.runByKey), steps: cloneMap(s.steps), attempts: cloneMap(s.attempts),
		events: cloneSliceMap(s.events), eventCache: cloneMap(s.eventCache), eventSeq: cloneMap(s.eventSeq),
		sessions: cloneMap(s.sessions), sessionKey: cloneMap(s.sessionKey), branches: cloneMap(s.branches),
		turns: cloneMap(s.turns), messages: cloneSliceMap(s.messages), profiles: cloneMap(s.profiles),
		flows: cloneMap(s.flows), flowInputs: cloneMap(s.flowInputs), intents: cloneMap(s.intents),
		intentStep: cloneMap(s.intentStep), approvals: cloneMap(s.approvals), decisions: cloneSliceMap(s.decisions),
		grants: cloneMap(s.grants), kv: cloneMap(s.kv), effects: cloneMap(s.effects), outbox: cloneMap(s.outbox),
		commands: cloneMap(s.commands), cmdKey: cloneMap(s.cmdKey), thoughts: cloneSliceMap(s.thoughts),
		caps: cloneSliceMap(s.caps), extensions: cloneMap(s.extensions), toolOff: cloneNestedMap(s.toolOff),
		outputRevs: cloneSliceMap(s.outputRevs), modelReqs: cloneMap(s.modelReqs),
		checkpoints: cloneMap(s.checkpoints), memories: cloneMap(s.memories), memHits: cloneSliceMap(s.memHits),
		compactions: cloneMap(s.compactions),
	}
}

func (s *Store) restoreStateLocked(sn *stateSnapshot) {
	s.runs, s.runByKey, s.steps, s.attempts = sn.runs, sn.runByKey, sn.steps, sn.attempts
	s.events, s.eventCache, s.eventSeq = sn.events, sn.eventCache, sn.eventSeq
	s.sessions, s.sessionKey, s.branches = sn.sessions, sn.sessionKey, sn.branches
	s.turns, s.messages, s.profiles = sn.turns, sn.messages, sn.profiles
	s.flows, s.flowInputs, s.intents, s.intentStep = sn.flows, sn.flowInputs, sn.intents, sn.intentStep
	s.approvals, s.decisions, s.grants = sn.approvals, sn.decisions, sn.grants
	s.kv, s.effects, s.outbox = sn.kv, sn.effects, sn.outbox
	s.commands, s.cmdKey = sn.commands, sn.cmdKey
	s.thoughts, s.caps, s.extensions, s.toolOff = sn.thoughts, sn.caps, sn.extensions, sn.toolOff
	s.outputRevs, s.modelReqs = sn.outputRevs, sn.modelReqs
	s.checkpoints, s.memories, s.memHits = sn.checkpoints, sn.memories, sn.memHits
	s.compactions = sn.compactions
}

func (s *Store) execPut(op putOp) {
	_, _ = s.db.Exec(`INSERT INTO objects(kind,id,parent,seq,data) VALUES(?,?,?,?,?)
		ON CONFLICT(kind,id) DO UPDATE SET parent=excluded.parent, seq=excluded.seq, data=excluded.data`,
		op.kind, op.id, op.parent, op.seq, op.data)
}

// commitBufferLocked writes buffered objects first and then the events. Event
// appends re-save their run with the advanced event cursor, so the order
// matters for LastEventSeq.
func (s *Store) commitBufferLocked(buf *stateBuf) {
	s.buf = nil
	for _, op := range buf.puts {
		s.execPut(op)
	}
	for key := range buf.dels {
		kind, id := splitObjectKey(key)
		_, _ = s.db.Exec(`DELETE FROM objects WHERE kind=? AND id=?`, kind, id)
	}
	for _, e := range buf.events {
		s.appendEventNow(e.tenant, e.runID, e.stepID, e.attemptID, e.typ, e.actorType, e.actorID, e.payload, e.at)
	}
}
