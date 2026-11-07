package localstore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go-sre-harness/internal/domain"
)

// eventIndex is the SQLite query projection of one JSONL event. The JSONL
// file is the source of truth; this row only points at the bytes.
type eventIndex struct {
	RunID      string `json:"run_id"`
	Seq        int64  `json:"seq"`
	ID         string `json:"id"`
	Type       string `json:"type"`
	StepID     string `json:"step_id,omitempty"`
	File       string `json:"file"`
	Offset     int64  `json:"offset"`
	Length     int64  `json:"length"`
	OccurredAt string `json:"occurred_at"`
}

func eventCapKey(runID string, seq int64) string {
	return fmt.Sprintf("%s:%020d", runID, seq)
}

// appendEvent is called with the store mutex held. The event payload is
// appended to JSONL first; SQLite receives only the byte-range index. A crash
// between the two is repaired by RebuildEventIndex.
func (s *Store) appendEventLogLocked(ev domain.Event) {
	rel := filepath.Join(safePart(ev.TenantID), safePart(ev.RunID)+".jsonl")
	path := filepath.Join(s.eventDir, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return
	}
	line, err := json.Marshal(ev)
	if err != nil {
		f.Close()
		return
	}
	line = append(line, '\n')
	if _, err := f.Write(line); err != nil {
		f.Close()
		return
	}
	_ = f.Sync()
	_ = f.Close()
	idx := eventIndex{RunID: ev.RunID, Seq: ev.Seq, ID: ev.ID, Type: ev.Type, StepID: deref(ev.StepID),
		File: rel, Offset: off, Length: int64(len(line)), OccurredAt: ev.OccurredAt.Format("2006-01-02T15:04:05.999999999Z07:00")}
	s.events[ev.RunID] = append(s.events[ev.RunID], idx)
	s.eventCache[eventCapKey(ev.RunID, ev.Seq)] = ev
	s.putEventIndex(idx)
}

func (s *Store) putEventIndex(idx eventIndex) {
	_, _ = s.db.Exec(`INSERT INTO event_index(run_id,seq,id,type,step_id,file,offset,length,occurred_at)
		VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id,seq) DO UPDATE SET id=excluded.id,type=excluded.type,
		step_id=excluded.step_id,file=excluded.file,offset=excluded.offset,length=excluded.length,
		occurred_at=excluded.occurred_at`, idx.RunID, idx.Seq, idx.ID, idx.Type, idx.StepID, idx.File,
		idx.Offset, idx.Length, idx.OccurredAt)
}

func (s *Store) loadEventIndex() error {
	rows, err := s.db.Query(`SELECT run_id,seq,id,type,step_id,file,offset,length,occurred_at FROM event_index`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var idx eventIndex
		if err := rows.Scan(&idx.RunID, &idx.Seq, &idx.ID, &idx.Type, &idx.StepID, &idx.File,
			&idx.Offset, &idx.Length, &idx.OccurredAt); err != nil {
			return err
		}
		s.events[idx.RunID] = append(s.events[idx.RunID], idx)
		if idx.Seq > s.eventSeq[idx.RunID] {
			s.eventSeq[idx.RunID] = idx.Seq
		}
	}
	return rows.Err()
}

func (s *Store) bootstrapEvents() {
	if err := s.loadEventIndex(); err != nil {
		return
	}
	if len(s.events) > 0 {
		return
	}
	if len(s.legacyEvents) > 0 {
		// Migration from the first local-store build, which put the full event
		// JSON in SQLite. Re-emit to JSONL and remove the old object rows.
		for _, ev := range s.legacyEvents {
			s.appendEventLogLocked(ev)
			s.del("event", fmt.Sprintf("%s:%020d", ev.RunID, ev.Seq))
		}
		return
	}
	_ = s.RebuildEventIndex(context.Background())
}

// RebuildEventIndex discards the SQLite projection and rebuilds it from the
// JSONL files. It is safe to run at any time; JSONL is authoritative.
func (s *Store) RebuildEventIndex(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.db.Exec(`DELETE FROM event_index`); err != nil {
		return err
	}
	s.events = map[string][]eventIndex{}
	s.eventCache = map[string]domain.Event{}
	s.eventSeq = map[string]int64{}
	if s.eventDir == "" {
		return nil
	}
	return filepath.WalkDir(s.eventDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), ".jsonl") {
			return err
		}
		rel, err := filepath.Rel(s.eventDir, path)
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64<<10), 8<<20)
		var off int64
		for sc.Scan() {
			line := sc.Bytes()
			var ev domain.Event
			if json.Unmarshal(line, &ev) == nil && ev.RunID != "" && ev.Seq > 0 {
				idx := eventIndex{RunID: ev.RunID, Seq: ev.Seq, ID: ev.ID, Type: ev.Type, StepID: deref(ev.StepID),
					File: rel, Offset: off, Length: int64(len(line)) + 1,
					OccurredAt: ev.OccurredAt.Format("2006-01-02T15:04:05.999999999Z07:00")}
				s.events[ev.RunID] = append(s.events[ev.RunID], idx)
				s.eventCache[eventCapKey(ev.RunID, ev.Seq)] = ev
				if ev.Seq > s.eventSeq[ev.RunID] {
					s.eventSeq[ev.RunID] = ev.Seq
				}
				s.putEventIndex(idx)
			}
			off += int64(len(line)) + 1
		}
		return sc.Err()
	})
}

func (s *Store) readEvent(idx eventIndex) (domain.Event, error) {
	if ev, ok := s.eventCache[eventCapKey(idx.RunID, idx.Seq)]; ok {
		return ev, nil
	}
	f, err := os.Open(filepath.Join(s.eventDir, idx.File))
	if err != nil {
		return domain.Event{}, err
	}
	defer f.Close()
	buf := make([]byte, idx.Length)
	if _, err := f.ReadAt(buf, idx.Offset); err != nil && err != io.EOF {
		return domain.Event{}, err
	}
	var ev domain.Event
	if err := json.Unmarshal(bytesTrimLine(buf), &ev); err != nil {
		return domain.Event{}, err
	}
	s.eventCache[eventCapKey(idx.RunID, idx.Seq)] = ev
	return ev, nil
}

func (s *Store) listEventsLocked(runID string, afterSeq int64, limit int) ([]domain.Event, error) {
	idx := s.events[runID]
	sort.Slice(idx, func(i, j int) bool { return idx[i].Seq < idx[j].Seq })
	out := make([]domain.Event, 0, len(idx))
	for _, x := range idx {
		if x.Seq <= afterSeq {
			continue
		}
		ev, err := s.readEvent(x)
		if err != nil {
			return nil, fmt.Errorf("read event %s#%d: %w", runID, x.Seq, err)
		}
		out = append(out, ev)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Store) reconcileRunSeqs() {
	for id, seq := range s.eventSeq {
		if r, ok := s.runs[id]; ok && r.LastEventSeq < seq {
			r.LastEventSeq = seq
			s.runs[id] = r
			s.put("run", r.ID, "", 0, r)
		}
	}
}

func safePart(s string) string {
	s = strings.ReplaceAll(s, "..", "_")
	s = strings.ReplaceAll(s, string(os.PathSeparator), "_")
	s = strings.ReplaceAll(s, "/", "_")
	if s == "" {
		return "_"
	}
	return s
}

func bytesTrimLine(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}
