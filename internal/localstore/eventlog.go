package localstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
)

func eventCapKey(runID string, seq int64) string {
	return fmt.Sprintf("%s:%020d", runID, seq)
}

// RebuildEventIndex rebuilds SQLite lookup indexes from authoritative event
// objects. Legacy JSONL archives never replace committed events.
func (s *Store) RebuildEventIndex(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, name := range []string{"objects_parent", "event_sequence", "event_identity"} {
		if _, err := tx.ExecContext(ctx, "REINDEX "+name); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) listEventsLocked(ctx context.Context, runID string, afterSeq int64, limit int) ([]domain.Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT data FROM objects WHERE parent=? AND kind='event' AND seq>? ORDER BY seq LIMIT ?`, runID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.Event, 0)
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var ev domain.Event
		if err := json.Unmarshal(data, &ev); err != nil {
			return nil, fmt.Errorf("decode event for run %s: %w", runID, err)
		}
		if ev.RunID != runID || ev.TenantID != s.runs[runID].TenantID {
			return nil, fmt.Errorf("event owner mismatch for run %s", runID)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
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
