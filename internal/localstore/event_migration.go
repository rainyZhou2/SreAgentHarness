package localstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go-sre-harness/internal/domain"
)

func migrateLocalEvents(ctx context.Context, tx *sql.Tx, path string) error {
	runs, err := migrationRuns(ctx, tx)
	if err != nil {
		return err
	}
	if err := validateStoredEvents(ctx, tx, runs); err != nil {
		return err
	}
	for _, query := range []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS event_sequence ON objects(parent,seq) WHERE kind='event'`,
		`CREATE UNIQUE INDEX IF NOT EXISTS event_identity ON objects(json_extract(data,'$.id')) WHERE kind='event'`,
	} {
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	for _, r := range runs {
		rel := filepath.Join(safePart(r.TenantID), safePart(r.ID)+".jsonl")
		file := filepath.Join(filepath.Dir(path), "events", rel)
		if err := importEventFile(ctx, tx, file, r); err != nil {
			return err
		}
		if err := verifyLegacyIndex(ctx, tx, r, rel); err != nil {
			return err
		}
		var count, maxSeq int64
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(seq),0) FROM objects WHERE kind='event' AND parent=?`, r.ID).Scan(&count, &maxSeq); err != nil {
			return err
		}
		if count != maxSeq || maxSeq < r.LastEventSeq {
			return fmt.Errorf("incomplete event history for run %s: count=%d cursor=%d last=%d", r.ID, count, r.LastEventSeq, maxSeq)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET data=json_set(data,'$.last_event_seq',?) WHERE kind='run' AND id=?`, maxSeq, r.ID); err != nil {
			return err
		}
	}
	var orphan int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_index e LEFT JOIN objects r ON r.kind='run' AND r.id=e.run_id WHERE r.id IS NULL`).Scan(&orphan); err != nil {
		return err
	}
	if orphan != 0 {
		return fmt.Errorf("legacy event index contains %d orphan events", orphan)
	}
	return nil
}

func migrationRuns(ctx context.Context, tx *sql.Tx) (map[string]domain.Run, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id,data FROM objects WHERE kind='run'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]domain.Run{}
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}
		var r domain.Run
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, err
		}
		if r.ID != id || r.ID == "" || r.TenantID == "" || r.LastEventSeq < 0 {
			return nil, fmt.Errorf("invalid run identity or cursor: %s", id)
		}
		out[id] = r
	}
	return out, rows.Err()
}

func validateStoredEvents(ctx context.Context, tx *sql.Tx, runs map[string]domain.Run) error {
	rows, err := tx.QueryContext(ctx, `SELECT id,parent,seq,data FROM objects WHERE kind='event'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, parent string
		var seq int64
		var data []byte
		if err := rows.Scan(&id, &parent, &seq, &data); err != nil {
			return err
		}
		var ev domain.Event
		if err := json.Unmarshal(data, &ev); err != nil {
			return err
		}
		if err := validateMigratedEvent(ev, runs[parent]); err != nil {
			return err
		}
		if ev.RunID != parent || ev.Seq != seq || id != eventCapKey(ev.RunID, ev.Seq) {
			return fmt.Errorf("invalid stored event key: %s", id)
		}
	}
	return rows.Err()
}

func validateMigratedEvent(ev domain.Event, r domain.Run) error {
	if r.ID == "" || ev.RunID != r.ID || ev.TenantID != r.TenantID || ev.ID == "" || ev.Type == "" || ev.Seq < 1 || ev.OccurredAt.IsZero() || ev.CommittedAt.IsZero() {
		return fmt.Errorf("invalid event identity for run %s sequence %d", r.ID, ev.Seq)
	}
	return nil
}

func importEventFile(ctx context.Context, tx *sql.Tx, path string, r domain.Run) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	for record := 1; ; record++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var data json.RawMessage
		if err := decoder.Decode(&data); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return fmt.Errorf("decode legacy event %s record %d: %w", path, record, err)
		}
		var ev domain.Event
		if err := json.Unmarshal(data, &ev); err != nil {
			return err
		}
		if err := validateMigratedEvent(ev, r); err != nil {
			return err
		}
		key := eventCapKey(ev.RunID, ev.Seq)
		if _, err := tx.ExecContext(ctx, `INSERT INTO objects(kind,id,parent,seq,data) VALUES('event',?,?,?,?) ON CONFLICT(kind,id) DO NOTHING`, key, ev.RunID, ev.Seq, data); err != nil {
			return err
		}
		var stored []byte
		if err := tx.QueryRowContext(ctx, `SELECT data FROM objects WHERE kind='event' AND id=?`, key).Scan(&stored); err != nil {
			return err
		}
		canonicalLeft, err := canonicalEventJSON(stored)
		if err != nil {
			return err
		}
		canonicalRight, err := canonicalEventJSON(data)
		if err != nil {
			return err
		}
		if !bytes.Equal(canonicalLeft, canonicalRight) {
			return fmt.Errorf("conflicting legacy event for run %s sequence %d", r.ID, ev.Seq)
		}
	}
}

func verifyLegacyIndex(ctx context.Context, tx *sql.Tx, r domain.Run, file string) error {
	rows, err := tx.QueryContext(ctx, `SELECT e.seq,e.id,e.type,e.step_id,e.file,e.occurred_at,o.data FROM event_index e LEFT JOIN objects o ON o.kind='event' AND o.parent=e.run_id AND o.seq=e.seq WHERE e.run_id=?`, r.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int64
		var id, typ, step, rel, occurred string
		var data []byte
		if err := rows.Scan(&seq, &id, &typ, &step, &rel, &occurred, &data); err != nil {
			return err
		}
		var ev domain.Event
		if len(data) == 0 {
			return fmt.Errorf("missing indexed event %s#%d", r.ID, seq)
		}
		if err := json.Unmarshal(data, &ev); err != nil {
			return err
		}
		at, err := time.Parse(time.RFC3339Nano, occurred)
		if err != nil {
			return err
		}
		if ev.ID != id || ev.Type != typ || deref(ev.StepID) != step || rel != file || !ev.OccurredAt.Equal(at) {
			return fmt.Errorf("conflicting event index %s#%d", r.ID, seq)
		}
	}
	return rows.Err()
}

// Preserve exact numbers when comparing opaque payloads such as target versions.
func canonicalEventJSON(data []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}
