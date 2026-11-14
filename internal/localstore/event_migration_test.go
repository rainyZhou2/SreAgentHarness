package localstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

func legacyEventFixture(t *testing.T) (string, string, []domain.Event) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "h.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		`CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)`,
		`INSERT INTO meta VALUES('schema_version','1')`,
		`CREATE TABLE objects(kind TEXT NOT NULL,id TEXT NOT NULL,parent TEXT NOT NULL DEFAULT '',seq INTEGER NOT NULL DEFAULT 0,data BLOB NOT NULL,PRIMARY KEY(kind,id))`,
		`CREATE TABLE event_index(run_id TEXT NOT NULL,seq INTEGER NOT NULL,id TEXT NOT NULL,type TEXT NOT NULL,step_id TEXT NOT NULL DEFAULT '',file TEXT NOT NULL,offset INTEGER NOT NULL,length INTEGER NOT NULL,occurred_at TEXT NOT NULL,PRIMARY KEY(run_id,seq))`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	r := domain.Run{ID: "run", TenantID: "t", Status: domain.RunQueued, Version: 1, LastEventSeq: 2}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO objects(kind,id,data) VALUES('run','run',?)`, data); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "events", "t", "run.jsonl")
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	events := make([]domain.Event, 3)
	var content []byte
	for i := range events {
		events[i] = domain.Event{ID: []string{"e1", "e2", "e3"}[i], TenantID: "t", RunID: "run", Seq: int64(i + 1), Type: "test.event", OccurredAt: time.Unix(int64(i+1), 0).UTC(), CommittedAt: time.Unix(int64(i+1), 0).UTC()}
		line, err := json.Marshal(events[i])
		if err != nil {
			t.Fatal(err)
		}
		line = append(line, '\n')
		if i < 2 {
			if _, err := db.Exec(`INSERT INTO event_index VALUES(?,?,?,?,?,?,?,?,?)`, "run", i+1, events[i].ID, events[i].Type, "", "t/run.jsonl", len(content), len(line), events[i].OccurredAt.Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
		}
		content = append(content, line...)
	}
	if err := os.WriteFile(file, content, 0600); err != nil {
		t.Fatal(err)
	}
	return path, file, events
}

func TestLocalEventMigrationIncludesUnindexedTail(t *testing.T) {
	path, file, want := legacyEventFixture(t)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.Upgraded() == nil || s.Upgraded().From != 1 || s.Upgraded().To != 2 {
		t.Fatalf("missing v2 migration: %+v", s.Upgraded())
	}
	got, err := s.ListEvents(context.Background(), "t", "run", 0, 100)
	if err != nil || len(got) != len(want) {
		t.Fatalf("tail missing: %d %v", len(got), err)
	}
	r, err := s.GetRun(context.Background(), "t", "run")
	if err != nil || r.LastEventSeq != 3 {
		t.Fatalf("cursor: %+v %v", r, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err = s.ListEvents(context.Background(), "t", "run", 0, 100)
	if err != nil || len(got) != 3 {
		t.Fatalf("migration still depends on JSONL: %d %v", len(got), err)
	}
}

func TestLocalEventMigrationRefusesIncompleteOrConflictingHistory(t *testing.T) {
	for _, failure := range []string{"missing file", "corrupt tail", "conflicting index", "foreign tenant", "gap"} {
		t.Run(failure, func(t *testing.T) {
			path, file, events := legacyEventFixture(t)
			switch failure {
			case "missing file":
				if err := os.Remove(file); err != nil {
					t.Fatal(err)
				}
			case "corrupt tail":
				f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.WriteString("{broken\n"); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			case "conflicting index":
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`UPDATE event_index SET id='conflict' WHERE seq=2`); err != nil {
					t.Fatal(err)
				}
				db.Close()
			case "foreign tenant", "gap":
				if failure == "foreign tenant" {
					events[2].TenantID = "other"
				} else {
					events[2].Seq = 4
				}
				var content []byte
				for _, ev := range events {
					b, err := json.Marshal(ev)
					if err != nil {
						t.Fatal(err)
					}
					content = append(content, append(b, '\n')...)
				}
				if err := os.WriteFile(file, content, 0600); err != nil {
					t.Fatal(err)
				}
			}
			s, err := Open(path)
			if err == nil {
				s.Close()
				t.Fatal("accepted damaged event history")
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var version string
			if err := db.QueryRow(`SELECT value FROM meta WHERE key='schema_version'`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := db.QueryRow(`SELECT COUNT(*) FROM objects WHERE kind='event'`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if version != "1" || count != 0 {
				t.Fatalf("partial migration survived: version=%s events=%d", version, count)
			}
		})
	}
}

func TestLocalEventMigrationDoesNotRoundConflictingNumbers(t *testing.T) {
	path, file, events := legacyEventFixture(t)
	events[0].Payload = json.RawMessage(`{"target_version":9007199254740992}`)
	conflicting := events[0]
	conflicting.Payload = json.RawMessage(`{"target_version":9007199254740993}`)
	events = append(events, conflicting)
	var content []byte
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		content = append(content, append(data, '\n')...)
	}
	if err := os.WriteFile(file, content, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err == nil {
		s.Close()
		t.Fatal("rounded distinct target versions into one event")
	}
}

func TestLocalEventMigrationAcceptsLargeLegacyEvents(t *testing.T) {
	path, file, events := legacyEventFixture(t)
	payload, err := json.Marshal(map[string]string{"evidence": strings.Repeat("x", 9<<20)})
	if err != nil {
		t.Fatal(err)
	}
	events[2].Payload = payload
	var content []byte
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		content = append(content, append(data, '\n')...)
	}
	if err := os.WriteFile(file, content, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.ListEvents(context.Background(), "t", "run", 2, 1)
	if err != nil || len(got) != 1 || !bytes.Equal(got[0].Payload, payload) {
		t.Fatalf("large legacy event lost: %d %v", len(got), err)
	}
}

func TestLocalEventMigrationRetryPreservesBackup(t *testing.T) {
	path, file, _ := legacyEventFixture(t)
	original, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, append(bytes.Clone(original), []byte("broken")...), 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(path); err == nil {
		s.Close()
		t.Fatal("corrupt archive migrated")
	}
	backup := path + ".v1.bak"
	before, err := os.ReadFile(backup)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, original, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	after, err := os.ReadFile(backup)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("retry overwrote original backup")
	}
	if s.Upgraded() == nil || s.Upgraded().Backup == backup {
		t.Fatal("retry must preserve first backup and record new snapshot")
	}
	if _, err := ensureSchemaFor(t, path, 1); !errors.Is(err, ErrNewerSchema) {
		t.Fatalf("old binary accepted schema: %v", err)
	}
	db, err := sql.Open("sqlite", s.Upgraded().Backup)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version string
	if err := db.QueryRow(`SELECT value FROM meta WHERE key='schema_version'`).Scan(&version); err != nil || version != "1" {
		t.Fatalf("backup is not pre-migration schema: %s %v", version, err)
	}
}

func TestLocalEventMigrationRetainsLegacySQLiteEvents(t *testing.T) {
	path, file, events := legacyEventFixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO objects(kind,id,parent,seq,data) VALUES('event',?,?,?,?)`, eventCapKey(ev.RunID, ev.Seq), ev.RunID, ev.Seq, data); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.ListEvents(context.Background(), "t", "run", 0, 100)
	expectedJSON, marshalErr := json.Marshal(events)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	actualJSON, marshalErr := json.Marshal(got)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if err != nil || !bytes.Equal(expectedJSON, actualJSON) {
		t.Fatalf("legacy SQLite payloads changed: %d %v", len(got), err)
	}
}

func TestLocalMigrationBackupIncludesPinnedWAL(t *testing.T) {
	path, _, _ := legacyEventFixture(t)
	writer, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	reader, err := writer.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	var n int
	if err := reader.QueryRow(`SELECT COUNT(*) FROM objects`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`INSERT INTO objects(kind,id,data) VALUES('test_marker','wal','{"value":"committed in WAL"}')`); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path + "-wal")
	if err != nil || info.Size() == 0 {
		t.Fatalf("fixture has no pending WAL: %v", err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Upgraded() == nil {
		t.Fatal("expected migration backup")
	}
	backup, err := sql.Open("sqlite", s.Upgraded().Backup)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	var data string
	if err := backup.QueryRow(`SELECT data FROM objects WHERE kind='test_marker' AND id='wal'`).Scan(&data); err != nil || data != `{"value":"committed in WAL"}` {
		t.Fatalf("backup lost committed WAL: %q %v", data, err)
	}
}
