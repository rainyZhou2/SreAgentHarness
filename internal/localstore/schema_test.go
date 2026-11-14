package localstore

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSchemaVersionLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "harness.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.Upgraded() != nil {
		t.Fatal("a fresh database is not an upgrade")
	}
	s.Close()
	s, err = Open(path) // reopen: same version, no upgrade
	if err != nil || s.Upgraded() != nil {
		t.Fatalf("%v %v", err, s.Upgraded())
	}
	s.Close()
}

func TestLegacyDatabaseWithoutMetaOpens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "harness.db")
	s, _ := Open(path)
	if _, err := s.db.Exec(`DROP TABLE meta`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err := Open(path)
	if err != nil {
		t.Fatalf("pre-versioning database must open: %v", err)
	}
	s.Close()
}

func TestNewerSchemaIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "harness.db")
	s, _ := Open(path)
	if _, err := s.db.Exec(`UPDATE meta SET value='99' WHERE key='schema_version'`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err := Open(path); !errors.Is(err, ErrNewerSchema) {
		t.Fatalf("want ErrNewerSchema, got %v", err)
	}
}

func TestSchemaUpgradeBacksUpAndMigrates(t *testing.T) {
	// Simulate the next schema version.
	saved := schemaMigrations
	defer func() { schemaMigrations = saved }()
	path := filepath.Join(t.TempDir(), "harness.db")
	s, _ := Open(path)
	s.Close()

	ran := false
	schemaMigrations = append(append([]func(context.Context, *sql.Tx, string) error(nil), saved...), func(context.Context, *sql.Tx, string) error { ran = true; return nil })
	up, err := ensureSchemaFor(t, path, SchemaVersion+1)
	if err != nil || up == nil || up.From != SchemaVersion || up.To != SchemaVersion+1 || !ran {
		t.Fatalf("upgrade: %+v %v ran=%v", up, err, ran)
	}
	if _, err := os.Stat(up.Backup); err != nil {
		t.Fatalf("backup must exist: %v", err)
	}
}

// ensureSchemaFor runs ensureSchema as if the build were at version want.
func ensureSchemaFor(t *testing.T, path string, want int) (*Upgrade, error) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	old := currentSchema
	currentSchema = want
	defer func() { currentSchema = old }()
	return ensureSchema(db, path)
}
