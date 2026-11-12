package localstore

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
)

// SchemaVersion is the local database layout this build understands.
//
// The store keeps JSON objects in a few generic tables, so most changes need
// no migration. When one does, bump SchemaVersion and append a function to
// schemaMigrations: it upgrades from version index+1 to index+2. An upgrade
// first copies the database file to <db>.v<old>.bak.
const SchemaVersion = 1

// currentSchema is SchemaVersion; a variable so tests can simulate a later build.
var currentSchema = SchemaVersion

// schemaMigrations[i] upgrades version i+1 to i+2 (empty while version 1 is
// current). A variable so tests can install migrations.
var schemaMigrations []func(*sql.DB) error

// ErrNewerSchema marks a database written by a newer harness.
var ErrNewerSchema = errors.New("local database was written by a newer harness")

// Upgrade describes a schema upgrade performed by Open.
type Upgrade struct {
	From, To int
	Backup   string
}

// ensureSchema records and checks the schema version. A database without a
// version is a pre-versioning v1 database. A newer one is refused rather
// than risk silently mangling data the old code cannot read.
func ensureSchema(db *sql.DB, path string) (*Upgrade, error) {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		return nil, err
	}
	cur := 1
	var raw string
	switch err := db.QueryRow(`SELECT value FROM meta WHERE key='schema_version'`).Scan(&raw); {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return nil, err
	default:
		v, perr := strconv.Atoi(raw)
		if perr != nil || v < 1 {
			return nil, fmt.Errorf("local database has an invalid schema version %q", raw)
		}
		cur = v
	}
	if cur > currentSchema {
		return nil, fmt.Errorf("%w (schema v%d, this build reads v%d): upgrade the harness or restore a backup",
			ErrNewerSchema, cur, currentSchema)
	}
	var up *Upgrade
	if cur < currentSchema {
		up = &Upgrade{From: cur, To: currentSchema, Backup: fmt.Sprintf("%s.v%d.bak", path, cur)}
		// Fold the WAL into the main file so the copy is complete.
		_, _ = db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
		if err := copyFile(path, up.Backup); err != nil {
			return nil, fmt.Errorf("backup before schema upgrade: %w", err)
		}
		for v := cur; v < currentSchema; v++ {
			if v-1 >= len(schemaMigrations) || schemaMigrations[v-1] == nil {
				return nil, fmt.Errorf("no migration from schema v%d", v)
			}
			if err := schemaMigrations[v-1](db); err != nil {
				return nil, fmt.Errorf("migrate local database v%d→v%d (backup at %s): %w", v, v+1, up.Backup, err)
			}
		}
	}
	if _, err := db.Exec(`INSERT INTO meta(key,value) VALUES('schema_version',?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, strconv.Itoa(currentSchema)); err != nil {
		return nil, err
	}
	return up, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// Upgraded reports the schema upgrade Open performed, if any.
func (s *Store) Upgraded() *Upgrade { return s.upgrade }

// Snapshot writes a consistent copy of the database to dst (which must not
// exist), safe while the store is in use.
func (s *Store) Snapshot(dst string) error {
	if _, err := os.Stat(dst); err == nil {
		return fmt.Errorf("%s already exists", dst)
	}
	_, err := s.db.Exec(`VACUUM INTO ?`, dst)
	return err
}

// Checkpoint folds the write-ahead log into the database file.
func (s *Store) Checkpoint() error {
	_, err := s.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}
