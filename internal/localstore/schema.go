package localstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// SchemaVersion is the local database layout this build understands.
//
// The store keeps JSON objects in a few generic tables, so most changes need
// no migration. When one does, bump SchemaVersion and append a function to
// schemaMigrations: it upgrades from version index+1 to index+2. An upgrade
// first takes a consistent SQLite snapshot; old binaries reject the new version.
const SchemaVersion = 2

// currentSchema is SchemaVersion; a variable so tests can simulate a later build.
var currentSchema = SchemaVersion

// schemaMigrations[i] upgrades version i+1 to i+2 inside the version transaction.
var schemaMigrations = []func(context.Context, *sql.Tx, string) error{migrateLocalEvents}

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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		return nil, err
	}
	cur := 1
	versioned := true
	var raw string
	switch err := db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='schema_version'`).Scan(&raw); {
	case errors.Is(err, sql.ErrNoRows):
		versioned = false
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
	if cur == currentSchema {
		return nil, nil
	}
	var objects, indexes int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects`).Scan(&objects); err != nil {
		return nil, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_index`).Scan(&indexes); err != nil {
		return nil, err
	}
	if versioned || objects > 0 || indexes > 0 {
		backup := fmt.Sprintf("%s.v%d.bak", path, cur)
		if _, err := os.Stat(backup); err == nil {
			backup = fmt.Sprintf("%s.v%d.%d.bak", path, cur, time.Now().UnixNano())
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if err := snapshotDatabase(ctx, db, backup); err != nil {
			return nil, fmt.Errorf("backup before schema upgrade: %w", err)
		}
		up = &Upgrade{From: cur, To: currentSchema, Backup: backup}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	for v := cur; v < currentSchema; v++ {
		if v-1 >= len(schemaMigrations) || schemaMigrations[v-1] == nil {
			return nil, fmt.Errorf("no migration from schema v%d", v)
		}
		if err := schemaMigrations[v-1](ctx, tx, path); err != nil {
			return nil, fmt.Errorf("migrate local database v%d to v%d: %w", v, v+1, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('schema_version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, strconv.Itoa(currentSchema)); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return up, nil
}

func snapshotDatabase(ctx context.Context, db *sql.DB, dst string) error {
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `VACUUM INTO ?`, dst); err != nil {
		return errors.Join(err, os.Remove(dst))
	}
	f, err = os.OpenFile(dst, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

// Upgraded reports the schema upgrade Open performed, if any.
func (s *Store) Upgraded() *Upgrade { return s.upgrade }

// Snapshot writes a consistent copy of the database to dst (which must not
// exist), safe while the store is in use.
func (s *Store) Snapshot(dst string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return snapshotDatabase(ctx, s.db, dst)
}

// Checkpoint folds the write-ahead log into the database file.
func (s *Store) Checkpoint() error {
	_, err := s.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}
