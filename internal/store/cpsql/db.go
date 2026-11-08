// Package cpsql persists the v2 agent control plane (works, event inbox,
// watches, world state, actions, outcomes, evidence) on PostgreSQL (team
// mode) and SQLite (local mode) with one implementation over database/sql.
//
// The dialects differ only where they must: placeholders, row locks
// (FOR UPDATE / SKIP LOCKED on PostgreSQL; SQLite serializes writers through
// one connection), and timestamp encoding (TIMESTAMPTZ vs. integer
// microseconds, so ordering comparisons are exact in both). The same
// contract tests run against both stores.
//
// Every state change runs in one transaction that also appends the Work
// timeline event, so state and history never diverge. All queries are
// tenant-scoped; PostgreSQL additionally enforces row-level security.
package cpsql

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"go-sre-harness/internal/cpkit"
)

// Dialect selects SQL differences.
type Dialect int

const (
	Postgres Dialect = iota
	SQLite
)

// Store implements the control-plane store ports.
type Store struct {
	db    *sql.DB
	d     Dialect
	Clock cpkit.Clock
	// TxRetries re-runs transactions PostgreSQL rolled back (deadlock,
	// serialization failure, lock timeout).
	TxRetries int
	ownsDB    bool
}

// OpenPostgres wraps an existing pool. The schema comes from the versioned
// migrations (0020–0024), applied by migrations.Apply.
func OpenPostgres(pool *pgxpool.Pool) *Store {
	return &Store{db: stdlib.OpenDBFromPool(pool), d: Postgres, TxRetries: 3, ownsDB: true}
}

//go:embed sqlite/*.sql
var sqliteFiles embed.FS

// OpenSQLite opens (creating if needed) a SQLite control-plane database and
// applies its schema migrations.
func OpenSQLite(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("sqlite path is required")
	}
	dsn := path + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(FULL)"
	if path == ":memory:" {
		dsn = "file::memory:?_pragma=foreign_keys(1)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection: SQLite has one writer, and a single connection makes
	// every transaction strictly serial (the equivalent of row locks).
	db.SetMaxOpenConns(1)
	s := &Store{db: db, d: SQLite, ownsDB: true}
	if err := s.migrateSQLite(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database handle.
func (s *Store) Close() error {
	if s.ownsDB {
		return s.db.Close()
	}
	return nil
}

// DB exposes the handle (tests, health checks).
func (s *Store) DB() *sql.DB { return s.db }

// Dialect reports the dialect.
func (s *Store) Dialect() Dialect { return s.d }

func (s *Store) migrateSQLite(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cp_schema_migrations (
		version TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		return err
	}
	entries, err := sqliteFiles.ReadDir("sqlite")
	if err != nil {
		return err
	}
	for _, e := range entries {
		version := strings.TrimSuffix(e.Name(), ".sql")
		var n int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM cp_schema_migrations WHERE version=?`, version).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			continue
		}
		body, err := sqliteFiles.ReadFile("sqlite/" + e.Name())
		if err != nil {
			return err
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			tx.Rollback()
			return fmt.Errorf("sqlite migration %s: %w", e.Name(), err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO cp_schema_migrations(version, applied_at) VALUES(?,?)`,
			version, time.Now().UnixMicro()); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) now() time.Time { return s.Clock.Now() }

// q rewrites ? placeholders into $n for PostgreSQL. Queries never contain a
// literal question mark.
func (s *Store) q(query string) string {
	if s.d != Postgres {
		return query
	}
	var b strings.Builder
	n := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			continue
		}
		b.WriteByte(query[i])
	}
	return b.String()
}

// forUpdate is the row-lock suffix (PostgreSQL only).
func (s *Store) forUpdate() string {
	if s.d == Postgres {
		return " FOR UPDATE"
	}
	return ""
}

// skipLocked is the queue-claim suffix (PostgreSQL only).
func (s *Store) skipLocked() string {
	if s.d == Postgres {
		return " FOR UPDATE SKIP LOCKED"
	}
	return ""
}

// t encodes a timestamp for the dialect.
func (s *Store) t(v time.Time) any {
	if s.d == SQLite {
		return v.UTC().UnixMicro()
	}
	return v.UTC()
}

// tp encodes an optional timestamp.
func (s *Store) tp(v *time.Time) any {
	if v == nil || v.IsZero() {
		return nil
	}
	return s.t(*v)
}

// js encodes a JSON value as text (accepted by JSONB and TEXT columns).
func js(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case json.RawMessage:
		if len(x) == 0 {
			return nil
		}
		return string(x)
	}
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	if string(b) == "null" {
		return nil
	}
	return string(b)
}

// ts scans a timestamp from either dialect.
type ts struct {
	T     time.Time
	Valid bool
}

func (x *ts) Scan(v any) error {
	switch t := v.(type) {
	case nil:
		x.Valid = false
	case time.Time:
		x.T, x.Valid = t.UTC(), true
	case int64:
		x.T, x.Valid = time.UnixMicro(t).UTC(), true
	case []byte:
		return x.parse(string(t))
	case string:
		return x.parse(t)
	default:
		return fmt.Errorf("cpsql: cannot scan %T into a timestamp", v)
	}
	return nil
}

func (x *ts) parse(s string) error {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		x.T, x.Valid = time.UnixMicro(n).UTC(), true
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999-07"} {
		if t, err := time.Parse(layout, s); err == nil {
			x.T, x.Valid = t.UTC(), true
			return nil
		}
	}
	return fmt.Errorf("cpsql: bad timestamp %q", s)
}

func (x ts) ptr() *time.Time {
	if !x.Valid {
		return nil
	}
	t := x.T
	return &t
}

// jb scans a JSON column (JSONB or TEXT) into raw bytes.
type jb []byte

func (x *jb) Scan(v any) error {
	switch t := v.(type) {
	case nil:
		*x = nil
	case []byte:
		*x = append((*x)[:0], t...)
	case string:
		*x = []byte(t)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return err
		}
		*x = b
	}
	return nil
}

// ns scans a nullable string.
type ns struct{ sql.NullString }

func (x ns) S() string { return x.String }

func nullStr(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// tx runs fn in a transaction, retrying PostgreSQL transactions the server
// guarantees were rolled back. fn must be re-runnable.
func (s *Store) tx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	retries := s.TxRetries
	for i := 0; ; i++ {
		err := s.tx1(ctx, fn)
		if err == nil || i >= retries || ctx.Err() != nil || !retryable(err) {
			return err
		}
		d := time.Duration(20<<i) * time.Millisecond
		select {
		case <-ctx.Done():
			return err
		case <-time.After(d):
		}
	}
}

func (s *Store) tx1(ctx context.Context, fn func(tx *sql.Tx) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	if s.d == Postgres {
		if _, err = tx.ExecContext(ctx, `SELECT set_config('lock_timeout','5000',true), set_config('statement_timeout','15000',true)`); err != nil {
			return err
		}
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func retryable(err error) bool {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "40001", "40P01", "55P03":
			return true
		}
	}
	return false
}

// isUnique reports a unique-constraint violation in either dialect.
func isUnique(err error, constraint string) bool {
	if err == nil {
		return false
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code == "23505" && (constraint == "" || strings.Contains(pe.ConstraintName, constraint))
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") && (constraint == "" || strings.Contains(msg, constraint))
}

type rowScanner interface {
	Scan(dest ...any) error
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func anys[T any](v []T) []any {
	out := make([]any, len(v))
	for i := range v {
		out[i] = v[i]
	}
	return out
}
