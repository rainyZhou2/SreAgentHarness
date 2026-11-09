package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go-sre-harness/internal/world"
)

var _ world.Store = (*Store)(nil)

// UpsertEntity creates or updates an entity (attributes replace the old
// ones; the version increments).
func (s *Store) UpsertEntity(ctx context.Context, e world.Entity, now time.Time) (world.Entity, error) {
	if err := e.Normalize(); err != nil {
		return e, err
	}
	if e.Attributes == nil {
		e.Attributes = map[string]string{}
	}
	e.UpdatedAt = now
	err := s.tx(ctx, func(tx *sql.Tx) error {
		var v int64
		err := tx.QueryRowContext(ctx, s.q(`SELECT version FROM world_entities WHERE tenant_id=? AND ref=?`+s.forUpdate()),
			e.TenantID, e.Ref).Scan(&v)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			e.Version = 1
			_, err = tx.ExecContext(ctx, s.q(`INSERT INTO world_entities (tenant_id, ref, kind, attributes, source, version, updated_at)
				VALUES (?,?,?,?,?,1,?)`), e.TenantID, e.Ref, e.Kind, js(e.Attributes), e.Source, s.t(now))
			if isUnique(err, "") {
				return errRetryCreate
			}
			return err
		case err != nil:
			return err
		}
		e.Version = v + 1
		_, err = tx.ExecContext(ctx, s.q(`UPDATE world_entities SET kind=?, attributes=?, source=?, version=?, updated_at=?
			WHERE tenant_id=? AND ref=?`), e.Kind, js(e.Attributes), e.Source, e.Version, s.t(now), e.TenantID, e.Ref)
		return err
	})
	if errors.Is(err, errRetryCreate) {
		return s.UpsertEntity(ctx, e, now)
	}
	return e, err
}

func scanEntity(r rowScanner) (world.Entity, error) {
	var e world.Entity
	var attrs jb
	var at ts
	err := r.Scan(&e.TenantID, &e.Ref, &e.Kind, &attrs, &e.Source, &e.Version, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return e, world.ErrNotFound
	}
	if err != nil {
		return e, err
	}
	_ = json.Unmarshal(attrs, &e.Attributes)
	e.UpdatedAt = at.T
	return e, nil
}

// GetEntity reads an entity by ref.
func (s *Store) GetEntity(ctx context.Context, tenantID, ref string) (world.Entity, error) {
	return scanEntity(s.db.QueryRowContext(ctx, s.q(`SELECT tenant_id, ref, kind, attributes, source, version, updated_at
		FROM world_entities WHERE tenant_id=? AND ref=?`), tenantID, ref))
}

var attrKeyRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)

// FindEntities finds entities whose attribute equals value.
func (s *Store) FindEntities(ctx context.Context, tenantID, attribute, value string, limit int) ([]world.Entity, error) {
	if !attrKeyRe.MatchString(attribute) {
		return nil, fmt.Errorf("%w: attribute %q", world.ErrInvalid, attribute)
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var rows *sql.Rows
	var err error
	if s.d == Postgres {
		rows, err = s.db.QueryContext(ctx, s.q(`SELECT tenant_id, ref, kind, attributes, source, version, updated_at
			FROM world_entities WHERE tenant_id=? AND attributes @> ?::jsonb ORDER BY ref LIMIT ?`),
			tenantID, js(map[string]string{attribute: value}), limit)
	} else {
		rows, err = s.db.QueryContext(ctx, `SELECT tenant_id, ref, kind, attributes, source, version, updated_at
			FROM world_entities WHERE tenant_id=? AND json_extract(attributes, ?) = ? ORDER BY ref LIMIT ?`,
			tenantID, "$."+attribute, value, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []world.Entity
	for rows.Next() {
		e, err := scanEntity(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

const factCols = `id, tenant_id, subject_ref, predicate, value, source, trust_tier, valid_from, valid_to, observed_at,
	closed_observed_at, confirmed_at, expires_at, evidence_ref, confidence`

func scanFact(r rowScanner) (world.Fact, error) {
	var f world.Fact
	var from, to, obs, closed, conf, exp ts
	var ev ns
	if err := r.Scan(&f.ID, &f.TenantID, &f.SubjectRef, &f.Predicate, &f.Value, &f.Source, &f.TrustTier, &from, &to, &obs,
		&closed, &conf, &exp, &ev, &f.Confidence); err != nil {
		return f, err
	}
	f.ValidFrom, f.ValidTo, f.ObservedAt, f.ClosedObservedAt, f.ExpiresAt = from.T, to.ptr(), obs.T, closed.ptr(), exp.ptr()
	f.ConfirmedAt = conf.ptr()
	f.EvidenceRef = ev.S()
	return f, nil
}

// AssertFact inserts a fact into its (subject, predicate, source) series:
// the interval in force at valid_from is closed there; a later interval
// bounds the new one. Re-asserting the value in force is a no-op.
func (s *Store) AssertFact(ctx context.Context, f world.Fact) (world.Fact, bool, error) {
	if err := f.Normalize(s.now()); err != nil {
		return f, false, err
	}
	var out world.Fact
	var created bool
	err := s.tx(ctx, func(tx *sql.Tx) error {
		created = false
		// Serialize writers of one series (PostgreSQL: advisory xact lock).
		if s.d == Postgres {
			if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`,
				f.TenantID+"|"+f.SubjectRef+"|"+f.Predicate+"|"+f.Source); err != nil {
				return err
			}
		}
		// The series' latest instant at or before f may hold several facts
		// (a same-instant conflict); all of them are considered, so the
		// outcome does not depend on row order.
		rows, err := tx.QueryContext(ctx, s.q(`SELECT `+factCols+` FROM world_facts WHERE tenant_id=? AND subject_ref=?
			AND predicate=? AND source=? AND valid_from = (SELECT MAX(valid_from) FROM world_facts WHERE tenant_id=?
			AND subject_ref=? AND predicate=? AND source=? AND valid_from <= ?) ORDER BY observed_at DESC, id`),
			f.TenantID, f.SubjectRef, f.Predicate, f.Source, f.TenantID, f.SubjectRef, f.Predicate, f.Source, s.t(f.ValidFrom))
		if err != nil {
			return err
		}
		var open []world.Fact
		for rows.Next() {
			p, err := scanFact(rows)
			if err != nil {
				rows.Close()
				return err
			}
			if p.ValidTo == nil || p.ValidTo.After(f.ValidFrom) {
				open = append(open, p)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		var match *world.Fact
		conflict := false
		for i := range open {
			if open[i].Value != f.Value {
				conflict = true
			} else if match == nil {
				match = &open[i]
			}
		}
		// Same value: a re-observation that refreshes freshness only. A
		// later observation of one side of a conflict resolves it instead
		// (below), so the stale side does not stay open.
		if match != nil && (!conflict || match.ValidFrom.Equal(f.ValidFrom)) {
			prev := *match
			if f.ObservedAt.After(prev.SeenBy(f.ObservedAt)) {
				if _, err := tx.ExecContext(ctx, s.q(`UPDATE world_facts SET confirmed_at=? WHERE id=?`),
					s.t(f.ObservedAt), prev.ID); err != nil {
					return err
				}
				c := f.ObservedAt
				prev.ConfirmedAt = &c
			}
			out = prev
			return nil
		}
		next, err := scanFact(tx.QueryRowContext(ctx, s.q(`SELECT `+factCols+` FROM world_facts WHERE tenant_id=? AND subject_ref=?
			AND predicate=? AND source=? AND valid_from > ? ORDER BY valid_from LIMIT 1`),
			f.TenantID, f.SubjectRef, f.Predicate, f.Source, s.t(f.ValidFrom)))
		if err == nil && f.ValidTo == nil {
			vt := next.ValidFrom
			f.ValidTo = &vt
			f.ClosedObservedAt = &f.ObservedAt
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		for _, prev := range open {
			if prev.ValidFrom.Equal(f.ValidFrom) {
				// Same instant, different value: keep both visible as a
				// conflict instead of rewriting history.
				continue
			}
			if _, err := tx.ExecContext(ctx, s.q(`UPDATE world_facts SET valid_to=?, closed_observed_at=? WHERE id=?`),
				s.t(f.ValidFrom), s.t(f.ObservedAt), prev.ID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO world_facts (`+factCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`),
			f.ID, f.TenantID, f.SubjectRef, f.Predicate, f.Value, f.Source, f.TrustTier, s.t(f.ValidFrom), s.tp(f.ValidTo),
			s.t(f.ObservedAt), s.tp(f.ClosedObservedAt), nil, s.tp(f.ExpiresAt), nullStr(f.EvidenceRef), f.Confidence); err != nil {
			return err
		}
		out, created = f, true
		return nil
	})
	return out, created, err
}

const relCols = `id, tenant_id, from_ref, relation_type, to_ref, source, valid_from, valid_to, observed_at, closed_observed_at`

func scanRel(r rowScanner) (world.Relation, error) {
	var x world.Relation
	var from, to, obs, closed ts
	if err := r.Scan(&x.ID, &x.TenantID, &x.FromRef, &x.Type, &x.ToRef, &x.Source, &from, &to, &obs, &closed); err != nil {
		return x, err
	}
	x.ValidFrom, x.ValidTo, x.ObservedAt, x.ClosedObservedAt = from.T, to.ptr(), obs.T, closed.ptr()
	return x, nil
}

// AssertRelation opens an edge unless one is already open for the same
// (from, type, to, source).
func (s *Store) AssertRelation(ctx context.Context, r world.Relation) (world.Relation, bool, error) {
	if err := r.Normalize(s.now()); err != nil {
		return r, false, err
	}
	var out world.Relation
	var created bool
	err := s.tx(ctx, func(tx *sql.Tx) error {
		created = false
		if s.d == Postgres {
			if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`,
				r.TenantID+"|"+r.FromRef+"|"+r.Type+"|"+r.ToRef+"|"+r.Source); err != nil {
				return err
			}
		}
		cur, err := scanRel(tx.QueryRowContext(ctx, s.q(`SELECT `+relCols+` FROM world_relations WHERE tenant_id=? AND from_ref=?
			AND relation_type=? AND to_ref=? AND source=? AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
			ORDER BY valid_from DESC LIMIT 1`), r.TenantID, r.FromRef, r.Type, r.ToRef, r.Source, s.t(r.ValidFrom), s.t(r.ValidFrom)))
		if err == nil {
			out = cur
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO world_relations (`+relCols+`) VALUES (?,?,?,?,?,?,?,?,?,?)`),
			r.ID, r.TenantID, r.FromRef, r.Type, r.ToRef, r.Source, s.t(r.ValidFrom), s.tp(r.ValidTo), s.t(r.ObservedAt),
			s.tp(r.ClosedObservedAt)); err != nil {
			return err
		}
		out, created = r, true
		return nil
	})
	return out, created, err
}

// RetractRelation closes the open interval of an edge.
func (s *Store) RetractRelation(ctx context.Context, tenantID, from, typ, to, source string, validTo, observedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE world_relations SET valid_to=?, closed_observed_at=? WHERE tenant_id=?
		AND from_ref=? AND relation_type=? AND to_ref=? AND source=? AND valid_to IS NULL AND valid_from < ?`),
		s.t(validTo), s.t(observedAt), tenantID, from, typ, to, source, s.t(validTo))
	return err
}

// visibility is the bitemporal predicate (see world.Fact.VisibleAt).
const visibility = ` AND observed_at <= ? AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ? OR closed_observed_at > ?)`

// FactsAt returns facts visible at (validAt, knownAt).
func (s *Store) FactsAt(ctx context.Context, tenantID string, subjects, predicates []string, validAt, knownAt time.Time) ([]world.Fact, error) {
	if len(subjects) == 0 {
		return nil, nil
	}
	q := `SELECT ` + factCols + ` FROM world_facts WHERE tenant_id=? AND subject_ref IN (` + placeholders(len(subjects)) + `)`
	args := append([]any{tenantID}, anys(subjects)...)
	if len(predicates) > 0 {
		q += ` AND predicate IN (` + placeholders(len(predicates)) + `)`
		args = append(args, anys(predicates)...)
	}
	q += visibility + ` ORDER BY subject_ref, predicate, source, valid_from`
	args = append(args, s.t(knownAt), s.t(validAt), s.t(validAt), s.t(knownAt))
	rows, err := s.db.QueryContext(ctx, s.q(q), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var all []world.Fact
	for rows.Next() {
		f, err := scanFact(rows)
		if err != nil {
			return nil, err
		}
		all = append(all, f)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Within one series, the latest-starting visible interval wins (an
	// interval closed after knownAt is still "in force" as known then, but
	// a newer interval known at knownAt supersedes it).
	latest := map[string]int{}
	out := all[:0:0]
	for _, f := range all {
		k := strings.Join([]string{f.SubjectRef, f.Predicate, f.Source}, "|")
		if i, ok := latest[k]; ok {
			if !f.ValidFrom.Before(out[i].ValidFrom) {
				if f.ValidFrom.Equal(out[i].ValidFrom) {
					out = append(out, f) // same-instant conflict: keep both
					continue
				}
				out[i] = f
			}
			continue
		}
		latest[k] = len(out)
		out = append(out, f)
	}
	return out, nil
}

// RelationsAt returns edges touching subjects visible at (validAt, knownAt).
func (s *Store) RelationsAt(ctx context.Context, tenantID string, subjects []string, validAt, knownAt time.Time) ([]world.Relation, error) {
	if len(subjects) == 0 {
		return nil, nil
	}
	ph := placeholders(len(subjects))
	q := `SELECT ` + relCols + ` FROM world_relations WHERE tenant_id=? AND (from_ref IN (` + ph + `) OR to_ref IN (` + ph + `))` +
		visibility + ` ORDER BY from_ref, relation_type, to_ref LIMIT 2000`
	args := append([]any{tenantID}, anys(subjects)...)
	args = append(args, anys(subjects)...)
	args = append(args, s.t(knownAt), s.t(validAt), s.t(validAt), s.t(knownAt))
	rows, err := s.db.QueryContext(ctx, s.q(q), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []world.Relation
	for rows.Next() {
		r, err := scanRel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SaveSnapshot persists an immutable snapshot.
func (s *Store) SaveSnapshot(ctx context.Context, snap world.Snapshot) error {
	_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO world_snapshots (id, tenant_id, as_of, known_at, subjects, body, digest, created_at)
		VALUES (?,?,?,?,?,?,?,?)`), snap.ID, snap.TenantID, s.t(snap.AsOf), s.t(snap.KnownAt), js(snap.Subjects), js(snap),
		snap.Digest, s.t(snap.CreatedAt))
	return err
}

// GetSnapshot reads a snapshot.
func (s *Store) GetSnapshot(ctx context.Context, tenantID, id string) (world.Snapshot, error) {
	var body jb
	err := s.db.QueryRowContext(ctx, s.q(`SELECT body FROM world_snapshots WHERE tenant_id=? AND id=?`), tenantID, id).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return world.Snapshot{}, world.ErrNotFound
	}
	if err != nil {
		return world.Snapshot{}, err
	}
	var snap world.Snapshot
	err = json.Unmarshal(body, &snap)
	return snap, err
}
