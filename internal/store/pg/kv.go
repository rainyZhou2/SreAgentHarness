package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/artifact"
)

// Agent KV (§6, §13.4): versioned, CAS-protected deterministic state. Not to
// be confused with a model's attention KV cache (ADR-013).

var (
	ErrKVNotFound      = errors.New("kv: not found") // absent, deleted or expired
	ErrKVAlreadyExists = errors.New("kv: already exists")
	ErrKVConflict      = errors.New("kv: version conflict")
	ErrKVTooLarge      = errors.New("kv: value too large for inline storage")
)

// MaxInlineValue bounds inline values. MVP: larger values are rejected rather
// than offloaded to the object store (RFC §13.4 allows value_ref for those).
const MaxInlineValue = 64 << 10

type KVProvenance struct {
	EventID   *string `json:"event_id,omitempty"`
	RunID     *string `json:"run_id,omitempty"`
	StepID    *string `json:"step_id,omitempty"`
	ActorType *string `json:"actor_type,omitempty"`
	ActorID   *string `json:"actor_id,omitempty"`
}

type KVValue struct {
	Data       []byte       `json:"data"`
	Version    int64        `json:"version"`
	Hash       string       `json:"hash"`
	SchemaID   *string      `json:"schema_id,omitempty"`
	ExpiresAt  *time.Time   `json:"expires_at,omitempty"`
	Provenance KVProvenance `json:"provenance"`
}

type kvProvenance struct {
	RunID, StepID, ActorType, ActorID string
}

func validScope(s string) bool {
	switch s {
	case "run", "incident", "agent", "service", "organization":
		return true
	}
	return false
}

func kvWrite(ctx context.Context, q querier, w KVWrite, prov kvProvenance) (int64, error) {
	if !validScope(w.Key.Scope) || w.Key.TenantID == "" || w.Key.OwnerID == "" || w.Key.Name == "" {
		return 0, fmt.Errorf("kv: invalid key")
	}
	if len(w.Data) > MaxInlineValue {
		return 0, ErrKVTooLarge
	}
	var expires *time.Time
	if w.TTL > 0 {
		t := time.Now().Add(w.TTL)
		expires = &t
	}
	hash := artifact.Hash(w.Data)
	k := w.Key
	var version int64
	var err error
	switch {
	case w.Delete:
		if w.ExpectedVersion == nil {
			return 0, fmt.Errorf("kv: delete requires expected version")
		}
		err = q.QueryRow(ctx, `UPDATE agent_kv SET tombstone=TRUE, value_inline=NULL, value_ref=NULL, version=version+1,
			updated_at=now() WHERE tenant_id=$1 AND scope=$2 AND owner_id=$3 AND key=$4 AND version=$5
			AND NOT tombstone AND (expires_at IS NULL OR expires_at > now()) RETURNING version`,
			k.TenantID, k.Scope, k.OwnerID, k.Name, *w.ExpectedVersion).Scan(&version)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrKVConflict
		}
	case w.ExpectedVersion != nil:
		err = q.QueryRow(ctx, `UPDATE agent_kv SET value_inline=$5, value_ref=NULL, value_hash=$6, schema_id=$7,
			expires_at=$8, version=version+1, updated_at=now()
			WHERE tenant_id=$1 AND scope=$2 AND owner_id=$3 AND key=$4 AND version=$9
			AND NOT tombstone AND (expires_at IS NULL OR expires_at > now()) RETURNING version`,
			k.TenantID, k.Scope, k.OwnerID, k.Name, w.Data, hash, nullable(w.SchemaID), expires, *w.ExpectedVersion).Scan(&version)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrKVConflict
		}
	default:
		// Create, or revive a deleted/expired key keeping its version sequence monotonic.
		err = q.QueryRow(ctx, `INSERT INTO agent_kv (tenant_id, scope, owner_id, key, value_inline, value_hash, schema_id,
			expires_at, version) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1)
			ON CONFLICT (tenant_id, scope, owner_id, key) DO UPDATE SET value_inline=EXCLUDED.value_inline,
			value_ref=NULL, value_hash=EXCLUDED.value_hash, schema_id=EXCLUDED.schema_id, expires_at=EXCLUDED.expires_at,
			tombstone=FALSE, version=agent_kv.version+1, updated_at=now()
			WHERE agent_kv.tombstone OR (agent_kv.expires_at IS NOT NULL AND agent_kv.expires_at <= now())
			RETURNING version`, k.TenantID, k.Scope, k.OwnerID, k.Name, w.Data, hash, nullable(w.SchemaID), expires).Scan(&version)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrKVAlreadyExists
		}
	}
	if err != nil {
		return 0, err
	}
	var data []byte
	var vhash *string
	if !w.Delete {
		data, vhash = w.Data, &hash
	}
	_, err = q.Exec(ctx, `INSERT INTO agent_kv_versions (tenant_id, scope, owner_id, key, version, value_inline, value_hash,
		schema_id, tombstone, run_id, step_id, actor_type, actor_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		k.TenantID, k.Scope, k.OwnerID, k.Name, version, data, vhash, nullable(w.SchemaID), w.Delete,
		uuidOrNil(prov.RunID), uuidOrNil(prov.StepID), nullable(prov.ActorType), nullable(prov.ActorID))
	return version, err
}

func uuidOrNil(s string) *string {
	if s == "" || !validID(s) {
		return nil
	}
	return &s
}

func (s *Store) KVPutIfAbsent(ctx context.Context, w KVWrite) (int64, error) {
	w.ExpectedVersion, w.Delete = nil, false
	return kvWrite(ctx, s.pool, w, kvProvenance{ActorType: "api"})
}

func (s *Store) KVCompareAndSwap(ctx context.Context, w KVWrite, expected int64) (int64, error) {
	w.ExpectedVersion, w.Delete = &expected, false
	return kvWrite(ctx, s.pool, w, kvProvenance{ActorType: "api"})
}

func (s *Store) KVDeleteIfVersion(ctx context.Context, k KVKey, expected int64) error {
	_, err := kvWrite(ctx, s.pool, KVWrite{Key: k, ExpectedVersion: &expected, Delete: true}, kvProvenance{ActorType: "api"})
	return err
}

// KVGet returns the live value; deleted or expired keys are ErrKVNotFound.
func (s *Store) KVGet(ctx context.Context, k KVKey) (KVValue, error) {
	var v KVValue
	err := s.pool.QueryRow(ctx, `SELECT k.value_inline, k.version, k.value_hash, k.schema_id, k.expires_at,
		h.event_id::text, h.run_id::text, h.step_id::text, h.actor_type, h.actor_id
		FROM agent_kv k LEFT JOIN agent_kv_versions h USING (tenant_id, scope, owner_id, key, version)
		WHERE k.tenant_id=$1 AND k.scope=$2 AND k.owner_id=$3 AND k.key=$4
		AND NOT k.tombstone AND (k.expires_at IS NULL OR k.expires_at > now())`,
		k.TenantID, k.Scope, k.OwnerID, k.Name).Scan(&v.Data, &v.Version, &v.Hash, &v.SchemaID, &v.ExpiresAt,
		&v.Provenance.EventID, &v.Provenance.RunID, &v.Provenance.StepID, &v.Provenance.ActorType, &v.Provenance.ActorID)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, ErrKVNotFound
	}
	return v, err
}

// KVGetAtVersion reads history; used to render pinned context snapshots.
func (s *Store) KVGetAtVersion(ctx context.Context, k KVKey, version int64) (KVValue, error) {
	var v KVValue
	var tomb bool
	var hash *string
	err := s.pool.QueryRow(ctx, `SELECT value_inline, version, value_hash, schema_id, tombstone,
		event_id::text, run_id::text, step_id::text, actor_type, actor_id FROM agent_kv_versions
		WHERE tenant_id=$1 AND scope=$2 AND owner_id=$3 AND key=$4 AND version=$5`,
		k.TenantID, k.Scope, k.OwnerID, k.Name, version).Scan(&v.Data, &v.Version, &hash, &v.SchemaID, &tomb,
		&v.Provenance.EventID, &v.Provenance.RunID, &v.Provenance.StepID, &v.Provenance.ActorType, &v.Provenance.ActorID)
	if errors.Is(err, pgx.ErrNoRows) || tomb {
		return v, ErrKVNotFound
	}
	if hash != nil {
		v.Hash = *hash
	}
	return v, err
}

type KVEntry struct {
	Name    string `json:"name"`
	Version int64  `json:"version"`
}

// KVList pages live keys of one (tenant, scope, owner); no cross-tenant scans.
func (s *Store) KVList(ctx context.Context, owner KVKey, prefix, after string, limit int) ([]KVEntry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT key, version FROM agent_kv WHERE tenant_id=$1 AND scope=$2 AND owner_id=$3
		AND key LIKE $4 AND key > $5 AND NOT tombstone AND (expires_at IS NULL OR expires_at > now())
		ORDER BY key LIMIT $6`, owner.TenantID, owner.Scope, owner.OwnerID,
		strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)+"%", after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KVEntry
	for rows.Next() {
		var e KVEntry
		if err := rows.Scan(&e.Name, &e.Version); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// SnapshotKVVersions returns the KV versions pinned by a context snapshot.
func (s *Store) SnapshotKVVersions(ctx context.Context, tenantID, snapshotID string) (map[string]int64, error) {
	v, err := s.GetSnapshot(ctx, tenantID, snapshotID)
	if err != nil {
		return nil, err
	}
	return v.KVVersions, nil
}

// KVDataAt returns the value bytes of a historical version.
func (s *Store) KVDataAt(ctx context.Context, k KVKey, version int64) ([]byte, error) {
	v, err := s.KVGetAtVersion(ctx, k, version)
	return v.Data, err
}
