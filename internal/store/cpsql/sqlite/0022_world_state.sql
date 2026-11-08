-- SQLite mirror of migrations/0022_world_state.sql (timestamps are unix microseconds,
-- JSON is TEXT). Keep both in sync; the contract tests run on both.
-- v2 control plane: bitemporal world state (RFC §7). Facts and relations are
-- never updated in place except to close their valid-time interval; the
-- moment of closing is kept (closed_observed_at) so "as known at" queries
-- reproduce what the platform knew then. Different sources coexist.
CREATE TABLE world_entities (
  tenant_id TEXT NOT NULL,
  ref TEXT NOT NULL,
  kind TEXT NOT NULL,
  attributes TEXT NOT NULL DEFAULT '{}',
  source TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, ref)
);
CREATE INDEX idx_world_entities_attrs ON world_entities (tenant_id, kind);

CREATE TABLE world_facts (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  predicate TEXT NOT NULL,
  value TEXT NOT NULL,
  source TEXT NOT NULL,
  trust_tier TEXT NOT NULL,
  valid_from INTEGER NOT NULL,
  valid_to INTEGER,
  observed_at INTEGER NOT NULL,
  closed_observed_at INTEGER,
  confirmed_at INTEGER,
  expires_at INTEGER,
  evidence_ref TEXT,
  confidence REAL NOT NULL DEFAULT 1,
  CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE INDEX idx_world_facts_subject ON world_facts (tenant_id, subject_ref, predicate, valid_from DESC);
CREATE INDEX idx_world_facts_series ON world_facts (tenant_id, subject_ref, predicate, source, valid_from);

CREATE TABLE world_relations (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  from_ref TEXT NOT NULL,
  relation_type TEXT NOT NULL,
  to_ref TEXT NOT NULL,
  source TEXT NOT NULL,
  valid_from INTEGER NOT NULL,
  valid_to INTEGER,
  observed_at INTEGER NOT NULL,
  closed_observed_at INTEGER,
  CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE INDEX idx_world_relations_src ON world_relations (tenant_id, from_ref, relation_type, valid_from DESC);
CREATE INDEX idx_world_relations_dst ON world_relations (tenant_id, to_ref, valid_from DESC);

CREATE TABLE world_snapshots (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  as_of INTEGER NOT NULL,
  known_at INTEGER NOT NULL,
  subjects TEXT NOT NULL,
  body TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  UNIQUE (tenant_id, id)
);
CREATE INDEX idx_world_snapshots_digest ON world_snapshots (tenant_id, digest);

