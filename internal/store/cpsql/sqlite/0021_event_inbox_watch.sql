-- SQLite mirror of migrations/0021_event_inbox_watch.sql (timestamps are unix microseconds,
-- JSON is TEXT). Keep both in sync; the contract tests run on both.
-- v2 control plane: unified event inbox and durable watches (RFC §6, §9).
CREATE TABLE event_inbox (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  source TEXT NOT NULL,
  external_event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  correlation_id TEXT,
  dedup_key TEXT,
  envelope TEXT NOT NULL,
  payload_ref TEXT,
  payload_hash TEXT,
  occurred_at INTEGER NOT NULL,
  observed_at INTEGER NOT NULL,
  received_at INTEGER NOT NULL,
  processing_state TEXT NOT NULL DEFAULT 'NEW'
    CHECK (processing_state IN ('NEW','PROCESSED','IGNORED','FAILED','DEAD')),
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  work_id TEXT,
  decision TEXT,
  updated_at INTEGER NOT NULL,
  UNIQUE (tenant_id, source, external_event_id)
);
CREATE INDEX idx_event_inbox_pending ON event_inbox (received_at) WHERE processing_state IN ('NEW','FAILED');
CREATE INDEX idx_event_inbox_subject_time ON event_inbox (tenant_id, subject_ref, occurred_at DESC);
CREATE INDEX idx_event_inbox_type_time ON event_inbox (tenant_id, event_type, occurred_at DESC);
CREATE INDEX idx_event_inbox_work ON event_inbox (tenant_id, work_id) WHERE work_id IS NOT NULL;

CREATE TABLE work_watches (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('event','time','either','periodic','approval')),
  predicate TEXT,
  subjects TEXT,
  purpose TEXT,
  generation INTEGER NOT NULL,
  cursor TEXT,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','FIRED','EXPIRED','CANCELLED')),
  fired_by TEXT,
  checks INTEGER NOT NULL DEFAULT 0,
  interval_ms INTEGER NOT NULL DEFAULT 0,
  not_before INTEGER NOT NULL,
  next_check_at INTEGER,
  deadline INTEGER,
  version INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_watches_due ON work_watches (next_check_at) WHERE status = 'ACTIVE' AND next_check_at IS NOT NULL;
CREATE INDEX idx_watches_event ON work_watches (tenant_id, mode) WHERE status = 'ACTIVE';
CREATE INDEX idx_watches_work ON work_watches (tenant_id, work_id, status);

-- One firing per (watch, trigger): an event id, or a deterministic timer id
-- per check, so a re-scanned due watch never wakes a Work twice.
CREATE TABLE watch_firings (
  tenant_id TEXT NOT NULL,
  watch_id TEXT NOT NULL,
  firing_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT,
  fired_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, watch_id, firing_id),
  FOREIGN KEY (tenant_id, watch_id) REFERENCES work_watches (tenant_id, id)
);

