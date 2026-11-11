-- SQLite mirror of migrations/0031_governance_quality.sql (timestamps are unix microseconds,
-- JSON is TEXT). Keep both in sync; the contract tests run on both.
-- v3: world collector health, correlation decisions, change ledger and the
-- runbook registry (design §9.4, §9.5, §10.4, §12.2).
-- Collector health: a collector that stops reporting never deletes facts;
-- its facts become stale and stop satisfying action preconditions.
CREATE TABLE world_collector_health (
  tenant_id TEXT NOT NULL,
  collector TEXT NOT NULL,
  last_success_at INTEGER,
  last_attempt_at INTEGER,
  last_full_sync INTEGER,
  last_error TEXT,
  error_count INTEGER NOT NULL DEFAULT 0,
  max_age_ms INTEGER NOT NULL DEFAULT 0,
  watch_cursor TEXT,
  source_version TEXT,
  updated_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, collector)
);

-- Why an event was routed where it went: the rule and its version, the
-- candidates considered and the facts the decision relied on.
CREATE TABLE event_correlation_decisions (
  tenant_id TEXT NOT NULL,
  inbox_id TEXT NOT NULL,
  rule_name TEXT,
  rule_version TEXT,
  decision TEXT NOT NULL,
  work_id TEXT,
  detail TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, inbox_id)
);
CREATE INDEX idx_event_decisions_work ON event_correlation_decisions (tenant_id, work_id, created_at DESC);

-- Change ledger: releases, config, topology and manual operations with the
-- subjects they touched. A change near a symptom is a hypothesis, not a cause.
CREATE TABLE change_records (
  tenant_id TEXT NOT NULL,
  change_id TEXT NOT NULL,
  category TEXT NOT NULL CHECK (category IN ('release','config','topology','manual')),
  source TEXT NOT NULL,
  actor TEXT,
  started_at INTEGER NOT NULL,
  ended_at INTEGER,
  record TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, change_id)
);
CREATE INDEX idx_change_records_time ON change_records (tenant_id, started_at DESC);
CREATE TABLE change_subjects (
  tenant_id TEXT NOT NULL,
  change_id TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, change_id, subject_ref),
  FOREIGN KEY (tenant_id, change_id) REFERENCES change_records (tenant_id, change_id) ON DELETE CASCADE
);
CREATE INDEX idx_change_subjects_ref ON change_subjects (tenant_id, subject_ref, started_at DESC);

-- Runbook registry: metadata over a Work template (owner, validity,
-- rehearsal) and its track record.
CREATE TABLE runbooks (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  version INTEGER NOT NULL,
  spec TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'DRAFT' CHECK (status IN ('DRAFT','ACTIVE','EXPIRED','RETIRED')),
  last_rehearsed_at INTEGER,
  executions INTEGER NOT NULL DEFAULT 0,
  successes INTEGER NOT NULL DEFAULT 0,
  unknown_effects INTEGER NOT NULL DEFAULT 0,
  approvals TEXT,
  row_version INTEGER NOT NULL DEFAULT 1,
  updated_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, id, version)
);
