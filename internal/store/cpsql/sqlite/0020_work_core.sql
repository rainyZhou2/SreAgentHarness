-- SQLite mirror of migrations/0020_work_core.sql (timestamps are unix microseconds,
-- JSON is TEXT). Keep both in sync; the contract tests run on both.
-- v2 control plane: the Work aggregate (RFC §5, §17). A Work is a long-lived
-- goal spanning many runs. State changes are version-CAS mutations that append
-- a gap-free timeline event in the same transaction.
CREATE TABLE agent_works (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('NEW','PLANNING','ACTIVE','WAITING','VERIFYING','BLOCKED',
    'SUSPENDED','SUCCEEDED','FAILED','CANCELLED')),
  title TEXT NOT NULL,
  goal TEXT NOT NULL,
  goal_hash TEXT NOT NULL,
  template TEXT NOT NULL,
  template_id TEXT NOT NULL,
  template_version INTEGER NOT NULL,
  generation INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
  stage INTEGER NOT NULL DEFAULT 0 CHECK (stage >= 0),
  stage_state TEXT,
  replans INTEGER NOT NULL DEFAULT 0,
  runs_started INTEGER NOT NULL DEFAULT 0,
  cost_micros INTEGER NOT NULL DEFAULT 0,
  reason TEXT,
  priority INTEGER NOT NULL DEFAULT 0,
  owner_actor TEXT,
  source_event TEXT,
  version INTEGER NOT NULL DEFAULT 1,
  last_event_seq INTEGER NOT NULL DEFAULT 0,
  wake_at INTEGER,
  lease_owner TEXT,
  lease_until INTEGER,
  create_idempotency_key TEXT NOT NULL,
  create_request_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  closed_at INTEGER,
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, create_idempotency_key)
);
CREATE INDEX idx_agent_works_queue ON agent_works (tenant_id, status, priority DESC, created_at);
CREATE INDEX idx_agent_works_recent ON agent_works (tenant_id, updated_at DESC, id DESC);
CREATE INDEX idx_agent_works_wake ON agent_works (wake_at) WHERE wake_at IS NOT NULL;

CREATE TABLE work_events (
  tenant_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  event_seq INTEGER NOT NULL,
  event_id TEXT NOT NULL,
  type TEXT NOT NULL,
  actor_type TEXT NOT NULL,
  actor_id TEXT,
  causation_id TEXT,
  payload TEXT,
  occurred_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, work_id, event_seq),
  UNIQUE (tenant_id, event_id),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);

-- A run may be referenced by several Works as evidence but has at most one
-- owner Work. Runs are created through the kernel (StartRun); the link is
-- tenant-scoped and the orchestrator only links runs it created.
CREATE TABLE work_run_links (
  tenant_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('owner','evidence')),
  phase TEXT NOT NULL,
  stage_id TEXT,
  agent_role TEXT,
  generation INTEGER NOT NULL,
  required INTEGER NOT NULL DEFAULT 1,
  status TEXT,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, work_id, run_id),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE UNIQUE INDEX uq_work_run_single_owner ON work_run_links (tenant_id, run_id) WHERE role = 'owner';

CREATE TABLE work_subject_links (
  tenant_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  subject_kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, work_id, subject_kind, subject_id),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_subject_lookup ON work_subject_links (tenant_id, subject_kind, subject_id);

-- Accepted operator commands, keyed by idempotency key: a replay returns the
-- stored result instead of applying the command again.
CREATE TABLE work_commands (
  tenant_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  command_type TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  actor_id TEXT,
  expected_version INTEGER,
  result TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, work_id, idempotency_key),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);

-- Correlation groups: events of one group within the window attach to the
-- same open Work; concurrent deliveries agree through the primary key.
CREATE TABLE work_correlations (
  tenant_id TEXT NOT NULL,
  group_key TEXT NOT NULL,
  work_id TEXT NOT NULL,
  rule TEXT NOT NULL,
  hits INTEGER NOT NULL DEFAULT 1,
  first_seen INTEGER NOT NULL,
  last_seen INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, group_key),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_correlations_expiry ON work_correlations (expires_at);

