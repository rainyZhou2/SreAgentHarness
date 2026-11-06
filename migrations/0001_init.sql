-- MVP schema derived from GoAgentHarness.md v1.1.3 §13 (13.1–13.7).
-- Deviations from the RFC are marked "MVP:".
-- Tables are created unqualified so tests can run each case in its own schema.

CREATE TABLE agent_profiles (
  tenant_id TEXT NOT NULL,
  profile_id TEXT NOT NULL,
  version BIGINT NOT NULL,
  spec JSONB NOT NULL,
  spec_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN
    ('DRAFT','CANDIDATE','ACTIVE','QUARANTINED','ARCHIVED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, profile_id, version)
);

CREATE TABLE incidents (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  service_id TEXT NOT NULL,
  environment TEXT NOT NULL,
  status TEXT NOT NULL,
  memory_version BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id)
);

CREATE TABLE agent_runs (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  incident_id UUID NULL,
  profile_id TEXT NOT NULL,
  profile_version BIGINT NOT NULL,
  status TEXT NOT NULL CHECK (status IN (
    'PENDING_ACTIVATION','QUEUED','RUNNING','PAUSED','WAITING_APPROVAL','NEEDS_HUMAN',
    'CANCELLED','COMPLETED','FAILED')),
  desired_state TEXT NOT NULL DEFAULT 'RUNNING'
    CHECK (desired_state IN ('RUNNING','PAUSED','CANCELLED')),
  version BIGINT NOT NULL DEFAULT 0,
  last_event_seq BIGINT NOT NULL DEFAULT 0,
  next_step_seq BIGINT NOT NULL DEFAULT 1,
  not_before TIMESTAMPTZ NULL,
  context_snapshot_id UUID NULL,
  lease_owner TEXT NULL,
  lease_until TIMESTAMPTZ NULL,
  fence_token BIGINT NOT NULL DEFAULT 0,
  -- MVP: creation idempotency, goal and result references live on the run row.
  create_idempotency_key TEXT NOT NULL,
  goal_ref TEXT NOT NULL,
  result_ref TEXT,
  failure_reason TEXT,
  initiator_type TEXT NOT NULL CHECK (initiator_type IN ('user','service_principal','system','unknown')),
  initiator_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, create_idempotency_key),
  FOREIGN KEY (tenant_id, incident_id) REFERENCES incidents(tenant_id, id),
  FOREIGN KEY (tenant_id, profile_id, profile_version)
    REFERENCES agent_profiles(tenant_id, profile_id, version),
  CHECK ((lease_owner IS NULL) = (lease_until IS NULL))
);
CREATE INDEX idx_run_queue ON agent_runs (not_before NULLS FIRST, created_at, id)
  WHERE status = 'QUEUED' AND desired_state = 'RUNNING';
CREATE INDEX idx_run_lease ON agent_runs (lease_until) WHERE status = 'RUNNING';
CREATE INDEX idx_run_incident ON agent_runs (tenant_id, incident_id, created_at DESC);

CREATE TABLE agent_steps (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  seq BIGINT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('reason','tool')),
  executor_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN (
    'PENDING','RUNNING','WAITING_APPROVAL','RETRY_WAIT','SUCCEEDED','FAILED',
    'SKIPPED','CANCELLED','UNKNOWN','RECONCILING','NEEDS_HUMAN')),
  input_ref TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  output_ref TEXT,
  output_hash TEXT,
  status_reason TEXT,              -- MVP: why a step was skipped / resolved (policy, approval, human)
  context_snapshot_id UUID,
  current_attempt INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (run_id, seq),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id)
);

CREATE TABLE step_attempts (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  step_id UUID NOT NULL,
  attempt_no INT NOT NULL,
  worker_id TEXT NOT NULL,
  fence_token BIGINT NOT NULL,
  status TEXT NOT NULL CHECK (status IN (
    'RUNNING','SUCCEEDED','FAILED','TIMED_OUT','CANCELLED','LOST','UNKNOWN')),
  idempotency_key TEXT NOT NULL,   -- H(tenant, run, step, action_hash): stable across attempts
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ,
  error_code TEXT,
  error_message TEXT,
  UNIQUE (step_id, attempt_no),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id),
  FOREIGN KEY (tenant_id, step_id) REFERENCES agent_steps(tenant_id, id)
);

CREATE TABLE run_events (
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  event_seq BIGINT NOT NULL,
  event_id UUID NOT NULL UNIQUE,
  step_id UUID,
  attempt_id UUID,
  event_type TEXT NOT NULL,
  actor_type TEXT NOT NULL,
  actor_id TEXT,
  schema_version INT NOT NULL DEFAULT 1,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload_ref TEXT,
  payload_hash TEXT,
  trace_id TEXT,
  occurred_at TIMESTAMPTZ NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, event_seq),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id)
);
CREATE INDEX idx_events_tenant_time ON run_events (tenant_id, committed_at DESC);

CREATE TABLE run_commands (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  idempotency_key TEXT NOT NULL,
  command_type TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  expected_version BIGINT,
  status TEXT NOT NULL DEFAULT 'PENDING'
    CHECK (status IN ('PENDING','APPLIED','REJECTED','EXPIRED')),
  reject_reason TEXT,
  run_version_after BIGINT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  applied_at TIMESTAMPTZ,
  UNIQUE (tenant_id, run_id, idempotency_key),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id)
);

CREATE TABLE outbox_messages (
  id UUID PRIMARY KEY,               -- also the NATS Msg-Id for JetStream de-duplication
  tenant_id TEXT NOT NULL,
  aggregate_id UUID NOT NULL,
  subject TEXT NOT NULL,
  payload JSONB NOT NULL,
  publish_attempt INT NOT NULL DEFAULT 0,
  next_retry_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_until TIMESTAMPTZ,
  published_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_outbox_pending ON outbox_messages (next_retry_at, created_at)
  WHERE published_at IS NULL;
CREATE INDEX idx_outbox_aggregate ON outbox_messages (aggregate_id) WHERE published_at IS NULL;

CREATE TABLE external_tool_effects (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  step_id UUID NOT NULL,
  idempotency_key TEXT NOT NULL,
  target TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN
    ('PREPARED','DISPATCHED','CONFIRMED','UNKNOWN','COMPENSATED')),
  external_operation_id TEXT,
  last_check_at TIMESTAMPTZ,
  result_ref TEXT,
  UNIQUE (tenant_id, idempotency_key, target),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id),
  FOREIGN KEY (tenant_id, step_id) REFERENCES agent_steps(tenant_id, id)
);

CREATE TABLE agent_kv (
  tenant_id TEXT NOT NULL,
  scope TEXT NOT NULL CHECK (scope IN ('run','incident','agent','service','organization')),
  owner_id TEXT NOT NULL,
  key TEXT NOT NULL,
  value_inline BYTEA,
  value_ref TEXT,
  value_hash TEXT NOT NULL,
  schema_id TEXT,
  schema_version INT NOT NULL DEFAULT 1,
  version BIGINT NOT NULL,
  expires_at TIMESTAMPTZ,
  tombstone BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, scope, owner_id, key),
  CHECK (tombstone OR ((value_inline IS NULL) <> (value_ref IS NULL)))
);

CREATE TABLE agent_kv_versions (
  tenant_id TEXT NOT NULL,
  scope TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  key TEXT NOT NULL,
  version BIGINT NOT NULL,
  value_inline BYTEA,
  value_ref TEXT,
  value_hash TEXT,
  schema_id TEXT,
  schema_version INT NOT NULL DEFAULT 1,
  tombstone BOOLEAN NOT NULL DEFAULT FALSE,
  -- MVP: nullable; run-scope writes made by a step or command reference run_events,
  -- other scopes have no event store yet.
  event_id UUID,
  run_id UUID,
  step_id UUID,
  actor_type TEXT,
  actor_id TEXT,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, scope, owner_id, key, version)
);

CREATE TABLE context_snapshots (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  step_id UUID NOT NULL,
  profile_id TEXT NOT NULL,
  profile_version BIGINT NOT NULL,
  working_memory_version BIGINT,
  incident_memory_version BIGINT,
  kv_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
  memory_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  refs JSONB NOT NULL,
  compaction_epoch BIGINT NOT NULL DEFAULT 0,
  context_hash TEXT,
  manifest_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id),
  UNIQUE (step_id),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id),
  FOREIGN KEY (tenant_id, step_id) REFERENCES agent_steps(tenant_id, id)
);

ALTER TABLE agent_runs ADD FOREIGN KEY (tenant_id, context_snapshot_id)
  REFERENCES context_snapshots(tenant_id, id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE agent_steps ADD FOREIGN KEY (tenant_id, context_snapshot_id)
  REFERENCES context_snapshots(tenant_id, id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE cognitive_artifacts (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  step_id UUID NOT NULL,
  kind TEXT NOT NULL,
  schema_version INT NOT NULL,
  artifact_ref TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  context_snapshot_id UUID NOT NULL,
  source_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id),
  FOREIGN KEY (tenant_id, step_id) REFERENCES agent_steps(tenant_id, id),
  FOREIGN KEY (tenant_id, context_snapshot_id) REFERENCES context_snapshots(tenant_id, id)
);

CREATE TABLE action_intents (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  step_id UUID NOT NULL,
  environment TEXT NOT NULL,
  target TEXT NOT NULL,
  tool_name TEXT NOT NULL,
  tool_version TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  action_hash TEXT NOT NULL,
  risk_class TEXT NOT NULL CHECK (risk_class IN ('R0','R1','R2','R3')),
  policy_version TEXT NOT NULL,
  effects JSONB NOT NULL,
  rollback_plan_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, step_id, action_hash),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id),
  FOREIGN KEY (tenant_id, step_id) REFERENCES agent_steps(tenant_id, id)
);

CREATE TABLE approval_requests (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  action_intent_id UUID NOT NULL,
  required_approvals INT NOT NULL DEFAULT 1 CHECK (required_approvals >= 1),
  status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN
    ('PENDING','APPROVED','REJECTED','EXPIRED','SUPERSEDED','CONSUMED')),
  expires_at TIMESTAMPTZ NOT NULL,
  requested_event_seq BIGINT NOT NULL,
  decided_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, action_intent_id) REFERENCES action_intents(tenant_id, id)
);
CREATE INDEX idx_approval_expiry ON approval_requests (expires_at) WHERE status = 'PENDING';
CREATE UNIQUE INDEX uq_pending_approval_per_intent ON approval_requests (action_intent_id)
  WHERE status = 'PENDING';

CREATE TABLE approval_decisions (
  tenant_id TEXT NOT NULL,
  approval_request_id UUID NOT NULL,
  approver_id TEXT NOT NULL,
  decision TEXT NOT NULL CHECK (decision IN ('APPROVE','REJECT')),
  action_hash TEXT NOT NULL,
  command_id UUID NOT NULL,
  decided_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (approval_request_id, approver_id),
  FOREIGN KEY (tenant_id, approval_request_id) REFERENCES approval_requests(tenant_id, id),
  FOREIGN KEY (tenant_id, command_id) REFERENCES run_commands(tenant_id, id)
);

CREATE TABLE approval_grants (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  approval_request_id UUID NOT NULL UNIQUE,
  action_hash TEXT NOT NULL,
  max_uses INT NOT NULL DEFAULT 1,
  used_count INT NOT NULL DEFAULT 0 CHECK (used_count <= max_uses),
  expires_at TIMESTAMPTZ NOT NULL,
  consumed_by_attempt_id UUID,
  FOREIGN KEY (tenant_id, approval_request_id) REFERENCES approval_requests(tenant_id, id)
);
