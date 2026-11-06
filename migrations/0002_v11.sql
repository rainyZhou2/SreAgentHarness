-- v1.1 schema: Conversation (§35.1), Run extensions (§35.2), Model Gateway (§35.3–35.4),
-- Checkpoint / output revisions (§13.6), Incident shared memory (§8.1), long-term
-- memory & experience (§13.5, §9), evaluation & evolution (§12), security audit (§19.3).
-- Every business table carries tenant_id and uses composite (tenant_id, id) FKs (§13.7).

-- ---------- step kinds / run extensions ----------
ALTER TABLE agent_steps DROP CONSTRAINT agent_steps_kind_check;
ALTER TABLE agent_steps ADD CONSTRAINT agent_steps_kind_check
  CHECK (kind IN ('reason','tool','retrieve_memory','compact'));
ALTER TABLE agent_steps ADD COLUMN output_revision INT NOT NULL DEFAULT 0;
ALTER TABLE agent_steps ADD COLUMN summary JSONB;

ALTER TABLE agent_runs
  ADD COLUMN session_id UUID,
  ADD COLUMN branch_id UUID,
  ADD COLUMN turn_id UUID,
  ADD COLUMN forked_from_checkpoint_id UUID,
  ADD COLUMN compaction_epoch BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN rollout_id UUID,
  ADD COLUMN priority INT NOT NULL DEFAULT 0;
ALTER TABLE agent_runs ADD CONSTRAINT ck_run_turn_session
  CHECK (turn_id IS NULL OR (session_id IS NOT NULL AND branch_id IS NOT NULL));
CREATE INDEX idx_agent_runs_session ON agent_runs (tenant_id, session_id, created_at DESC)
  WHERE session_id IS NOT NULL;
CREATE INDEX idx_agent_runs_tenant_status ON agent_runs (tenant_id, status);

ALTER TABLE context_snapshots ADD COLUMN tool_schema_set_hash TEXT;
ALTER TABLE context_snapshots ADD COLUMN message_cursor BIGINT;

-- ---------- conversation (§35.1) ----------
CREATE TABLE conversation_sessions (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  created_by TEXT NOT NULL,
  title TEXT,
  profile_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','ARCHIVED','DELETED')),
  default_branch_id UUID,
  incident_id UUID,
  create_idempotency_key TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, create_idempotency_key)
);

CREATE TABLE conversation_branches (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  session_id UUID NOT NULL,
  parent_branch_id UUID,
  parent_message_cursor BIGINT,
  forked_from_checkpoint_id UUID,
  version BIGINT NOT NULL DEFAULT 0,
  last_turn_seq BIGINT NOT NULL DEFAULT 0,
  last_message_seq BIGINT NOT NULL DEFAULT 0,
  active_turn_id UUID,
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, session_id) REFERENCES conversation_sessions(tenant_id, id),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, session_id, id)
);
ALTER TABLE conversation_branches ADD FOREIGN KEY (tenant_id, parent_branch_id)
  REFERENCES conversation_branches(tenant_id, id);
ALTER TABLE conversation_sessions ADD FOREIGN KEY (tenant_id, default_branch_id)
  REFERENCES conversation_branches(tenant_id, id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE conversation_turns (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  session_id UUID NOT NULL,
  branch_id UUID NOT NULL,
  run_id UUID,
  seq BIGINT NOT NULL,
  input_ref TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  input_message_cursor BIGINT,
  activated_at TIMESTAMPTZ,
  client_request_id TEXT NOT NULL,
  created_by TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('ACCEPTED','QUEUED','RUNNING','COMPLETED','FAILED','CANCELLED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, session_id, branch_id) REFERENCES conversation_branches(tenant_id, session_id, id),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id),
  UNIQUE (tenant_id, branch_id, seq),
  UNIQUE (tenant_id, branch_id, client_request_id),
  UNIQUE (tenant_id, id)
);
CREATE INDEX idx_turns_queued ON conversation_turns (tenant_id, branch_id, seq) WHERE status = 'QUEUED';
ALTER TABLE conversation_branches ADD FOREIGN KEY (tenant_id, active_turn_id)
  REFERENCES conversation_turns(tenant_id, id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE conversation_messages (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  session_id UUID NOT NULL,
  branch_id UUID NOT NULL,
  turn_id UUID NOT NULL,
  seq BIGINT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('system','developer','user','assistant','tool')),
  content_ref TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  message_kind TEXT NOT NULL,
  source_step_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, session_id, branch_id) REFERENCES conversation_branches(tenant_id, session_id, id),
  FOREIGN KEY (tenant_id, turn_id) REFERENCES conversation_turns(tenant_id, id),
  UNIQUE (tenant_id, branch_id, seq)
);

ALTER TABLE agent_runs ADD FOREIGN KEY (tenant_id, session_id) REFERENCES conversation_sessions(tenant_id, id);
ALTER TABLE agent_runs ADD FOREIGN KEY (tenant_id, branch_id) REFERENCES conversation_branches(tenant_id, id);
ALTER TABLE agent_runs ADD FOREIGN KEY (tenant_id, turn_id) REFERENCES conversation_turns(tenant_id, id)
  DEFERRABLE INITIALLY DEFERRED;

-- ---------- checkpoints & output revisions (§13.6) ----------
CREATE TABLE run_checkpoints (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  last_committed_step_seq BIGINT NOT NULL,
  last_event_seq BIGINT NOT NULL,
  profile_id TEXT NOT NULL,
  profile_version BIGINT NOT NULL,
  context_snapshot_id UUID,
  message_cursor BIGINT,
  compaction_epoch BIGINT NOT NULL DEFAULT 0,
  pending_refs JSONB NOT NULL DEFAULT '{}'::jsonb,
  workspace_snapshot_ref TEXT,
  provider_state_ref TEXT,
  manifest_ref TEXT NOT NULL,
  manifest_hash TEXT NOT NULL,
  replayable BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (run_id, last_committed_step_seq),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id),
  FOREIGN KEY (tenant_id, context_snapshot_id) REFERENCES context_snapshots(tenant_id, id)
);
ALTER TABLE agent_runs ADD FOREIGN KEY (tenant_id, forked_from_checkpoint_id)
  REFERENCES run_checkpoints(tenant_id, id);
ALTER TABLE conversation_branches ADD FOREIGN KEY (tenant_id, forked_from_checkpoint_id)
  REFERENCES run_checkpoints(tenant_id, id);

CREATE TABLE step_output_revisions (
  step_id UUID NOT NULL,
  revision INT NOT NULL,
  tenant_id TEXT NOT NULL,
  output_ref TEXT NOT NULL,
  output_hash TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('EXECUTION','HUMAN_OVERRIDE')),
  command_id UUID,
  reason TEXT,
  side_effects_reverted BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (step_id, revision),
  FOREIGN KEY (tenant_id, step_id) REFERENCES agent_steps(tenant_id, id),
  FOREIGN KEY (tenant_id, command_id) REFERENCES run_commands(tenant_id, id)
);

-- ---------- incident shared working memory (§8.1) ----------
CREATE TABLE incident_memory_events (
  tenant_id TEXT NOT NULL,
  incident_id UUID NOT NULL,
  version BIGINT NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN
    ('evidence_added','hypothesis_updated','decision_recorded','action_taken','fact_confirmed')),
  run_id UUID,
  step_id UUID,
  actor_type TEXT NOT NULL,
  actor_id TEXT,
  payload JSONB NOT NULL,
  trust TEXT NOT NULL DEFAULT 'T2',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (incident_id, version),
  FOREIGN KEY (tenant_id, incident_id) REFERENCES incidents(tenant_id, id)
);

-- ---------- model gateway (§35.3, §35.4) ----------
CREATE TABLE model_route_policies (
  policy_id TEXT NOT NULL,
  version BIGINT NOT NULL,
  spec JSONB NOT NULL,
  spec_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (policy_id, version),
  UNIQUE (policy_id, spec_hash)
);

CREATE TABLE model_requests (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  step_id UUID NOT NULL,
  step_attempt_id UUID NOT NULL,
  fence_token BIGINT NOT NULL,
  context_snapshot_id UUID NOT NULL,
  model_alias TEXT NOT NULL,
  route_policy_id TEXT NOT NULL,
  route_policy_version BIGINT NOT NULL,
  request_ref TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  manifest_ref TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN
    ('QUEUED','INFLIGHT','SUCCEEDED','FAILED','CANCELLED','OUTCOME_UNKNOWN')),
  max_output_tokens BIGINT NOT NULL,
  response_ref TEXT,
  error_code TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id),
  FOREIGN KEY (tenant_id, step_id) REFERENCES agent_steps(tenant_id, id),
  FOREIGN KEY (tenant_id, context_snapshot_id) REFERENCES context_snapshots(tenant_id, id)
);
CREATE INDEX idx_model_requests_run ON model_requests (tenant_id, run_id, created_at DESC);

CREATE TABLE model_request_attempts (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  model_request_id UUID NOT NULL,
  attempt_no INT NOT NULL,
  provider_id TEXT NOT NULL,
  endpoint_id TEXT NOT NULL,
  resolved_model_id TEXT NOT NULL,
  resolved_model_version TEXT NOT NULL,
  route_reason JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL CHECK (status IN
    ('PREPARED','SENT','STREAMING','SUCCEEDED','FAILED','CANCELLED','OUTCOME_UNKNOWN')),
  provider_request_id TEXT,
  response_ref TEXT,
  finish_reason TEXT,
  error_code TEXT,
  ttft_ms BIGINT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ,
  FOREIGN KEY (tenant_id, model_request_id) REFERENCES model_requests(tenant_id, id),
  UNIQUE (model_request_id, attempt_no),
  UNIQUE (tenant_id, id)
);

CREATE TABLE model_usage_ledger (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  model_attempt_id UUID NOT NULL,
  usage_state TEXT NOT NULL CHECK (usage_state IN ('ESTIMATED','REPORTED','RECONCILED','UNKNOWN')),
  revision INT NOT NULL,
  supersedes_id UUID REFERENCES model_usage_ledger(id),
  input_tokens BIGINT,
  output_tokens BIGINT,
  cached_input_tokens BIGINT,
  cache_write_tokens BIGINT,
  pricing_version TEXT NOT NULL,
  estimated_cost_micros BIGINT,
  reconciled_cost_micros BIGINT,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, model_attempt_id) REFERENCES model_request_attempts(tenant_id, id),
  UNIQUE (model_attempt_id, revision)
);
CREATE INDEX idx_usage_tenant_time ON model_usage_ledger (tenant_id, recorded_at DESC);
CREATE VIEW model_usage_effective AS
SELECT DISTINCT ON (model_attempt_id) * FROM model_usage_ledger ORDER BY model_attempt_id, revision DESC;

-- Token deltas are stored as bounded segments in object storage, never one row per token (§35.5).
CREATE TABLE model_stream_segments (
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  model_request_id UUID NOT NULL,
  segment_no INT NOT NULL,
  first_delta_seq BIGINT NOT NULL,
  last_delta_seq BIGINT NOT NULL,
  segment_ref TEXT NOT NULL,
  segment_hash TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (model_request_id, segment_no),
  FOREIGN KEY (tenant_id, model_request_id) REFERENCES model_requests(tenant_id, id)
);
CREATE INDEX idx_stream_segments_run ON model_stream_segments (tenant_id, run_id, created_at);
CREATE INDEX idx_stream_segments_expiry ON model_stream_segments (expires_at);

-- ---------- long-term memory (§9, §13.5) ----------
CREATE TABLE agent_memories (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  scope TEXT NOT NULL CHECK (scope IN ('organization','service','incident','agent')),
  scope_id TEXT NOT NULL,
  memory_type TEXT NOT NULL CHECK (memory_type IN ('semantic','episodic','procedural')),
  status TEXT NOT NULL CHECK (status IN
    ('CANDIDATE','VALIDATING','ACTIVE','STALE','SUPERSEDED','REJECTED','ARCHIVED')),
  current_version BIGINT NOT NULL DEFAULT 1,
  subject TEXT NOT NULL,                 -- dedup / contradiction key, e.g. "checkout:root_cause"
  title TEXT NOT NULL,
  content_text TEXT NOT NULL,            -- redacted, searchable projection of content_ref
  content_ref TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  source_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  trust TEXT NOT NULL DEFAULT 'T3',
  sensitivity TEXT NOT NULL DEFAULT 'internal' CHECK (sensitivity IN ('public','internal','restricted')),
  confidence DOUBLE PRECISION,
  verified_by TEXT,
  environment TEXT,
  service_version TEXT,
  policy_version TEXT,
  superseded_by UUID,
  valid_from TIMESTAMPTZ,
  valid_until TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  search_tsv tsvector GENERATED ALWAYS AS
    (to_tsvector('simple', coalesce(title,'') || ' ' || coalesce(content_text,'') || ' ' || subject)) STORED,
  CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  UNIQUE (tenant_id, id)
);
CREATE INDEX idx_memory_scope ON agent_memories (tenant_id, scope, scope_id, memory_type, status);
CREATE INDEX idx_memory_subject ON agent_memories (tenant_id, subject) WHERE status IN ('ACTIVE','CANDIDATE','VALIDATING');
CREATE INDEX idx_memory_tsv ON agent_memories USING GIN (search_tsv);
CREATE UNIQUE INDEX uq_memory_content ON agent_memories (tenant_id, scope, scope_id, content_hash)
  WHERE status NOT IN ('REJECTED','ARCHIVED');

CREATE TABLE agent_memory_versions (
  tenant_id TEXT NOT NULL,
  memory_id UUID NOT NULL,
  version BIGINT NOT NULL,
  content_ref TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  provenance JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (memory_id, version),
  FOREIGN KEY (tenant_id, memory_id) REFERENCES agent_memories(tenant_id, id)
);

-- Embeddings are versioned by embedding model and dimension (§13.5). REAL[] keeps the
-- schema portable; a pgvector backend can replace the similarity scan without kernel changes.
CREATE TABLE memory_embeddings (
  tenant_id TEXT NOT NULL,
  memory_id UUID NOT NULL,
  memory_version BIGINT NOT NULL,
  embedding_model TEXT NOT NULL,
  dims INT NOT NULL,
  embedding REAL[] NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (memory_id, memory_version, embedding_model),
  FOREIGN KEY (tenant_id, memory_id) REFERENCES agent_memories(tenant_id, id)
);

-- Entity graph used by the retrieval pipeline's dependency branch (§9.3).
CREATE TABLE service_dependencies (
  tenant_id TEXT NOT NULL,
  service_id TEXT NOT NULL,
  depends_on TEXT NOT NULL,
  relation TEXT NOT NULL DEFAULT 'calls',
  PRIMARY KEY (tenant_id, service_id, depends_on)
);

-- Memory hits produced by a retrieve_memory step; pinned by the next snapshot (§10.2 ①).
CREATE TABLE step_memory_hits (
  tenant_id TEXT NOT NULL,
  step_id UUID NOT NULL,
  memory_id UUID NOT NULL,
  memory_version BIGINT NOT NULL,
  score DOUBLE PRECISION NOT NULL,
  rank INT NOT NULL,
  PRIMARY KEY (step_id, memory_id),
  FOREIGN KEY (tenant_id, step_id) REFERENCES agent_steps(tenant_id, id),
  FOREIGN KEY (tenant_id, memory_id) REFERENCES agent_memories(tenant_id, id)
);

CREATE TABLE experiences (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  incident_id UUID,
  run_id UUID NOT NULL,
  decision_id UUID,
  action_step_id UUID,
  outcome TEXT NOT NULL,
  outcome_ref TEXT,
  lessons_ref TEXT NOT NULL,
  feedback_ref TEXT,
  source_refs JSONB NOT NULL,
  quality_state TEXT NOT NULL CHECK (quality_state IN ('UNVERIFIED','VERIFIED','DISPUTED')),
  extractor_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, run_id, extractor_version),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id)
);

CREATE TABLE run_feedback (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  actor_id TEXT NOT NULL,
  root_cause_correct BOOLEAN,
  root_cause_label TEXT,
  rating INT CHECK (rating BETWEEN 1 AND 5),
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, run_id, actor_id),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id)
);

-- Cold-path work queue: a finished run is consolidated exactly once per extractor version.
CREATE TABLE consolidation_queue (
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','DONE','FAILED')),
  attempts INT NOT NULL DEFAULT 0,
  claimed_until TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id)
);

-- ---------- evaluation & evolution (§12) ----------
CREATE TABLE evaluation_datasets (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  version BIGINT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('DRAFT','FROZEN','ARCHIVED')),
  manifest_hash TEXT,
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  frozen_at TIMESTAMPTZ,
  UNIQUE (tenant_id, name, version),
  UNIQUE (tenant_id, id)
);

CREATE TABLE evaluation_cases (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  dataset_id UUID NOT NULL,
  case_key TEXT NOT NULL,
  split TEXT NOT NULL CHECK (split IN ('train','holdout')),
  service_id TEXT NOT NULL,
  environment TEXT NOT NULL,
  goal TEXT NOT NULL,
  recording_ref TEXT NOT NULL,          -- recorded tool outputs (and optionally model outputs)
  ground_truth JSONB NOT NULL,          -- {"root_cause_labels": [...], "must_not_call": [...]}
  allowed_tools JSONB NOT NULL,
  risk_labels JSONB NOT NULL DEFAULT '[]'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL,
  source_run_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (dataset_id, case_key),
  FOREIGN KEY (tenant_id, dataset_id) REFERENCES evaluation_datasets(tenant_id, id)
);

CREATE TABLE evaluation_runs (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  dataset_id UUID NOT NULL,
  profile_id TEXT NOT NULL,
  profile_version BIGINT NOT NULL,
  split TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
  metrics JSONB,
  report_ref TEXT,
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, dataset_id) REFERENCES evaluation_datasets(tenant_id, id),
  FOREIGN KEY (tenant_id, profile_id, profile_version) REFERENCES agent_profiles(tenant_id, profile_id, version)
);

CREATE TABLE evolution_candidates (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  profile_id TEXT NOT NULL,
  base_version BIGINT NOT NULL,
  candidate_version BIGINT,
  candidate_spec_ref TEXT NOT NULL,
  change_summary JSONB NOT NULL,
  rationale_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  base_evaluation_id UUID,
  evaluation_id UUID,
  gate_result JSONB,
  status TEXT NOT NULL CHECK (status IN
    ('PROPOSED','EVALUATING','GATE_FAILED','PENDING_APPROVAL','APPROVED','REJECTED','ROLLED_OUT')),
  proposed_by TEXT NOT NULL,
  approved_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, profile_id, base_version) REFERENCES agent_profiles(tenant_id, profile_id, version)
);

CREATE TABLE evolution_rollouts (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  profile_id TEXT NOT NULL,
  from_version BIGINT NOT NULL,
  to_version BIGINT NOT NULL,
  candidate_id UUID,
  status TEXT NOT NULL CHECK (status IN ('PENDING_APPROVAL','CANARY','PROMOTED','ROLLED_BACK','QUARANTINED')),
  traffic_percent NUMERIC(5,2) NOT NULL DEFAULT 0 CHECK (traffic_percent BETWEEN 0 AND 100),
  guardrails JSONB NOT NULL,
  observations JSONB,
  approved_by TEXT,
  started_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, profile_id, to_version) REFERENCES agent_profiles(tenant_id, profile_id, version),
  FOREIGN KEY (tenant_id, candidate_id) REFERENCES evolution_candidates(tenant_id, id)
);
CREATE UNIQUE INDEX uq_active_rollout ON evolution_rollouts (tenant_id, profile_id)
  WHERE status IN ('PENDING_APPROVAL','CANARY');
ALTER TABLE agent_runs ADD FOREIGN KEY (tenant_id, rollout_id) REFERENCES evolution_rollouts(tenant_id, id);

-- Default (non-canary) profile version per tenant/profile.
CREATE TABLE profile_channels (
  tenant_id TEXT NOT NULL,
  profile_id TEXT NOT NULL,
  active_version BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, profile_id),
  FOREIGN KEY (tenant_id, profile_id, active_version) REFERENCES agent_profiles(tenant_id, profile_id, version)
);

-- ---------- security audit (§19.3) & access control ----------
CREATE TABLE security_audit_log (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  actor_type TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  action TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  resource_id TEXT,
  outcome TEXT NOT NULL CHECK (outcome IN ('allowed','denied','applied','rejected')),
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  trace_id TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_audit_tenant_time ON security_audit_log (tenant_id, occurred_at DESC);

CREATE TABLE role_bindings (
  tenant_id TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('viewer','operator','approver','admin')),
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, principal_id, role)
);

CREATE TABLE tenant_quotas (
  tenant_id TEXT PRIMARY KEY,
  max_active_runs INT NOT NULL DEFAULT 200,
  max_queued_runs INT NOT NULL DEFAULT 2000,
  max_running_steps INT NOT NULL DEFAULT 50,
  llm_tokens_per_minute BIGINT NOT NULL DEFAULT 2000000,
  llm_requests_per_minute INT NOT NULL DEFAULT 600,
  llm_daily_budget_micros BIGINT NOT NULL DEFAULT 500000000,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- row level security (§13.7) ----------
-- Defense in depth: when a connection sets app.tenant_id (API read paths do), rows of other
-- tenants are invisible. Background roles (workers, reconciler) leave it unset.
CREATE OR REPLACE FUNCTION harness_tenant_visible(t TEXT) RETURNS BOOLEAN LANGUAGE sql STABLE AS $$
  SELECT coalesce(nullif(current_setting('app.tenant_id', true), ''), t) = t
$$;
DO $$
DECLARE tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['agent_runs','agent_steps','step_attempts','run_events','run_commands',
    'context_snapshots','cognitive_artifacts','action_intents','approval_requests','agent_kv',
    'conversation_sessions','conversation_branches','conversation_turns','conversation_messages',
    'run_checkpoints','step_output_revisions','model_requests','model_request_attempts','model_usage_ledger',
    'agent_memories','agent_memory_versions','experiences','incident_memory_events','evaluation_runs',
    'evolution_candidates','evolution_rollouts','security_audit_log'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id))', tbl);
  END LOOP;
END $$;
