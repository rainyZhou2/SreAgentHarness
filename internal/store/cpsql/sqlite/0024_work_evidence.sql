-- SQLite mirror of migrations/0024_work_evidence.sql (timestamps are unix microseconds,
-- JSON is TEXT). Keep both in sync; the contract tests run on both.
-- v2 control plane: shared evidence and hypotheses across runs and agents
-- (RFC §12–13). Evidence is append-only; corrections supersede.
CREATE TABLE work_evidence (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  source_type TEXT NOT NULL,
  source_ref TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  supersedes TEXT,
  record TEXT NOT NULL,
  captured_at INTEGER NOT NULL,
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, work_id, content_hash),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_evidence_work ON work_evidence (tenant_id, work_id, captured_at);

CREATE TABLE work_hypotheses (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  hyp_key TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('OPEN','SUPPORTED','REFUTED','CHALLENGED','INCONCLUSIVE')),
  record TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  updated_at INTEGER NOT NULL,
  UNIQUE (tenant_id, work_id, hyp_key),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);

