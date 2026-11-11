-- SQLite mirror of migrations/0030_work_graph.sql (timestamps are unix microseconds,
-- JSON is TEXT). Keep both in sync; the contract tests run on both.
-- v3: Work graph and agent findings (design §8, §11, §16.3).
-- A Work has at most one parent (ownership: budget and permission ceiling)
-- and any number of dependency edges to Works of the same root. The CHECK
-- only forbids a self edge; the store verifies in the inserting transaction
-- that the graph stays acyclic, within its depth and child bounds, and
-- inside one tenant.
ALTER TABLE agent_works ADD COLUMN parent_work_id TEXT;
ALTER TABLE agent_works ADD COLUMN root_work_id TEXT;
ALTER TABLE agent_works ADD COLUMN depth INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_agent_works_parent ON agent_works (tenant_id, parent_work_id) WHERE parent_work_id IS NOT NULL;
CREATE INDEX idx_agent_works_root ON agent_works (tenant_id, root_work_id) WHERE root_work_id IS NOT NULL;

CREATE TABLE work_dependencies (
  tenant_id TEXT NOT NULL,
  parent_work_id TEXT NOT NULL,
  child_work_id TEXT NOT NULL,
  dependency_kind TEXT NOT NULL CHECK (dependency_kind IN ('after_success','after_terminal','after_signal')),
  predicate_ref TEXT,
  required INTEGER NOT NULL DEFAULT 1,
  satisfied_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, parent_work_id, child_work_id),
  CHECK (parent_work_id <> child_work_id),
  FOREIGN KEY (tenant_id, parent_work_id) REFERENCES agent_works (tenant_id, id),
  FOREIGN KEY (tenant_id, child_work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_dependencies_child ON work_dependencies (tenant_id, child_work_id);

-- Structured findings of agent assignments. Hypotheses stay the merged
-- ledger; a finding is what one agent reported, with the evidence it cited.
CREATE TABLE work_findings (
  tenant_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  finding_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  role TEXT NOT NULL,
  hypothesis_key TEXT,
  verdict TEXT NOT NULL CHECK (verdict IN ('supported','refuted','insufficient','contested')),
  statement TEXT NOT NULL,
  evidence_refs TEXT NOT NULL,
  record TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, finding_id),
  UNIQUE (tenant_id, work_id, payload_hash),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_findings_work ON work_findings (tenant_id, work_id, created_at);
