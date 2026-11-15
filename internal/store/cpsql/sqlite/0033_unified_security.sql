CREATE TABLE security_revocation_scope (
  tenant_id TEXT NOT NULL,
  scope_type TEXT NOT NULL CHECK (scope_type IN ('tenant','subject','work','run')),
  scope_id TEXT NOT NULL,
  version INTEGER NOT NULL CHECK (version >= 0),
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, scope_type, scope_id)
);
CREATE TABLE security_revocation_request (
  tenant_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  event_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, idempotency_key)
);
CREATE TABLE security_revocation_event (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  scope_type TEXT NOT NULL CHECK (scope_type IN ('tenant','subject','work','run')),
  scope_id TEXT NOT NULL,
  scope_version INTEGER NOT NULL CHECK (scope_version > 0),
  actor_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, id)
);
CREATE INDEX idx_security_revocation_event_scope ON security_revocation_event (tenant_id, scope_type, scope_id, scope_version DESC);
CREATE TABLE security_decision_audit (
  tenant_id TEXT NOT NULL,
  decision_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  operation TEXT NOT NULL,
  effect TEXT NOT NULL,
  mode TEXT NOT NULL,
  request_json TEXT NOT NULL,
  decision_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, decision_id)
);
CREATE INDEX idx_security_decision_request ON security_decision_audit (tenant_id, request_id, created_at DESC);
