-- SQLite mirror of migrations/0029_quota_reservations.sql (timestamps are unix microseconds,
-- JSON is TEXT). Keep both in sync; the contract tests run on both.
-- v3: cluster-wide admission budgets (design §6.2, §16.2). A reservation is
-- taken in the same transaction that locks its budget row, so replicas can
-- never admit more than the capacity together. Release and renewal are a
-- fenced CAS on (reservation, fence_token): a late release cannot free the
-- units of a newer owner. Expired reservations are reaped, and reconciled
-- against the run links before their units are returned.
CREATE TABLE admission_budgets (
  tenant_id TEXT NOT NULL,
  queue TEXT NOT NULL,
  resource TEXT NOT NULL,
  capacity INTEGER NOT NULL CHECK (capacity >= 0),
  reserved INTEGER NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  version INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, queue, resource)
);

CREATE TABLE admission_reservations (
  tenant_id TEXT NOT NULL,
  reservation_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  queue TEXT NOT NULL,
  resource TEXT NOT NULL,
  units INTEGER NOT NULL CHECK (units > 0),
  fence_token INTEGER NOT NULL,
  owner TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('ACTIVE','RELEASED','EXPIRED')),
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, reservation_id),
  UNIQUE (tenant_id, idempotency_key),
  FOREIGN KEY (tenant_id, queue, resource) REFERENCES admission_budgets (tenant_id, queue, resource)
);
CREATE INDEX idx_reservations_reap ON admission_reservations (expires_at) WHERE status = 'ACTIVE';
CREATE INDEX idx_reservations_work ON admission_reservations (tenant_id, work_id) WHERE status = 'ACTIVE';
