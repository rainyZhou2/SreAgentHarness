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
  capacity BIGINT NOT NULL CHECK (capacity >= 0),
  reserved BIGINT NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  version BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, queue, resource)
);

CREATE TABLE admission_reservations (
  tenant_id TEXT NOT NULL,
  reservation_id UUID NOT NULL,
  work_id UUID NOT NULL,
  idempotency_key TEXT NOT NULL,
  queue TEXT NOT NULL,
  resource TEXT NOT NULL,
  units BIGINT NOT NULL CHECK (units > 0),
  fence_token BIGINT NOT NULL,
  owner TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('ACTIVE','RELEASED','EXPIRED')),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, reservation_id),
  UNIQUE (tenant_id, idempotency_key),
  FOREIGN KEY (tenant_id, queue, resource) REFERENCES admission_budgets (tenant_id, queue, resource)
);
CREATE INDEX idx_reservations_reap ON admission_reservations (expires_at) WHERE status = 'ACTIVE';
CREATE INDEX idx_reservations_work ON admission_reservations (tenant_id, work_id) WHERE status = 'ACTIVE';

DO $$
DECLARE tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['admission_budgets','admission_reservations'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id))', tbl);
  END LOOP;
END $$;
