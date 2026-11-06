-- Per-tenant switch that hides a tool from runs even when the profile allows
-- it. It can only restrict: a disabled profile tool cannot be turned back on.
CREATE TABLE IF NOT EXISTS tenant_tool_settings (
  tenant_id TEXT NOT NULL,
  tool_name TEXT NOT NULL,
  enabled BOOLEAN NOT NULL,
  updated_by TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, tool_name)
);

ALTER TABLE tenant_tool_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_tool_settings FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tenant_tool_settings;
CREATE POLICY tenant_isolation ON tenant_tool_settings
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));
