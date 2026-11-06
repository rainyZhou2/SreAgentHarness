-- Runtime extensions (MCP servers, HTTP tools, skills) added per tenant
-- through the API; every process syncs them (internal/extensions).
CREATE TABLE tenant_extensions (
  tenant_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('mcp_server','http_tool','skill')),
  name TEXT NOT NULL,
  spec JSONB NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT true,
  version BIGINT NOT NULL DEFAULT 1,
  updated_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, kind, name)
);
ALTER TABLE tenant_extensions ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_extensions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant_extensions
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));
