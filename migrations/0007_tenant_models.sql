-- Tenant-configured model providers are stored as extensions of kind 'model'.
ALTER TABLE tenant_extensions DROP CONSTRAINT IF EXISTS tenant_extensions_kind_check;
ALTER TABLE tenant_extensions ADD CONSTRAINT tenant_extensions_kind_check
  CHECK (kind IN ('mcp_server','http_tool','skill','model'));
