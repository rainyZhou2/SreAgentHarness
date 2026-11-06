package extensions

import (
	"net/netip"

	"go-sre-harness/internal/netguard"
)

// NetPolicy is the shared network policy for anything that dials URLs on
// behalf of a tenant (HTTP tools, MCP servers, tenant models, web tools).
type NetPolicy = netguard.NetPolicy

// ParseNetworks parses a comma-separated list of CIDRs or single addresses.
func ParseNetworks(v string) ([]netip.Prefix, error) { return netguard.ParseNetworks(v) }
