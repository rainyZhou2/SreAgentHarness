// Package graph maintains the capability projection and serves read-only
// neighborhood queries over typed provenance tables.
package graph

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/extensions"
	"go-sre-harness/internal/tools"
)

type Store interface {
	UpsertCapabilityNodes(ctx context.Context, nodes []domain.CapabilityNode) error
	CapabilityNodes(ctx context.Context, tenantID string) ([]domain.CapabilityNode, error)
	UpsertCapabilityRelation(ctx context.Context, rel domain.CapabilityRelation) error
	DeleteCapabilityRelation(ctx context.Context, tenantID, src, dst, relation, source string) error
	CapabilityRelations(ctx context.Context, tenantID string) ([]domain.CapabilityRelation, error)
	GraphSearch(ctx context.Context, tenantID, q string, kinds []string, limit int) ([]domain.GraphNode, error)
	GraphNeighborhood(ctx context.Context, tenantID, ref string, depth int) (domain.Graph, error)
	CapabilityUsage(ctx context.Context, tenantID string, since time.Time, limit int) (map[string]domain.GraphNode, error)
}

type Service struct {
	Store      Store
	Tools      *tools.Registry
	Extensions *extensions.Runtime
	Plugins    []PluginInfo
	Log        *slog.Logger
}

type PluginInfo struct {
	Kind     string
	Name     string
	Instance string
}

func ToolRef(d tools.Descriptor) string {
	version := d.Version
	if version == "" {
		version = "0"
	}
	return "tool:" + d.Name + "@" + version
}

func SkillRef(tenantID string, s cognitive.Skill) string {
	return cognitive.SkillRef(tenantID, s)
}

func MCPServerRef(tenantID, name string, version int64) string {
	return "mcp-server:" + tenantID + "/" + name + "@" + strconv.FormatInt(version, 10)
}

func PluginRef(kind, name string) string {
	return "plugin:" + kind + "/" + name
}

// Sync projects the tenant's visible tools, skills, MCP servers and active
// plugins into capability_nodes and derived relations. It is idempotent.
func (s *Service) Sync(ctx context.Context, tenantID string) error {
	if tenantID == "" {
		return errors.New("tenant is required")
	}
	nodes := make([]domain.CapabilityNode, 0, 32)
	relations := make([]domain.CapabilityRelation, 0, 16)
	servers := map[string]string{}

	if s.Extensions != nil {
		recs, err := s.Extensions.Store.ListExtensions(ctx, tenantID)
		if err != nil {
			return err
		}
		for _, rec := range recs {
			if rec.Kind != extensions.KindMCP {
				continue
			}
			ref := MCPServerRef(tenantID, rec.Name, rec.Version)
			servers[rec.Name] = ref
			nodes = append(nodes, domain.CapabilityNode{
				TenantID: tenantID, Ref: ref, Kind: "mcp_server", Name: rec.Name,
				Version: strconv.FormatInt(rec.Version, 10), Source: rec.UpdatedBy,
			})
		}
		for _, sk := range s.Extensions.Skills(tenantID) {
			nodes = append(nodes, domain.CapabilityNode{
				TenantID: tenantID, Ref: SkillRef(tenantID, sk), Kind: "skill", Name: sk.Name,
			})
		}
	}

	if s.Tools != nil {
		for _, d := range s.Tools.DescriptorsFor(tenantID) {
			ref := ToolRef(d)
			nodes = append(nodes, domain.CapabilityNode{
				TenantID: tenantID, Ref: ref, Kind: "tool", Name: d.Name, Version: d.Version,
				Source: d.Source, Risk: string(d.Risk), Trust: d.Trust,
			})
			if name, ok := strings.CutPrefix(d.Source, "mcp:"); ok {
				serverRef := servers[name]
				if serverRef == "" {
					serverRef = "mcp-server:" + tenantID + "/" + name + "@0"
				}
				relations = append(relations, domain.CapabilityRelation{
					TenantID: tenantID, SrcRef: serverRef, DstRef: ref, Relation: "provides",
					Source: "derived", Weight: 1,
				})
			}
		}
	}

	for _, p := range s.Plugins {
		name := p.Instance
		if name == "" {
			name = p.Name
		}
		nodes = append(nodes, domain.CapabilityNode{
			TenantID: tenantID, Ref: PluginRef(p.Kind, name), Kind: "plugin", Name: p.Name,
			Labels: map[string]any{"kind": p.Kind, "instance": p.Instance},
		})
	}

	if err := s.Store.UpsertCapabilityNodes(ctx, nodes); err != nil {
		return err
	}
	for _, rel := range relations {
		if err := s.Store.UpsertCapabilityRelation(ctx, rel); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Capabilities(ctx context.Context, tenantID string) ([]domain.CapabilityNode, error) {
	if err := s.Sync(ctx, tenantID); err != nil {
		if s.Log != nil {
			s.Log.Warn("capability sync failed", "tenant", tenantID, "err", err)
		}
	}
	return s.Store.CapabilityNodes(ctx, tenantID)
}

func (s *Service) Search(ctx context.Context, tenantID, q string, kinds []string, limit int) ([]domain.GraphNode, error) {
	return s.Store.GraphSearch(ctx, tenantID, q, kinds, limit)
}

func (s *Service) Neighborhood(ctx context.Context, tenantID, ref string, depth int) (domain.Graph, error) {
	return s.Store.GraphNeighborhood(ctx, tenantID, ref, depth)
}

func (s *Service) Usage(ctx context.Context, tenantID string, since time.Time, limit int) (map[string]domain.GraphNode, error) {
	return s.Store.CapabilityUsage(ctx, tenantID, since, limit)
}

func (s *Service) Relate(ctx context.Context, rel domain.CapabilityRelation) error {
	if rel.Source == "" {
		rel.Source = "declared"
	}
	if rel.Source != "declared" {
		return errors.New("relations written through the API are declared")
	}
	switch rel.Relation {
	case "provides", "belongs_to", "depends_on", "alternative_to", "composes", "precedes", "related_to":
	default:
		return errors.New("unsupported relation")
	}
	if rel.SrcRef == "" || rel.DstRef == "" || rel.SrcRef == rel.DstRef {
		return errors.New("src_ref and dst_ref are required and must differ")
	}
	if rel.Weight == 0 {
		rel.Weight = 1
	}
	return s.Store.UpsertCapabilityRelation(ctx, rel)
}

func (s *Service) Unrelate(ctx context.Context, tenantID, src, dst, relation string) error {
	if src == "" || dst == "" || relation == "" {
		return errors.New("src_ref, dst_ref and relation are required")
	}
	return s.Store.DeleteCapabilityRelation(ctx, tenantID, src, dst, relation, "declared")
}
