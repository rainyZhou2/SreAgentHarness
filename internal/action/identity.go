package action

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/world"
)

// ResourceIdentity is the stable identity of an action target (design
// §7.2). Two side-effect channels address the same resources under
// different names: kernel tools key their effects by tool arguments
// ("prod/checkout"), actions by resource refs ("deployment://prod/checkout",
// "service://prod/checkout"). The target lock, the cross-ledger check, the
// approval and the audit all use the resolved identity, so an alias cannot
// slip a second write past a lock held under another name.
type ResourceIdentity struct {
	TenantID  string `json:"tenant_id"`
	Provider  string `json:"provider,omitempty"`
	Region    string `json:"region,omitempty"`
	Cluster   string `json:"cluster,omitempty"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
	// UID is the immutable id of the resource in its system of record (a
	// Kubernetes UID, a cloud resource id). A resource deleted and created
	// again under the same name has a new UID and is a different target.
	UID string `json:"uid,omitempty"`
	// Ref is the canonical resource ref every alias resolves to.
	Ref string `json:"ref"`
}

// Digest identifies the identity; it changes when the UID changes.
func (id ResourceIdentity) Digest() string {
	return cpkit.MustHash(struct{ T, P, R, C, K, NS, N, U, Ref string }{id.TenantID, id.Provider, id.Region, id.Cluster,
		id.Kind, id.Namespace, id.Name, id.UID, id.Ref})
}

// IdentityResolver maps a raw target ref to its identity and back to every
// name the resource is known under.
type IdentityResolver interface {
	ResolveTarget(ctx context.Context, tenantID, rawRef string) (ResourceIdentity, error)
	Aliases(ctx context.Context, id ResourceIdentity) ([]string, error)
}

// ErrAmbiguousTarget: the ref resolves to more than one resource.
var ErrAmbiguousTarget = errors.New("target identity is ambiguous")

// World entity attributes read by WorldIdentity.
const (
	// AttrIdentity on an entity points at the canonical ref of the resource
	// it is another name for (service://prod/checkout → deployment://prod/checkout).
	AttrIdentity = "identity"
	AttrUID      = "uid"
	AttrProvider = "provider"
	AttrRegion   = "region"
	AttrCluster  = "cluster"
	// AttrAlias is the free-form name world.Resolve matches.
	AttrAlias = "alias"
)

// WorldIdentity resolves identities from the world state: an entity may
// declare the canonical ref it is an alias of (attribute "identity") and
// carries the immutable id of the resource (attribute "uid").
type WorldIdentity struct {
	World *world.Service
}

func identityOf(tenantID, ref string, attrs map[string]string) ResourceIdentity {
	id := ResourceIdentity{TenantID: tenantID, Ref: ref, Kind: cpkit.RefScheme(ref), UID: attrs[AttrUID],
		Provider: attrs[AttrProvider], Region: attrs[AttrRegion], Cluster: attrs[AttrCluster]}
	_, path, _ := strings.Cut(ref, "://")
	parts := strings.Split(path, "/")
	id.Name = parts[len(parts)-1]
	if len(parts) >= 2 {
		id.Namespace = parts[len(parts)-2]
	}
	if len(parts) >= 3 && id.Cluster == "" {
		id.Cluster = parts[0]
	}
	return id
}

// ResolveTarget follows at most one alias hop: an alias of an alias is a
// modelling error and is reported, not chased.
func (w WorldIdentity) ResolveTarget(ctx context.Context, tenantID, rawRef string) (ResourceIdentity, error) {
	ref, err := cpkit.NormalizeRef(rawRef)
	if err != nil {
		return ResourceIdentity{}, err
	}
	ent, err := w.World.Store.GetEntity(ctx, tenantID, ref)
	switch {
	case errors.Is(err, world.ErrNotFound):
		// Unknown to the world: the ref is its own identity, without a UID.
		return identityOf(tenantID, ref, nil), nil
	case err != nil:
		return ResourceIdentity{}, err
	}
	if canon := ent.Attributes[AttrIdentity]; canon != "" && canon != ref {
		cref, err := cpkit.NormalizeRef(canon)
		if err != nil {
			return ResourceIdentity{}, fmt.Errorf("%w: entity %s names an invalid identity", ErrAmbiguousTarget, ref)
		}
		target, err := w.World.Store.GetEntity(ctx, tenantID, cref)
		switch {
		case errors.Is(err, world.ErrNotFound):
			return identityOf(tenantID, cref, nil), nil
		case err != nil:
			return ResourceIdentity{}, err
		}
		if next := target.Attributes[AttrIdentity]; next != "" && next != cref {
			return ResourceIdentity{}, fmt.Errorf("%w: %s is an alias of an alias (%s → %s → %s)", ErrAmbiguousTarget, ref, ref, cref, next)
		}
		return identityOf(tenantID, cref, target.Attributes), nil
	}
	return identityOf(tenantID, ref, ent.Attributes), nil
}

// Aliases lists every ref of the resource: the canonical one and the
// entities that declare it as their identity.
func (w WorldIdentity) Aliases(ctx context.Context, id ResourceIdentity) ([]string, error) {
	out := []string{id.Ref}
	ents, err := w.World.Store.FindEntities(ctx, id.TenantID, AttrIdentity, id.Ref, 50)
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		if e.Ref != id.Ref {
			out = append(out, e.Ref)
		}
	}
	sort.Strings(out)
	return out, nil
}

// identity resolves the target of a plan (the ref itself when no resolver
// is configured) with the refs and kernel-effect targets it is known by.
func (rt *Runtime) identity(ctx context.Context, tenantID, ref string) (id ResourceIdentity, refs, effectTargets []string, err error) {
	id = identityOf(tenantID, ref, nil)
	refs = []string{ref}
	if rt.Identity != nil {
		if id, err = rt.Identity.ResolveTarget(ctx, tenantID, ref); err != nil {
			return id, nil, nil, err
		}
		if refs, err = rt.Identity.Aliases(ctx, id); err != nil {
			return id, nil, nil, err
		}
		if !containsStr(refs, ref) {
			refs = append(refs, ref)
		}
	}
	seen := map[string]bool{}
	for _, r := range refs {
		for _, t := range EffectTargets(r) {
			if !seen[t] {
				seen[t] = true
				effectTargets = append(effectTargets, t)
			}
		}
	}
	return id, refs, effectTargets, nil
}

func containsStr(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// UnsettledFinder is optionally implemented by a Store: it lists actions on
// any of the refs whose effect is still open (DISPATCHED, EFFECT_UNKNOWN,
// MANUAL_RECOVERY). A target lock expires; an unknown effect does not, so
// the runtime asks before every dispatch.
type UnsettledFinder interface {
	UnsettledActions(ctx context.Context, tenantID string, targetRefs []string, exceptID string, limit int) ([]Record, error)
}

// FactQuality judges the facts an action relies on beyond their age: a
// collector that stopped reporting, and sources that disagree where no
// authority settles the value (design §10.3, §10.4). It returns the
// problems that must refuse the action; nil means the facts may be used.
type FactQuality interface {
	Problems(ctx context.Context, tenantID, target string, required []string, snap *world.Snapshot) []string
}
