package sre

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/world"
)

// GPU identity (design §14.2). A slot ("gpu-node-1/0") names a place in a
// chassis; the card in it can be replaced, and a MIG layout can be rebuilt.
// The stable identity of a GPU is its UUID:
//
//	gpu://<node>/<uuid>          the physical card (canonical)
//	gpu://<node>/<index>         the slot — an alias of the card in it
//	mig://<node>/<uuid>/<gi>     a MIG GPU instance of the card
//
// Facts and telemetry stay on the slot ref (the dashboards and alerts speak
// in slots). The canonical ref carries the identity: its entity holds the
// UUID as the immutable id, and the slot entity points at it, so an action
// planned for "slot 0" is bound to the card that was in slot 0 when it was
// approved — a replaced card invalidates the plan.

var gpuUUIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{3,79}$`)

// canonicalGPU returns the UUID-based ref of a row ("" without a usable
// UUID: the slot then stays its own, weaker identity).
func canonicalGPU(r *gpuRow) string {
	uuid := strings.ToLower(strings.TrimSpace(r.UUID))
	if !gpuUUIDRe.MatchString(uuid) {
		return ""
	}
	return "gpu://" + strings.ToLower(r.Host) + "/" + uuid
}

func migLayout(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = id + ":" + m[id]
	}
	return strings.Join(parts, ",")
}

// identity adds what the world needs to follow a GPU as a physical device:
// the canonical entity, the slot alias, the MIG instances, and topology
// events when the card in a slot or its MIG layout changed since the last
// collection.
func (g *gpuCap) identity(o *capability.Observation, r *gpuRow, slot, node, health string, now time.Time) {
	canon := canonicalGPU(r)
	t2 := world.TrustFirstParty
	if canon == "" {
		o.Entities = append(o.Entities, world.Entity{Ref: slot, Kind: "gpu", Attributes: map[string]string{"slot": r.Index}})
		return
	}
	uuid := strings.TrimPrefix(canon, "gpu://"+strings.ToLower(r.Host)+"/")
	o.Entities = append(o.Entities,
		world.Entity{Ref: canon, Kind: "gpu", Attributes: map[string]string{action.AttrUID: r.UUID, "slot": r.Index,
			"model": cpkit.Truncate(r.Model, 80), "node": strings.ToLower(r.Host)}},
		world.Entity{Ref: slot, Kind: "gpu", Attributes: map[string]string{action.AttrIdentity: canon, "slot": r.Index}})
	o.Facts = append(o.Facts,
		world.Fact{SubjectRef: slot, Predicate: "gpu_uuid", Value: r.UUID, TrustTier: t2},
		world.Fact{SubjectRef: canon, Predicate: "slot", Value: r.Index, TrustTier: t2},
		world.Fact{SubjectRef: canon, Predicate: "health", Value: health, TrustTier: world.TrustTelemetry})
	o.Relations = append(o.Relations, world.Relation{FromRef: node, Type: "has_gpu", ToRef: canon})
	if r.Pod != "" && dns1123.MatchString(r.Namespace) && dns1123.MatchString(r.Pod) {
		o.Relations = append(o.Relations, world.Relation{FromRef: "pod://" + r.Namespace + "/" + r.Pod, Type: "uses_gpu", ToRef: canon})
	}
	layout := migLayout(r.MIG)
	if layout != "" {
		o.Facts = append(o.Facts, world.Fact{SubjectRef: canon, Predicate: "mig_layout", Value: cpkit.Truncate(layout, 500), TrustTier: t2})
		for gi, profile := range r.MIG {
			if !regexp.MustCompile(`^[0-9]{1,3}$`).MatchString(gi) {
				continue
			}
			mig := "mig://" + strings.ToLower(r.Host) + "/" + uuid + "/" + gi
			o.Entities = append(o.Entities, world.Entity{Ref: mig, Kind: "mig", Attributes: map[string]string{
				action.AttrUID: r.UUID + "/" + gi, "profile": cpkit.Truncate(profile, 40)}})
			o.Facts = append(o.Facts, world.Fact{SubjectRef: mig, Predicate: "mig_profile", Value: cpkit.Truncate(profile, 40), TrustTier: t2})
			o.Relations = append(o.Relations, world.Relation{FromRef: canon, Type: "has_mig", ToRef: mig})
		}
	}

	g.mu.Lock()
	prevUUID, sawSlot := g.lastUUID[slot]
	prevLayout, sawLayout := g.lastMIG[canon]
	g.lastUUID[slot], g.lastMIG[canon] = r.UUID, layout
	g.mu.Unlock()
	// Topology changes are recorded as changes (they feed the change ledger
	// and the timeline of whatever runs on the node); they are not faults
	// and do not open an incident by themselves.
	if sawSlot && prevUUID != r.UUID {
		payload, _ := json.Marshal(map[string]any{"slot": slot, "previous_uuid": prevUUID, "uuid": r.UUID})
		o.Events = append(o.Events, eventing.Envelope{ID: "dcgm:replaced:" + slot + ":" + uuid, Source: "dcgm",
			Type: "topology.gpu_replaced", SubjectRef: node, OccurredAt: now, ObservedAt: now, Confidence: 0.95,
			Labels: map[string]string{"gpu": r.Index, "uuid": cpkit.Truncate(r.UUID, 64), "before": cpkit.Truncate(prevUUID, 64),
				"after": cpkit.Truncate(r.UUID, 64), "summary": "the GPU in slot " + r.Index + " was replaced"}, Payload: payload})
	}
	if sawLayout && prevLayout != layout {
		payload, _ := json.Marshal(map[string]any{"gpu": canon, "previous": prevLayout, "layout": layout})
		o.Events = append(o.Events, eventing.Envelope{ID: "dcgm:mig:" + canon + ":" + cpkit.HashBytes([]byte(layout))[7:19], Source: "dcgm",
			Type: "topology.gpu_mig_changed", SubjectRef: node, OccurredAt: now, ObservedAt: now, Confidence: 0.95,
			Labels:  map[string]string{"gpu": r.Index, "uuid": cpkit.Truncate(r.UUID, 64), "summary": "the MIG layout of the GPU changed"},
			Payload: payload})
	}
}
