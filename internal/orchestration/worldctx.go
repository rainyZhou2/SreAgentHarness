package orchestration

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/envctx"
	"go-sre-harness/internal/world"
)

// SnapshotLabel is the run label carrying the World snapshot pinned for a
// Work run (RFC §7.5 PinForRun): every fact the run sees comes from it.
const SnapshotLabel = "world_snapshot"

// WorldContext serves World facts to runs as an envctx provider (RFC §7.6:
// envctx stays the prompt-facing adapter, World is the system of record).
// A run started by a Work reads the snapshot pinned for it; other runs read
// the current facts of their services.
type WorldContext struct {
	World *world.Service
	// MaxFacts bounds the facts contributed per run (default 20).
	MaxFacts int
}

// Name implements envctx.Provider.
func (WorldContext) Name() string { return "world" }

// Facts implements envctx.Provider.
func (wc WorldContext) Facts(ctx context.Context, q envctx.Query) ([]domain.ContextFact, error) {
	var snap world.Snapshot
	var err error
	if id := q.Labels[SnapshotLabel]; id != "" {
		snap, err = wc.World.Store.GetSnapshot(ctx, q.TenantID, id)
		if err != nil {
			return nil, fmt.Errorf("pinned snapshot %s: %w", id, err)
		}
	} else {
		refs := serviceRefs(q.Services)
		if len(refs) == 0 {
			return nil, nil
		}
		snap, err = wc.World.Snapshot(ctx, world.SnapshotRequest{TenantID: q.TenantID, Subjects: refs, Depth: 1, MaxNodes: 50})
		if err != nil {
			return nil, err
		}
	}
	max := wc.MaxFacts
	if max <= 0 {
		max = 20
	}
	stale := map[string]bool{}
	for _, id := range snap.Stale {
		stale[id] = true
	}
	var out []domain.ContextFact
	for _, f := range snap.Facts {
		if len(out) >= max {
			break
		}
		at := f.ValidFrom
		note := ""
		if stale[f.ID] {
			note = ", stale"
		}
		out = append(out, domain.ContextFact{Provider: "world", Kind: "fact:" + f.Predicate, Subject: f.SubjectRef,
			Summary: fmt.Sprintf("%s = %s (source %s, trust %s, since %s%s; snapshot %s)", f.Predicate,
				cpkit.Truncate(f.Value, 300), f.Source, f.TrustTier, f.ValidFrom.UTC().Format(time.RFC3339), note, snap.ID),
			At: &at, Ref: "fact:" + f.ID, Trust: f.TrustTier})
	}
	for _, r := range snap.Relations {
		if len(out) >= max {
			break
		}
		at := r.ValidFrom
		out = append(out, domain.ContextFact{Provider: "world", Kind: "relation:" + r.Type, Subject: r.FromRef,
			Summary: fmt.Sprintf("%s %s %s (source %s)", r.FromRef, r.Type, r.ToRef, r.Source), At: &at,
			Ref: "relation:" + r.ID, Trust: world.TrustFirstParty})
	}
	return out, nil
}

func serviceRefs(services []string) []string {
	var out []string
	for _, s := range services {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if !strings.Contains(s, "://") {
			s = "service://" + s
		}
		if ref, err := cpkit.NormalizeRef(s); err == nil {
			out = append(out, ref)
		}
	}
	return out
}

var predicateClean = regexp.MustCompile(`[^a-z0-9_.:-]+`)

// EnvFactRecorder records the facts context providers return for runs into
// the World as low-trust history (source "envctx:<provider>"), so what an
// agent was told is queryable "as known at" later. Facts the World itself
// served are skipped.
type EnvFactRecorder struct {
	Store world.Store
	Clock cpkit.Clock
	Log   *slog.Logger
}

// Record implements envctx.Gatherer.OnFacts.
func (r EnvFactRecorder) Record(ctx context.Context, q envctx.Query, facts []domain.ContextFact) {
	now := r.Clock.Now()
	for _, f := range facts {
		if f.Provider == "world" || f.Subject == "" || f.Summary == "" {
			continue
		}
		refs := serviceRefs([]string{f.Subject})
		if len(refs) == 0 {
			continue
		}
		pred := "env." + strings.Trim(predicateClean.ReplaceAllString(strings.ToLower(f.Kind), "_"), "_")
		if len(pred) > 120 {
			pred = pred[:120]
		}
		trust := f.Trust
		switch trust {
		case world.TrustAuthoritative, world.TrustFirstParty, world.TrustTelemetry, world.TrustUntrusted:
		default:
			trust = world.TrustUntrusted
		}
		valid := now
		if f.At != nil && !f.At.IsZero() && !f.At.After(now) {
			valid = *f.At
		}
		fact := world.Fact{TenantID: q.TenantID, SubjectRef: refs[0], Predicate: pred,
			Value: cpkit.Truncate(f.Summary, 1000), Source: "envctx:" + f.Provider, TrustTier: trust,
			ValidFrom: valid, ObservedAt: now, EvidenceRef: f.Ref, Confidence: 0.5}
		if _, _, err := r.Store.AssertFact(ctx, fact); err != nil && r.Log != nil {
			r.Log.Debug("record context fact", "provider", f.Provider, "subject", fact.SubjectRef, "err", err)
		}
	}
}
