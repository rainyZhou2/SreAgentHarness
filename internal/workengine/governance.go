package workengine

import (
	"context"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/governance"
	"go-sre-harness/internal/observability"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/migrations"
)

// Deployment describes the process for the governance gate (the parts the
// engine cannot see for itself).
type Deployment struct {
	Tier           governance.Tier
	ReleaseVersion string
	BuildSHA       string
	StoreKind      string // postgres | sqlite
	AuthMode       string // dev | token | oidc
	ArtifactStore  string
	PolicyCustom   bool
	PolicyVersion  string
	AuditSink      bool
	// ConfigDigest identifies the effective configuration.
	ConfigDigest string
}

// govFlags converts the rollout switches for the gate.
func govFlags(f Flags) governance.Flags {
	return governance.Flags{Engine: f.Engine, Shadow: f.Shadow, Actions: f.Actions, ActionDryRun: f.ActionDryRun,
		RequireKnownTarget: f.RequireKnownTarget, RequireDistinctApprover: f.RequireDistinctApprover,
		InlineTemplates: f.InlineTemplates, ActionAllowlist: f.ActionAllowlist, ActionScope: f.ActionScope,
		Preauthorized: f.Preauthorized, ModelMaxDataClass: f.ModelMaxDataClass}
}

// ExpectedSchema is the newest migration compiled into this binary.
func ExpectedSchema() string {
	v := migrations.Versions()
	if len(v) == 0 {
		return ""
	}
	return v[len(v)-1]
}

// GateInput assembles what the governance gate judges.
func GateInput(d Deployment, f Flags, schema string, capabilities []string, actions []action.Descriptor, metricSources []string,
	sources []governance.SourceStatus, now time.Time) governance.Input {
	in := governance.Input{Tier: d.Tier, Flags: govFlags(f), ReleaseVersion: d.ReleaseVersion, BuildSHA: d.BuildSHA, SchemaVersion: schema,
		ExpectedSchema: ExpectedSchema(), Store: d.StoreKind, AuthMode: d.AuthMode, ArtifactStore: d.ArtifactStore,
		PolicyVersion: d.PolicyVersion, PolicyCustom: d.PolicyCustom, AuditSink: d.AuditSink, Capabilities: capabilities,
		MetricSources: metricSources, DataSources: sources, ConfigDigest: d.ConfigDigest, Now: now}
	in.PolicyDigest = governance.Digest(struct {
		Version string
		Custom  bool
	}{d.PolicyVersion, d.PolicyCustom})
	for _, a := range actions {
		in.Actions = append(in.Actions, governance.ActionInfo{Name: a.Name, Risk: string(a.Risk), IdempotencyMode: a.IdempotencyMode,
			HasVersionFact: a.VersionFact != ""})
	}
	sort.Slice(in.Actions, func(i, j int) bool { return in.Actions[i].Name < in.Actions[j].Name })
	return in
}

// sourceStatuses reads the configured sources for the gate. A source the
// catalog cannot return counts as disabled: the gate fails closed.
func sourceStatuses(ctx context.Context, store *cpsql.Store, cfg *ObservabilityConfig) []governance.SourceStatus {
	if cfg == nil {
		return nil
	}
	required := map[string]bool{}
	for _, r := range cfg.Required {
		required[r] = true
	}
	var out []governance.SourceStatus
	seen := map[string]bool{}
	add := func(tenant, id string) {
		key := tenant + "/" + id
		if seen[key] {
			return
		}
		seen[key] = true
		st := governance.SourceStatus{ID: key, State: observability.StateDisabled, Required: required[key]}
		if src, err := store.GetSource(ctx, tenant, id); err == nil {
			st.Kind, st.State = string(src.Kind), src.State
			if src.Health != nil {
				st.Probed, st.Healthy = true, src.Health.OK
			}
		}
		out = append(out, st)
	}
	for _, s := range cfg.Sources {
		add(s.TenantID, s.ID)
	}
	for r := range required {
		if tenant, id, ok := strings.Cut(r, "/"); ok {
			add(tenant, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Attestation evaluates the governance gate against the current state.
func (e *Engine) Attestation(ctx context.Context) governance.Attestation {
	schema := ExpectedSchema()
	if e.schema != nil {
		if v, err := e.schema(ctx); err == nil {
			schema = v
		}
	}
	in := GateInput(e.deployment, e.requested, schema, e.capabilities, e.actionDescs, e.metricNames,
		sourceStatuses(ctx, e.store, e.obsCfg), e.store.Clock.Now())
	return governance.Evaluate(in)
}
