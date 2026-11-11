package sre

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/outcome"
)

// ServingPool is one pool of a prefill/decode-separated deployment.
type ServingPool struct {
	// Ref is the pool's Deployment (deployment://<ns>/<name>).
	Ref string `json:"ref"`
	// Selector is the PromQL label matcher of the pool's series.
	Selector string `json:"selector"`
	// QueueLimit marks the pool as backed up (default: the service's).
	QueueLimit float64 `json:"queue_limit,omitempty"`
	// MinReplicas and MaxReplicas bound scaling; MaxStep bounds one action
	// (default 2 replicas).
	MinReplicas    int `json:"min_replicas,omitempty"`
	MaxReplicas    int `json:"max_replicas"`
	MaxStep        int `json:"max_step,omitempty"`
	GPUsPerReplica int `json:"gpus_per_replica,omitempty"`
}

// CostModel prices serving capacity (design §14.6): cost is minimized
// inside the reliability constraints, never traded against them.
type CostModel struct {
	GPUHourUSD float64 `json:"gpu_hour_usd"`
	// MaxHourlyUSD is the budget a scale-out may not exceed (0: no cap).
	MaxHourlyUSD float64 `json:"max_hourly_usd,omitempty"`
}

// HourlyUSD is the cost of running gpus for one hour.
func (c *CostModel) HourlyUSD(gpus float64) float64 {
	if c == nil {
		return 0
	}
	return gpus * c.GPUHourUSD
}

// v3 metric keys and their default series per engine. Engines rename
// metrics between versions: the service's "metrics" map overrides any key.
var engineExtra = map[string]map[string]string{
	"vllm": {"tpot": "vllm:time_per_output_token_seconds_bucket", "e2e": "vllm:e2e_request_latency_seconds_bucket",
		"prompt": "vllm:prompt_tokens_total", "cache_hits": "vllm:gpu_prefix_cache_hits_total",
		"cache_queries": "vllm:gpu_prefix_cache_queries_total"},
	"sglang": {"tpot": "sglang:time_per_output_token_seconds_bucket", "e2e": "sglang:e2e_request_latency_seconds_bucket",
		"prompt": "sglang:prompt_tokens_total", "cache_hit_rate": "sglang:cache_hit_rate"},
}

var metricNameRe = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]{0,199}$`)

func (s InferenceService) metric(key string) string {
	if v := s.Metrics[key]; v != "" {
		return v
	}
	switch m := engines[s.Engine]; key {
	case "queue":
		return m.queue
	case "running":
		return m.running
	}
	return engineExtra[s.Engine][key]
}

func (s InferenceService) validateV3() error {
	for k, v := range s.Metrics {
		if !metricNameRe.MatchString(v) {
			return fmt.Errorf("metrics.%s is not a metric name", k)
		}
	}
	if s.TPOTSLO < 0 || s.E2ESLO < 0 || s.GPUsPerReplica < 0 {
		return fmt.Errorf("SLOs and gpus_per_replica must not be negative")
	}
	for name, p := range map[string]*ServingPool{"prefill": s.Prefill, "decode": s.Decode} {
		if p == nil {
			continue
		}
		ref, err := cpkit.NormalizeRef(p.Ref)
		if err != nil || cpkit.RefScheme(ref) != "deployment" {
			return fmt.Errorf("%s.ref must be a deployment:// ref", name)
		}
		p.Ref = ref
		if !selectorRe.MatchString(p.Selector) {
			return fmt.Errorf("%s.selector must be label matchers", name)
		}
		if p.MaxReplicas < 1 || p.MinReplicas < 0 || p.MinReplicas > p.MaxReplicas || p.MaxStep < 0 || p.GPUsPerReplica < 0 {
			return fmt.Errorf("%s: need 0 <= min_replicas <= max_replicas and max_replicas >= 1", name)
		}
		if p.MinReplicas == 0 {
			p.MinReplicas = 1
		}
		if p.MaxStep == 0 {
			p.MaxStep = 2
		}
	}
	if c := s.Cost; c != nil && (c.GPUHourUSD <= 0 || c.MaxHourlyUSD < 0) {
		return fmt.Errorf("cost.gpu_hour_usd must be positive")
	}
	return nil
}

// podSelector is the DCGM matcher of the service's pods (the exporter
// labels each GPU with the pod using it).
func (s InferenceService) podSelector() string {
	_, path, _ := strings.Cut(s.Ref, "://")
	ns, name, ok := strings.Cut(path, "/")
	if !ok {
		return ""
	}
	return `namespace="` + labelValue(ns) + `",pod=~"` + regexp.QuoteMeta(name) + `-.*"`
}

// exprV3 renders the v3 queries; ok is false for a query it does not own.
func (s InferenceService) exprV3(query string) (string, bool, error) {
	sel := "{" + s.Selector + "}"
	need := func(key string) (string, error) {
		m := s.metric(key)
		if m == "" {
			return "", fmt.Errorf("inference: engine %s has no default series for %q; set metrics.%s", s.Engine, key, key)
		}
		return m, nil
	}
	quantile := func(key string) (string, bool, error) {
		m, err := need(key)
		if err != nil {
			return "", true, err
		}
		return fmt.Sprintf("histogram_quantile(0.95, sum by (le) (rate(%s%s[5m])))", m, sel), true, nil
	}
	pool := func(p *ServingPool, name string) (string, bool, error) {
		if p == nil {
			return "", true, fmt.Errorf("inference: %s has no %s pool configured", s.Ref, name)
		}
		m, err := need("queue")
		if err != nil {
			return "", true, err
		}
		return fmt.Sprintf("sum(%s{%s})", m, p.Selector), true, nil
	}
	gpus := func() (string, error) {
		ps := s.podSelector()
		if ps == "" {
			return "", fmt.Errorf("inference: %s has no namespace/name to find its GPUs by", s.Ref)
		}
		return "count(DCGM_FI_DEV_GPU_UTIL{" + ps + "})", nil
	}
	switch query {
	case "tpot_p95":
		return quantile("tpot")
	case "e2e_p95":
		return quantile("e2e")
	case "prompt_tokens_per_s":
		m, err := need("prompt")
		return fmt.Sprintf("sum(rate(%s%s[5m]))", m, sel), true, err
	case "cache_hit_rate":
		if m := s.metric("cache_hit_rate"); m != "" {
			return fmt.Sprintf("avg(%s%s)", m, sel), true, nil
		}
		hits, err := need("cache_hits")
		if err != nil {
			return "", true, err
		}
		queries, err := need("cache_queries")
		return fmt.Sprintf("sum(rate(%s%s[5m])) / sum(rate(%s%s[5m]))", hits, sel, queries, sel), true, err
	case "prefill_queue":
		return pool(s.Prefill, "prefill")
	case "decode_queue":
		return pool(s.Decode, "decode")
	case "gpu_count":
		e, err := gpus()
		return e, true, err
	case "tokens_per_gpu_second":
		g, err := gpus()
		if err != nil {
			return "", true, err
		}
		gen := s.Metrics["gen"]
		if gen == "" {
			gen = engines[s.Engine].gen
		}
		return fmt.Sprintf("sum(rate(%s%s[5m])) / %s", gen, sel, g), true, nil
	case "hourly_cost_usd":
		if s.Cost == nil {
			return "", true, fmt.Errorf("inference: %s has no cost model", s.Ref)
		}
		g, err := gpus()
		return fmt.Sprintf("%s * %g", g, s.Cost.GPUHourUSD), true, err
	}
	return "", false, nil
}

// snapshotV3 reads the optional signals. A signal that cannot be read is
// left out: the diagnosis then says less, it does not guess.
func (i *inference) snapshotV3(ctx context.Context, s InferenceService, snap *snapshot) {
	read := func(query string, dst **float64) {
		expr, err := s.expr(query)
		if err != nil {
			return
		}
		if v, ok, err := i.p.scalar(ctx, expr); err == nil && ok {
			*dst = &v
		}
	}
	if s.TPOTSLO > 0 || s.Metrics["tpot"] != "" {
		read("tpot_p95", &snap.TPOTp95)
	}
	if s.E2ESLO > 0 || s.Metrics["e2e"] != "" {
		read("e2e_p95", &snap.E2Ep95)
	}
	if s.Prefill != nil {
		read("prefill_queue", &snap.PrefillQueue)
	}
	if s.Decode != nil {
		read("decode_queue", &snap.DecodeQueue)
	}
	if s.metric("cache_hit_rate") != "" || s.metric("cache_hits") != "" && s.Metrics["cache_hits"] != "" {
		read("cache_hit_rate", &snap.CacheHitRate)
	}
}

// diagnosePD tells where a prefill/decode-separated service is short
// (design §14.4): TTFT is governed by the prefill path, TPOT by decode.
//   - decode_backpressure: TPOT breaches its SLO, or the decode pool is
//     backed up — streaming tokens are late, and scaling prefill would not
//     help;
//   - prefill_saturation: the prefill pool is backed up while TPOT holds —
//     requests wait before their first token.
//
// ok is false when the signals do not decide; the caller falls back to
// the single-pool classification.
func diagnosePD(s InferenceService, snap snapshot) (Diagnosis, bool) {
	var d Diagnosis
	limit := func(p *ServingPool) float64 {
		if p != nil && p.QueueLimit > 0 {
			return p.QueueLimit
		}
		return s.QueueLimit
	}
	tpotBreach := s.TPOTSLO > 0 && snap.TPOTp95 != nil && *snap.TPOTp95 > s.TPOTSLO
	decodeBacked := snap.DecodeQueue != nil && *snap.DecodeQueue >= limit(s.Decode)
	prefillBacked := snap.PrefillQueue != nil && *snap.PrefillQueue >= limit(s.Prefill)
	if snap.TPOTp95 != nil && s.TPOTSLO > 0 {
		d.Evidence = append(d.Evidence, fmt.Sprintf("TPOT p95 %.3fs (SLO %.3fs)", *snap.TPOTp95, s.TPOTSLO))
	}
	if snap.PrefillQueue != nil {
		d.Evidence = append(d.Evidence, fmt.Sprintf("prefill queue %.0f (limit %.0f)", *snap.PrefillQueue, limit(s.Prefill)))
	}
	if snap.DecodeQueue != nil {
		d.Evidence = append(d.Evidence, fmt.Sprintf("decode queue %.0f (limit %.0f)", *snap.DecodeQueue, limit(s.Decode)))
	}
	if snap.CacheHitRate != nil {
		d.Evidence = append(d.Evidence, fmt.Sprintf("prefix cache hit rate %.0f%%", *snap.CacheHitRate*100))
	}
	switch {
	case tpotBreach || decodeBacked:
		d.Cause = "decode_backpressure"
		d.Suggested = "deployment.scale (add replicas)"
		if s.Decode != nil {
			d.Suggested = "serving.scale_decode on " + s.Ref
		}
	case prefillBacked:
		d.Cause = "prefill_saturation"
		d.Suggested = "serving.scale_prefill on " + s.Ref
	default:
		return Diagnosis{}, false
	}
	return d, true
}

// InferenceSuccess is the success spec of an inference Work under several
// objectives (design §14.5): TTFT and, when configured, TPOT and end-to-end
// latency within their SLOs over a stable window, and the hourly cost
// inside the budget. Higher GPU utilization alone is never success.
func InferenceSuccess(s InferenceService) outcome.Spec {
	window := cpkit.Duration(10 * time.Minute)
	metric := func(id, query, op string, threshold float64, optional bool) outcome.Objective {
		return outcome.Objective{ID: id, Kind: outcome.KindMetric, TargetRef: s.Ref, Window: window, MinSamples: 3, Optional: optional,
			Metric: &outcome.MetricExpr{Source: "inference", Query: query, Aggregate: outcome.AggMax, Operator: op, Threshold: threshold}}
	}
	spec := outcome.Spec{Version: 1, StableFor: window, AllOf: []outcome.Objective{metric("ttft", "ttft_p95", "<", s.TTFTSLO, false)}}
	if s.TPOTSLO > 0 {
		spec.AllOf = append(spec.AllOf, metric("tpot", "tpot_p95", "<", s.TPOTSLO, false))
	}
	if s.E2ESLO > 0 {
		spec.AllOf = append(spec.AllOf, metric("e2e", "e2e_p95", "<", s.E2ESLO, false))
	}
	if s.Cost != nil && s.Cost.MaxHourlyUSD > 0 {
		spec.AllOf = append(spec.AllOf, metric("cost", "hourly_cost_usd", "<=", s.Cost.MaxHourlyUSD, false))
	}
	return spec
}

// ---- serving.scale_prefill / serving.scale_decode ----

// servingScale scales one pool of a prefill/decode-separated service. The
// target is the service; which Deployment that means is operator
// configuration, so a proposal cannot point the action at another workload.
//
// Guards, all checked in Preflight and again at dispatch:
//   - the pool's replica bounds and the step per action;
//   - capacity: a scale-out needs that many idle GPUs in the cluster, a
//     scale-in is refused while the pool is backed up;
//   - cost: the hourly cost after the change stays inside the budget.
type servingScale struct {
	i    *inference
	pool string // prefill | decode
}

func (a *servingScale) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "serving.scale_" + a.pool, Version: "1", ResourceKinds: []string{"deployment", "service"},
		Risk:        domain.RiskR2,
		Description: "Set the replicas of the " + a.pool + " pool of a prefill/decode-separated inference service, within its capacity and cost guards.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"replicas":{"type":"integer","minimum":1}},` +
			`"required":["replicas"],"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, SupportsRollback: true, Compensation: "serving.scale_" + a.pool,
		BlastRadius: "one_serving_pool", Timeout: 30 * time.Second}
}

type scalePlan struct {
	svc      InferenceService
	pool     *ServingPool
	ns, name string
	d        k8sDeployment
	cur, to  int
}

func (a *servingScale) plan(ctx context.Context, p action.Plan) (scalePlan, error) {
	var sp scalePlan
	svc, err := a.i.service(p.TargetRef)
	if err != nil {
		return sp, capability.NotApplied("%v", err)
	}
	sp.svc, sp.pool = svc, svc.Prefill
	if a.pool == "decode" {
		sp.pool = svc.Decode
	}
	if sp.pool == nil {
		return sp, capability.NotApplied("%s has no %s pool configured", svc.Ref, a.pool)
	}
	var args struct {
		Replicas *int `json:"replicas"`
	}
	if err := parseArgs(p, &args); err != nil {
		return sp, err
	}
	if args.Replicas == nil {
		return sp, capability.NotApplied("replicas is required")
	}
	sp.to = *args.Replicas
	if sp.to < sp.pool.MinReplicas || sp.to > sp.pool.MaxReplicas {
		return sp, capability.NotApplied("replicas must be within [%d, %d] for the %s pool", sp.pool.MinReplicas, sp.pool.MaxReplicas, a.pool)
	}
	if sp.ns, sp.name, err = a.i.k.nsName(sp.pool.Ref, "deployment"); err != nil {
		return sp, capability.NotApplied("%v", err)
	}
	if sp.d, err = a.i.k.deployment(ctx, sp.ns, sp.name); err != nil {
		return sp, err // a failed read: nothing was sent
	}
	sp.cur = int(sp.d.replicas())
	if annotated(sp.d.Metadata, p.EffectKey) {
		return sp, capability.NotApplied("this scaling was already applied")
	}
	if sp.to == sp.cur {
		return sp, capability.NotApplied("the %s pool already has %d replicas", a.pool, sp.cur)
	}
	if step := sp.to - sp.cur; step > sp.pool.MaxStep || -step > sp.pool.MaxStep {
		return sp, capability.NotApplied("scaling %d -> %d exceeds %d replicas per action", sp.cur, sp.to, sp.pool.MaxStep)
	}
	return sp, nil
}

// guards checks capacity and cost for a plan that is within its bounds.
func (a *servingScale) guards(ctx context.Context, sp scalePlan) error {
	delta := sp.to - sp.cur
	gpr := sp.pool.GPUsPerReplica
	if gpr == 0 {
		gpr = sp.svc.GPUsPerReplica
	}
	if delta < 0 {
		// Capacity guard for a scale-in: never shrink a pool that is behind.
		expr, err := sp.svc.expr(a.pool + "_queue")
		if err != nil {
			return capability.NotApplied("%v", err)
		}
		q, ok, err := a.i.p.scalar(ctx, expr)
		if err != nil || !ok {
			return capability.NotApplied("the %s queue cannot be read; a scale-in needs it", a.pool)
		}
		limit := sp.pool.QueueLimit
		if limit <= 0 {
			limit = sp.svc.QueueLimit
		}
		if q >= limit/2 {
			return capability.NotApplied("the %s pool has %.0f queued requests; scaling in would breach the SLO", a.pool, q)
		}
		return nil
	}
	if gpr > 0 {
		// Capacity guard for a scale-out: the GPUs must exist and be idle.
		need := float64(delta * gpr)
		free, ok, err := a.i.p.scalar(ctx, `count(DCGM_FI_DEV_GPU_UTIL{pod=""})`)
		if err != nil {
			return capability.NotApplied("free GPU capacity cannot be read; a scale-out needs it")
		}
		if !ok {
			free = 0
		}
		if free < need {
			return capability.NotApplied("scaling out needs %.0f idle GPUs, %.0f are free", need, free)
		}
		if c := sp.svc.Cost; c != nil && c.MaxHourlyUSD > 0 {
			expr, err := sp.svc.expr("gpu_count")
			if err != nil {
				return capability.NotApplied("%v", err)
			}
			cur, _, err := a.i.p.scalar(ctx, expr)
			if err != nil {
				return capability.NotApplied("the current GPU count cannot be read; the cost guard needs it")
			}
			if after := c.HourlyUSD(cur + need); after > c.MaxHourlyUSD {
				return capability.NotApplied("hourly cost would be $%.2f, the budget is $%.2f", after, c.MaxHourlyUSD)
			}
		}
	}
	return nil
}

func (a *servingScale) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	sp, err := a.plan(ctx, p)
	if err == nil {
		err = a.guards(ctx, sp)
	}
	if err != nil {
		return refused(err.Error()), nil
	}
	return action.PreflightResult{Allowed: true, EvidenceRefs: []string{fmt.Sprintf("%s#replicas=%d->%d", sp.pool.Ref, sp.cur, sp.to)}}, nil
}

func (a *servingScale) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	sp, err := a.plan(ctx, p)
	if err == nil {
		err = a.guards(ctx, sp)
	}
	if err != nil {
		return nil, capability.BeforeWrite(err)
	}
	patch := map[string]any{"metadata": map[string]any{"annotations": map[string]string{effectKeyAnno: key}},
		"spec": map[string]any{"replicas": sp.to}}
	if err := a.i.k.patch(ctx, "/apis/apps/v1/namespaces/"+sp.ns+"/deployments/"+sp.name, "application/merge-patch+json", patch, nil); err != nil {
		return nil, capability.ActionError(err)
	}
	return json.Marshal(map[string]any{"pool": a.pool, "deployment": sp.pool.Ref, "from": sp.cur, "to": sp.to, "dry_run": a.i.k.dryRun})
}

func (a *servingScale) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	svc, err := a.i.service(p.TargetRef)
	if err != nil {
		return action.InspectResult{}, err
	}
	pool := svc.Prefill
	if a.pool == "decode" {
		pool = svc.Decode
	}
	if pool == nil {
		return action.InspectResult{}, action.ErrUnsupported
	}
	ns, name, err := a.i.k.nsName(pool.Ref, "deployment")
	if err != nil {
		return action.InspectResult{}, err
	}
	d, err := a.i.k.deployment(ctx, ns, name)
	if err != nil {
		return action.InspectResult{}, err
	}
	applied := annotated(d.Metadata, p.EffectKey)
	return action.InspectResult{Applied: &applied}, nil
}

// RegisterActions offers the pool scaling actions when a service has
// pools and the Kubernetes client is configured.
func (i *inference) RegisterActions(r capability.ActionRegistry) error {
	if i.k == nil {
		return nil
	}
	pools := map[string]bool{}
	for _, s := range i.byRef {
		pools["prefill"] = pools["prefill"] || s.Prefill != nil
		pools["decode"] = pools["decode"] || s.Decode != nil
	}
	for _, name := range []string{"prefill", "decode"} {
		if pools[name] {
			if err := r.AddAction(&servingScale{i: i, pool: name}); err != nil {
				return err
			}
		}
	}
	return nil
}
