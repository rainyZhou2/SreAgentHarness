package sre

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// AIInfraConfig configures the AI-infrastructure capabilities. GPU and
// inference telemetry is read from the Prometheus that scrapes the NVIDIA
// DCGM exporter and the serving engines (vLLM, SGLang); training jobs are
// read from the Kubernetes API (Kubeflow training operator), which needs
// the kubernetes section.
type AIInfraConfig struct {
	Prometheus capability.HTTPConfig `json:"prometheus"`
	GPU        *GPUConfig            `json:"gpu,omitempty"`
	Inference  []InferenceService    `json:"inference,omitempty"`
	// InferenceEvery is the inference collection interval (default 30s).
	InferenceEvery capability.Duration `json:"inference_every,omitempty"`
	Training       *TrainingConfig     `json:"training,omitempty"`
	// Group is the correlation group of AI-infra events (default
	// "service-health", shared with alerts and deploys).
	Group string `json:"group,omitempty"`
}

// GPUConfig configures DCGM collection.
type GPUConfig struct {
	CollectEvery capability.Duration `json:"collect_every,omitempty"` // default 60s
	// TempLimitC raises gpu.overheat above this temperature (default 87).
	TempLimitC float64 `json:"temp_limit_c,omitempty"`
	MaxGPUs    int     `json:"max_gpus,omitempty"` // default 4096
}

// InferenceService is one model deployment served by vLLM or SGLang.
type InferenceService struct {
	// Ref is the subject (deployment://<ns>/<name> lets the TTFT scenario
	// scale it; service://... works for investigation only).
	Ref    string `json:"ref"`
	Engine string `json:"engine"` // vllm | sglang
	// Selector is the PromQL label matcher of the engine's series, e.g.
	// namespace="prod",model_name="llama-3-70b".
	Selector string `json:"selector"`
	// TTFTSLO is the p95 time-to-first-token objective in seconds.
	TTFTSLO float64 `json:"ttft_slo_seconds"`
	// QueueLimit marks queue saturation in diagnosis (default 16).
	QueueLimit float64 `json:"queue_limit,omitempty"`
	// TPOTSLO and E2ESLO are the p95 time-per-output-token and end-to-end
	// latency objectives in seconds (optional).
	TPOTSLO float64 `json:"tpot_slo_seconds,omitempty"`
	E2ESLO  float64 `json:"e2e_slo_seconds,omitempty"`
	// Metrics overrides the engine's series names by key (ttft, tpot, e2e,
	// queue, running, kv, gen, prompt, cache_hits, cache_queries). Engine
	// versions rename their metrics; set the names your exporter serves.
	Metrics map[string]string `json:"metrics,omitempty"`
	// Prefill and Decode describe the pools of a prefill/decode-separated
	// deployment. With them the diagnosis tells a prefill backlog from
	// decode back-pressure and the serving.scale_* actions are available.
	Prefill *ServingPool `json:"prefill,omitempty"`
	Decode  *ServingPool `json:"decode,omitempty"`
	// GPUsPerReplica and Cost feed the cost guard and the cost queries.
	GPUsPerReplica int        `json:"gpus_per_replica,omitempty"`
	Cost           *CostModel `json:"cost,omitempty"`
}

// TrainingConfig configures training-job collection.
type TrainingConfig struct {
	CollectEvery capability.Duration `json:"collect_every,omitempty"` // default 60s
	// Kinds are training-operator resources (default pytorchjobs).
	Kinds []string `json:"kinds,omitempty"`
	// ProgressQuery is a PromQL template returning the step counter of a
	// job (${namespace}, ${name}); a job whose counter does not move for
	// StallAfter raises training.stalled.
	ProgressQuery string              `json:"progress_query,omitempty"`
	StallAfter    capability.Duration `json:"stall_after,omitempty"` // default 15m
}

var selectorRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(=|!=|=~|!~)"[^"\\]*"(,[a-zA-Z_][a-zA-Z0-9_]*(=|!=|=~|!~)"[^"\\]*")*$`)

// engine metric names.
type engineMetrics struct{ ttft, queue, running, kv, gen string }

var engines = map[string]engineMetrics{
	"vllm": {ttft: "vllm:time_to_first_token_seconds_bucket", queue: "vllm:num_requests_waiting",
		running: "vllm:num_requests_running", kv: "vllm:gpu_cache_usage_perc", gen: "vllm:generation_tokens_total"},
	"sglang": {ttft: "sglang:time_to_first_token_seconds_bucket", queue: "sglang:num_queue_reqs",
		running: "sglang:num_running_reqs", kv: "sglang:token_usage", gen: "sglang:generation_tokens_total"},
}

// inferenceQueries are the named queries of the "inference" metric source.
var inferenceQueries = []string{"ttft_p95", "ttft_p99", "queue_depth", "running", "kv_cache_usage", "gen_tokens_per_s",
	"tpot_p95", "e2e_p95", "prompt_tokens_per_s", "cache_hit_rate", "prefill_queue", "decode_queue", "gpu_count",
	"tokens_per_gpu_second", "hourly_cost_usd"}

func (s InferenceService) expr(query string) (string, error) {
	if e, ok, err := s.exprV3(query); ok || err != nil {
		return e, err
	}
	m := engines[s.Engine]
	for key, dst := range map[string]*string{"ttft": &m.ttft, "queue": &m.queue, "running": &m.running, "kv": &m.kv, "gen": &m.gen} {
		if v := s.Metrics[key]; v != "" {
			*dst = v
		}
	}
	sel := "{" + s.Selector + "}"
	switch query {
	case "ttft_p95", "ttft_p99":
		q := "0.95"
		if query == "ttft_p99" {
			q = "0.99"
		}
		return fmt.Sprintf("histogram_quantile(%s, sum by (le) (rate(%s%s[5m])))", q, m.ttft, sel), nil
	case "queue_depth":
		return fmt.Sprintf("sum(%s%s)", m.queue, sel), nil
	case "running":
		return fmt.Sprintf("sum(%s%s)", m.running, sel), nil
	case "kv_cache_usage":
		return fmt.Sprintf("max(%s%s)", m.kv, sel), nil
	case "gen_tokens_per_s":
		return fmt.Sprintf("sum(rate(%s%s[5m]))", m.gen, sel), nil
	}
	return "", fmt.Errorf("unknown inference query %q (supported: %s)", query, strings.Join(inferenceQueries, ", "))
}

// promAPI is the small Prometheus HTTP API surface the AI capabilities use.
type promAPI struct{ c *capability.Client }

type promSeries struct {
	Labels map[string]string
	Value  float64
}

func (p promAPI) instant(ctx context.Context, expr string, at time.Time) ([]promSeries, error) {
	var m promMatrix
	q := url.Values{"query": {expr}}
	if !at.IsZero() {
		q.Set("time", strconv.FormatInt(at.Unix(), 10))
	}
	if err := p.c.Get(ctx, "/api/v1/query", q, &m); err != nil {
		return nil, err
	}
	if m.Status != "success" {
		return nil, fmt.Errorf("prometheus: %s", m.Error)
	}
	out := make([]promSeries, 0, len(m.Data.Result))
	for _, r := range m.Data.Result {
		s, err := sampleOf(r.Value)
		if err != nil || math.IsNaN(s.V) || math.IsInf(s.V, 0) {
			continue
		}
		out = append(out, promSeries{Labels: r.Metric, Value: s.V})
	}
	return out, nil
}

// scalar runs an instant query expected to return at most one series.
func (p promAPI) scalar(ctx context.Context, expr string) (float64, bool, error) {
	ss, err := p.instant(ctx, expr, time.Time{})
	if err != nil || len(ss) == 0 {
		return 0, false, err
	}
	return ss[0].Value, true, nil
}

func (p promAPI) rangeQuery(ctx context.Context, expr string, q outcome.MetricQuery) (outcome.MetricResult, error) {
	span := q.End.Sub(q.Start)
	if span <= 0 || span > 24*time.Hour {
		return outcome.MetricResult{}, fmt.Errorf("range must be within (0, 24h]")
	}
	step := max(span/120, 15*time.Second)
	var m promMatrix
	if err := p.c.Get(ctx, "/api/v1/query_range", url.Values{"query": {expr},
		"start": {strconv.FormatInt(q.Start.Unix(), 10)}, "end": {strconv.FormatInt(q.End.Unix(), 10)},
		"step": {strconv.Itoa(int(step.Seconds()))}}, &m); err != nil {
		return outcome.MetricResult{}, err
	}
	if m.Status != "success" {
		return outcome.MetricResult{}, fmt.Errorf("prometheus: %s", m.Error)
	}
	if len(m.Data.Result) > 1 {
		return outcome.MetricResult{}, fmt.Errorf("query returned %d series, want 1", len(m.Data.Result))
	}
	var res outcome.MetricResult
	if len(m.Data.Result) == 1 {
		for _, v := range m.Data.Result[0].Values {
			s, err := sampleOf(v)
			if err != nil {
				return outcome.MetricResult{}, err
			}
			if !math.IsNaN(s.V) {
				res.Samples = append(res.Samples, s)
			}
		}
	}
	res.EvidenceRef = p.c.URL() + "/graph?g0.expr=" + url.QueryEscape(expr)
	return res, nil
}

func newAIInfra(cfg AIInfraConfig, k *kubernetes) ([]capability.Capability, error) {
	c, err := capability.NewClient(cfg.Prometheus)
	if err != nil {
		return nil, fmt.Errorf("ai_infra: prometheus: %w", err)
	}
	p := promAPI{c}
	group := cfg.Group
	if group == "" {
		group = "service-health"
	}
	var caps []capability.Capability
	if cfg.GPU != nil {
		g := *cfg.GPU
		if g.CollectEvery == 0 {
			g.CollectEvery = capability.Duration(time.Minute)
		}
		if g.TempLimitC <= 0 {
			g.TempLimitC = 87
		}
		if g.MaxGPUs <= 0 {
			g.MaxGPUs = 4096
		}
		caps = append(caps, &gpuCap{cfg: g, p: p, lastUUID: map[string]string{}, lastMIG: map[string]string{}})
	}
	if len(cfg.Inference) > 0 {
		inf := &inference{p: p, every: cfg.InferenceEvery.D(), group: group, byRef: map[string]InferenceService{}}
		if cfg.InferenceEvery == 0 {
			inf.every = 30 * time.Second
		}
		for _, s := range cfg.Inference {
			ref, err := cpkit.NormalizeRef(s.Ref)
			if err != nil {
				return nil, fmt.Errorf("ai_infra: inference ref %q: %w", s.Ref, err)
			}
			s.Ref = ref
			if _, ok := engines[s.Engine]; !ok {
				return nil, fmt.Errorf("ai_infra: %s: engine must be vllm or sglang", ref)
			}
			if !selectorRe.MatchString(s.Selector) {
				return nil, fmt.Errorf("ai_infra: %s: selector must be label matchers like namespace=\"prod\",model_name=\"m\"", ref)
			}
			if s.TTFTSLO <= 0 {
				return nil, fmt.Errorf("ai_infra: %s: ttft_slo_seconds is required", ref)
			}
			if s.QueueLimit <= 0 {
				s.QueueLimit = 16
			}
			if err := s.validateV3(); err != nil {
				return nil, fmt.Errorf("ai_infra: %s: %w", ref, err)
			}
			if _, dup := inf.byRef[ref]; dup {
				return nil, fmt.Errorf("ai_infra: %s configured twice", ref)
			}
			inf.byRef[ref] = s
			inf.order = append(inf.order, ref)
		}
		if k != nil {
			inf.k = k.k
		}
		caps = append(caps, inf)
	}
	if cfg.Training != nil {
		if k == nil {
			return nil, fmt.Errorf("ai_infra: training requires the kubernetes section")
		}
		t := *cfg.Training
		if t.CollectEvery == 0 {
			t.CollectEvery = capability.Duration(time.Minute)
		}
		if len(t.Kinds) == 0 {
			t.Kinds = []string{"pytorchjobs"}
		}
		for _, kind := range t.Kinds {
			if !dns1123.MatchString(kind) {
				return nil, fmt.Errorf("ai_infra: training kind %q", kind)
			}
		}
		if t.StallAfter <= 0 {
			t.StallAfter = capability.Duration(15 * time.Minute)
		}
		caps = append(caps, &training{cfg: t, k: k.k, p: p, group: group, progress: map[string]progress{}})
	}
	if len(caps) == 0 {
		return nil, fmt.Errorf("ai_infra: configure gpu, inference or training")
	}
	return caps, nil
}

// ================= GPU (DCGM) =================

type gpuCap struct {
	capability.Base
	cfg GPUConfig
	p   promAPI

	// What each slot held at the last collection, to notice a replaced
	// card or a changed MIG layout (see identity).
	mu       sync.Mutex
	lastUUID map[string]string
	lastMIG  map[string]string
}

func (g *gpuCap) ID() string      { return "gpu" }
func (g *gpuCap) Version() string { return "1" }

// criticalXID are XID codes that indicate a GPU needing service (NVIDIA
// XID catalog: 48 DBE, 63/64 row remap, 74 NVLink, 79 fallen off the bus,
// 94/95 contained/uncontained ECC).
var criticalXID = map[int]bool{48: true, 63: true, 64: true, 74: true, 79: true, 94: true, 95: true}

func (g *gpuCap) RegisterWorld(r capability.WorldRegistry) error {
	if g.cfg.CollectEvery <= 0 {
		return nil
	}
	return r.AddCollector("dcgm", g.cfg.CollectEvery.D(), capability.CollectorFunc(g.collect))
}

func (g *gpuCap) RegisterEvents(r capability.EventRegistry) error {
	// A GPU fault opens an incident on the node; a later inference alert
	// on a deployment is a separate subject (the TTFT Work links it via
	// diagnosis, not correlation).
	return r.AddRule(eventing.Rule{Name: "gpu-faults", Match: eventing.Predicate{Sources: []string{"dcgm"},
		EventTypes: []string{"gpu.*"}}, Key: "subject", WorkKind: "incident", Template: "incident",
		Title: "${type} on ${subject}", Intent: "restore GPU node health", Window: cpkit.Duration(time.Hour)})
}

type gpuRow struct {
	Host, Index, UUID, Model, Namespace, Pod string
	Util, MemUsed, MemFree, Temp, XID        float64
	// ECC double-bit errors, NVLink CRC/replay errors and PCIe replays
	// since the driver loaded (monotonic counters).
	ECCDBE, NVLinkErr, PCIeReplay float64
	// MIG maps a GPU instance id to its profile ("1g.10gb") when the card
	// is partitioned.
	MIG map[string]string
	has map[string]bool
}

func gpuKey(l map[string]string) string { return l["Hostname"] + "/" + l["gpu"] }

func (g *gpuCap) rows(ctx context.Context, filter string) ([]*gpuRow, error) {
	byKey := map[string]*gpuRow{}
	var keys []string
	for _, m := range []struct{ name, metric string }{{"util", "DCGM_FI_DEV_GPU_UTIL"}, {"mem", "DCGM_FI_DEV_FB_USED"},
		{"free", "DCGM_FI_DEV_FB_FREE"}, {"temp", "DCGM_FI_DEV_GPU_TEMP"}, {"xid", "DCGM_FI_DEV_XID_ERRORS"},
		{"ecc", "DCGM_FI_DEV_ECC_DBE_VOL_TOTAL"}, {"nvlink", "DCGM_FI_DEV_NVLINK_CRC_FLIT_ERROR_COUNT_TOTAL"},
		{"pcie", "DCGM_FI_DEV_PCIE_REPLAY_COUNTER"}} {
		expr := m.metric
		if filter != "" {
			expr += "{" + filter + "}"
		}
		ss, err := g.p.instant(ctx, expr, time.Time{})
		if err != nil {
			return nil, err
		}
		for _, s := range ss {
			if s.Labels["Hostname"] == "" || s.Labels["gpu"] == "" {
				continue
			}
			k := gpuKey(s.Labels)
			r := byKey[k]
			if r == nil {
				if len(byKey) >= g.cfg.MaxGPUs {
					return nil, fmt.Errorf("gpu: more than max_gpus=%d GPUs", g.cfg.MaxGPUs)
				}
				r = &gpuRow{Host: s.Labels["Hostname"], Index: s.Labels["gpu"], UUID: s.Labels["UUID"],
					Model: s.Labels["modelName"], has: map[string]bool{}}
				byKey[k] = r
				keys = append(keys, k)
			}
			if r.Pod == "" && s.Labels["pod"] != "" {
				r.Namespace, r.Pod = s.Labels["namespace"], s.Labels["pod"]
			}
			if gi := s.Labels["GPU_I_ID"]; gi != "" {
				if r.MIG == nil {
					r.MIG = map[string]string{}
				}
				r.MIG[gi] = s.Labels["GPU_I_PROFILE"]
			}
			r.has[m.name] = true
			switch m.name {
			case "util":
				r.Util = s.Value
			case "mem":
				r.MemUsed = s.Value
			case "free":
				r.MemFree = s.Value
			case "temp":
				r.Temp = s.Value
			case "xid":
				r.XID = s.Value
			case "ecc":
				r.ECCDBE = s.Value
			case "nvlink":
				r.NVLinkErr = s.Value
			case "pcie":
				r.PCIeReplay = s.Value
			}
		}
	}
	sort.Strings(keys)
	out := make([]*gpuRow, 0, len(keys))
	for _, k := range keys {
		out = append(out, byKey[k])
	}
	return out, nil
}

func (r *gpuRow) ref() (string, string, bool) {
	if !dns1123.MatchString(strings.ToLower(r.Host)) || !regexp.MustCompile(`^[0-9]{1,3}$`).MatchString(r.Index) {
		return "", "", false
	}
	host := strings.ToLower(r.Host)
	return "gpu://" + host + "/" + r.Index, "node://" + host, true
}

func (g *gpuCap) collect(ctx context.Context, now time.Time) (capability.Observation, error) {
	rows, err := g.rows(ctx, "")
	if err != nil {
		return capability.Observation{}, err
	}
	var o capability.Observation
	t3 := world.TrustTelemetry
	for _, r := range rows {
		ref, node, ok := r.ref()
		if !ok {
			continue
		}
		f := func(pred, v string) {
			o.Facts = append(o.Facts, world.Fact{SubjectRef: ref, Predicate: pred, Value: v, TrustTier: t3})
		}
		f("model", cpkit.Truncate(r.Model, 80))
		if r.has["util"] {
			f("util_pct", strconv.FormatFloat(r.Util, 'f', 0, 64))
		}
		if r.has["mem"] {
			f("mem_used_mib", strconv.FormatFloat(r.MemUsed, 'f', 0, 64))
		}
		if r.has["temp"] {
			f("temp_c", strconv.FormatFloat(r.Temp, 'f', 0, 64))
		}
		health := "ok"
		xid := int(r.XID)
		if xid != 0 {
			f("xid_last", strconv.Itoa(xid))
			if criticalXID[xid] {
				health = "faulted"
			}
		}
		if r.has["temp"] && r.Temp >= g.cfg.TempLimitC {
			health = "overheating"
		}
		if r.has["ecc"] {
			f("ecc_dbe_total", strconv.FormatFloat(r.ECCDBE, 'f', 0, 64))
			if r.ECCDBE > 0 {
				// A double-bit ECC error is uncorrectable memory corruption.
				health = "faulted"
			}
		}
		if r.has["nvlink"] {
			f("nvlink_errors_total", strconv.FormatFloat(r.NVLinkErr, 'f', 0, 64))
		}
		if r.has["pcie"] {
			f("pcie_replay_total", strconv.FormatFloat(r.PCIeReplay, 'f', 0, 64))
		}
		f("health", health)
		g.identity(&o, r, ref, node, health, now)
		o.Relations = append(o.Relations, world.Relation{FromRef: node, Type: "has_gpu", ToRef: ref})
		if r.Pod != "" && dns1123.MatchString(r.Namespace) && dns1123.MatchString(r.Pod) {
			o.Relations = append(o.Relations, world.Relation{FromRef: "pod://" + r.Namespace + "/" + r.Pod, Type: "uses_gpu", ToRef: ref})
		}
		// Events are deduplicated per GPU per hour: a persisting fault
		// stays one incident; the hourly key lets a recurrence reopen.
		hour := now.UTC().Format("2006010215")
		lbl := map[string]string{"gpu": r.Index, "uuid": cpkit.Truncate(r.UUID, 64), "model": cpkit.Truncate(r.Model, 64)}
		if xid != 0 && criticalXID[xid] {
			lbl["xid"] = strconv.Itoa(xid)
			payload, _ := json.Marshal(map[string]any{"gpu": ref, "xid": xid, "uuid": r.UUID})
			o.Events = append(o.Events, eventing.Envelope{ID: "dcgm:xid:" + ref + ":" + hour, Source: "dcgm", Type: "gpu.xid_error",
				SubjectRef: node, DedupKey: "dcgm:xid:" + ref + ":" + hour, OccurredAt: now, ObservedAt: now, Confidence: 0.9,
				Labels: lbl, Payload: payload})
		}
		if health == "overheating" {
			payload, _ := json.Marshal(map[string]any{"gpu": ref, "temp_c": r.Temp, "limit_c": g.cfg.TempLimitC})
			o.Events = append(o.Events, eventing.Envelope{ID: "dcgm:temp:" + ref + ":" + hour, Source: "dcgm", Type: "gpu.overheat",
				SubjectRef: node, DedupKey: "dcgm:temp:" + ref + ":" + hour, OccurredAt: now, ObservedAt: now, Confidence: 0.8,
				Labels: lbl, Payload: payload})
		}
	}
	return o, nil
}

func (g *gpuCap) RegisterTools(r capability.ToolRegistry) error {
	return r.AddTool(&tools.Func{Desc: tools.Descriptor{Name: "gpu.status", Version: "1", Risk: domain.RiskR0, Trust: "T3",
		Source: "native", TargetArg: "node",
		Description: "Per-GPU utilization, memory, temperature, last XID and the pod using it (DCGM), optionally for one node or namespace.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"node":{"type":"string"},"namespace":{"type":"string"}},"additionalProperties":false}`)},
		Fn: g.toolStatus})
}

func (g *gpuCap) toolStatus(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct{ Node, Namespace string }
	_ = json.Unmarshal(args, &a)
	var f []string
	if a.Node != "" {
		f = append(f, `Hostname="`+labelValue(a.Node)+`"`)
	}
	if a.Namespace != "" {
		f = append(f, `namespace="`+labelValue(a.Namespace)+`"`)
	}
	rows, err := g.rows(ctx, strings.Join(f, ","))
	if err != nil {
		return nil, err
	}
	type gpu struct {
		Node       string  `json:"node"`
		GPU        string  `json:"gpu"`
		Model      string  `json:"model,omitempty"`
		UtilPct    float64 `json:"util_pct"`
		MemUsedMiB float64 `json:"mem_used_mib"`
		MemFreeMiB float64 `json:"mem_free_mib"`
		TempC      float64 `json:"temp_c"`
		XID        int     `json:"xid_last,omitempty"`
		Critical   bool    `json:"xid_critical,omitempty"`
		Pod        string  `json:"pod,omitempty"`
	}
	out := []gpu{}
	for i, r := range rows {
		if i >= 256 {
			break
		}
		pod := ""
		if r.Pod != "" {
			pod = r.Namespace + "/" + r.Pod
		}
		out = append(out, gpu{r.Host, r.Index, r.Model, r.Util, r.MemUsed, r.MemFree, r.Temp, int(r.XID), criticalXID[int(r.XID)], pod})
	}
	return json.Marshal(map[string]any{"gpus": out, "total": len(rows)})
}

// ================= Inference (vLLM / SGLang) =================

type inference struct {
	capability.Base
	p     promAPI
	every time.Duration
	group string
	byRef map[string]InferenceService
	order []string
	// k is the Kubernetes client the serving.scale_* actions use (nil:
	// the actions are not registered).
	k *kube
}

func (i *inference) ID() string      { return "inference" }
func (i *inference) Version() string { return "1" }

func (i *inference) RegisterOutcomes(r capability.OutcomeRegistry) error {
	return r.AddMetricSource("inference", i)
}

func (i *inference) RegisterWorld(r capability.WorldRegistry) error {
	if i.every <= 0 {
		return nil
	}
	return r.AddCollector("serving", i.every, capability.CollectorFunc(i.collect))
}

func (i *inference) RegisterEvents(r capability.EventRegistry) error {
	return r.AddRule(eventing.Rule{Name: "inference-ttft", Group: i.group, Lookback: cpkit.Duration(time.Hour),
		Match: eventing.Predicate{Sources: []string{"inference"}, EventTypes: []string{"inference.ttft_breach"}},
		Key:   "subject", WorkKind: "incident", Template: "inference-latency", Window: cpkit.Duration(time.Hour),
		Title: "TTFT p95 above SLO on ${subject}", Intent: "restore time-to-first-token within SLO"})
}

func (i *inference) service(ref string) (InferenceService, error) {
	if s, ok := i.byRef[ref]; ok {
		return s, nil
	}
	return InferenceService{}, fmt.Errorf("inference: %s is not a configured inference service", ref)
}

// QueryRange implements outcome.MetricSource for the named queries.
func (i *inference) QueryRange(ctx context.Context, q outcome.MetricQuery) (outcome.MetricResult, error) {
	s, err := i.service(q.TargetRef)
	if err != nil {
		return outcome.MetricResult{}, err
	}
	expr, err := s.expr(q.Query)
	if err != nil {
		return outcome.MetricResult{}, err
	}
	return i.p.rangeQuery(ctx, expr, q)
}

// snapshot is the current serving state of one service.
type snapshot struct {
	TTFTp95   *float64 `json:"ttft_p95_seconds,omitempty"`
	Queue     *float64 `json:"queue_depth,omitempty"`
	Running   *float64 `json:"running,omitempty"`
	KVCache   *float64 `json:"kv_cache_usage,omitempty"`
	GenTokens *float64 `json:"gen_tokens_per_s,omitempty"`
	// The fields below are read when the service configures them.
	TPOTp95      *float64 `json:"tpot_p95_seconds,omitempty"`
	E2Ep95       *float64 `json:"e2e_p95_seconds,omitempty"`
	PromptTokens *float64 `json:"prompt_tokens_per_s,omitempty"`
	CacheHitRate *float64 `json:"cache_hit_rate,omitempty"`
	PrefillQueue *float64 `json:"prefill_queue,omitempty"`
	DecodeQueue  *float64 `json:"decode_queue,omitempty"`
}

func (i *inference) snapshot(ctx context.Context, s InferenceService) (snapshot, error) {
	var snap snapshot
	for _, q := range []struct {
		name string
		dst  **float64
	}{{"ttft_p95", &snap.TTFTp95}, {"queue_depth", &snap.Queue}, {"running", &snap.Running},
		{"kv_cache_usage", &snap.KVCache}, {"gen_tokens_per_s", &snap.GenTokens}} {
		expr, _ := s.expr(q.name)
		v, ok, err := i.p.scalar(ctx, expr)
		if err != nil {
			return snap, err
		}
		if ok {
			*q.dst = &v
		}
	}
	i.snapshotV3(ctx, s, &snap)
	return snap, nil
}

func fmtF(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }

func (i *inference) collect(ctx context.Context, now time.Time) (capability.Observation, error) {
	var o capability.Observation
	var firstErr error
	for _, ref := range i.order {
		s := i.byRef[ref]
		snap, err := i.snapshot(ctx, s)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		t3 := world.TrustTelemetry
		add := func(pred string, v *float64) {
			if v != nil {
				o.Facts = append(o.Facts, world.Fact{SubjectRef: ref, Predicate: pred, Value: fmtF(*v), TrustTier: t3})
			}
		}
		add("ttft_p95_seconds", snap.TTFTp95)
		add("queue_depth", snap.Queue)
		add("kv_cache_usage", snap.KVCache)
		add("gen_tokens_per_s", snap.GenTokens)
		add("tpot_p95_seconds", snap.TPOTp95)
		add("e2e_p95_seconds", snap.E2Ep95)
		add("cache_hit_rate", snap.CacheHitRate)
		add("prefill_queue", snap.PrefillQueue)
		add("decode_queue", snap.DecodeQueue)
		o.Facts = append(o.Facts, world.Fact{SubjectRef: ref, Predicate: "serving_engine", Value: s.Engine, TrustTier: t3})
		if snap.TTFTp95 == nil {
			continue
		}
		state := "ok"
		if *snap.TTFTp95 > s.TTFTSLO {
			state = "breach"
			// One event per service per 10 minutes while breaching; the
			// rule window keeps them in one Work.
			bucket := now.UTC().Truncate(10 * time.Minute).Format("200601021504")
			d := diagnose(s, snap, nil)
			payload, _ := json.Marshal(map[string]any{"snapshot": snap, "slo_seconds": s.TTFTSLO, "diagnosis": d})
			o.Events = append(o.Events, eventing.Envelope{ID: "ttft:" + ref + ":" + bucket, Source: "inference",
				Type: "inference.ttft_breach", SubjectRef: ref, DedupKey: "ttft:" + ref + ":" + bucket, OccurredAt: now,
				ObservedAt: now, Confidence: 0.9, Labels: map[string]string{"engine": s.Engine,
					"ttft_p95": fmtF(*snap.TTFTp95), "slo": fmtF(s.TTFTSLO), "cause": d.Cause}, Payload: payload})
		}
		o.Facts = append(o.Facts, world.Fact{SubjectRef: ref, Predicate: "ttft_slo", Value: state, TrustTier: t3})
	}
	return o, firstErr
}

// Diagnosis is the deterministic first-pass classification of a TTFT
// breach. It is evidence for the investigating agent, not a decision.
type Diagnosis struct {
	Cause     string   `json:"cause"`
	Evidence  []string `json:"evidence"`
	Suggested string   `json:"suggested_action,omitempty"`
}

// diagnose classifies a breach:
//   - gpu_fault: a GPU serving the model reports a critical XID;
//   - kv_cache_pressure: KV cache nearly full and requests queue (preemption
//     and recomputation inflate TTFT) -> scale out;
//   - capacity: requests queue with KV headroom -> scale out;
//   - prefill_bound: no queue, cache fine -> long prompts or a config
//     change (chunked prefill, batch size): look at recent deploys.
func diagnose(s InferenceService, snap snapshot, gpus []*gpuRow) Diagnosis {
	var d Diagnosis
	for _, g := range gpus {
		if criticalXID[int(g.XID)] {
			d.Cause = "gpu_fault"
			d.Evidence = append(d.Evidence, fmt.Sprintf("GPU %s/%s reports XID %d", g.Host, g.Index, int(g.XID)))
			d.Suggested = "node.cordon on node://" + strings.ToLower(g.Host)
			return d
		}
	}
	if d2, ok := diagnosePD(s, snap); ok {
		return d2
	}
	queue := snap.Queue != nil && *snap.Queue >= s.QueueLimit
	kv := snap.KVCache != nil && *snap.KVCache >= 0.9
	if snap.Queue != nil {
		d.Evidence = append(d.Evidence, fmt.Sprintf("queue depth %.0f (limit %.0f)", *snap.Queue, s.QueueLimit))
	}
	if snap.KVCache != nil {
		d.Evidence = append(d.Evidence, fmt.Sprintf("KV cache usage %.0f%%", *snap.KVCache*100))
	}
	switch {
	case queue && kv:
		d.Cause, d.Suggested = "kv_cache_pressure", "deployment.scale (add replicas)"
	case queue:
		d.Cause, d.Suggested = "capacity", "deployment.scale (add replicas)"
	default:
		d.Cause = "prefill_bound"
		d.Suggested = "check recent changes (gitops.history) and prompt lengths; deployment.rollback if a deploy preceded the breach"
	}
	return d
}

func (i *inference) RegisterTools(r capability.ToolRegistry) error {
	return r.AddTool(&tools.Func{Desc: tools.Descriptor{Name: "inference.diagnose", Version: "1", Risk: domain.RiskR0, Trust: "T3",
		Source: "native", TargetArg: "service",
		Description: "Serving state of an inference service (TTFT p95, queue, KV cache, throughput, GPU faults) with a first-pass cause.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string","description":"the configured ref, e.g. deployment://prod/llama"}},` +
			`"required":["service"],"additionalProperties":false}`)},
		Fn: i.toolDiagnose})
}

func (i *inference) toolDiagnose(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct{ Service string }
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrNotDispatched, err)
	}
	ref, err := cpkit.NormalizeRef(a.Service)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrNotDispatched, err)
	}
	s, err := i.service(ref)
	if err != nil {
		return nil, fmt.Errorf("%w: %v (configured: %v)", domain.ErrNotDispatched, err, i.order)
	}
	snap, err := i.snapshot(ctx, s)
	if err != nil {
		return nil, err
	}
	// GPUs of the service's pods (DCGM pod mapping), when available.
	var gpus []*gpuRow
	if ns, name, ok := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(ref, "deployment://"), "service://"), "/"); ok {
		g := &gpuCap{p: i.p, cfg: GPUConfig{MaxGPUs: 256}}
		gpus, _ = g.rows(ctx, `namespace="`+labelValue(ns)+`",pod=~"`+regexp.QuoteMeta(name)+`-.*"`)
	}
	d := diagnose(s, snap, gpus)
	return json.Marshal(map[string]any{"service": ref, "engine": s.Engine, "slo_seconds": s.TTFTSLO, "state": snap,
		"gpus": len(gpus), "diagnosis": d})
}

// WorkTemplates ships the TTFT incident scenario.
func (i *inference) WorkTemplates() []work.Template {
	return append([]work.Template{InferenceLatencyTemplate(), InferenceContinuousTemplate()}, InferenceIncidentTemplates()...)
}

// InferenceContinuousTemplate monitors repeated inference incidents within a lifetime budget.
func InferenceContinuousTemplate() work.Template {
	t := InferenceLatencyTemplate()
	t.ID, t.RepeatFrom = "inference-continuous", "observe"
	t.Limits = work.Limits{MaxRuns: 200, MaxActions: 50, MaxReplans: 20, MaxDuration: cpkit.Duration(30 * 24 * time.Hour)}
	t.Stages = append([]work.Stage{{ID: "observe", Type: work.StageWait, Phase: "detect",
		Wait: &continuous.Spec{Mode: continuous.OnEvent, Purpose: "inference TTFT anomaly",
			Predicate: &eventing.Predicate{Sources: []string{"inference"}, EventTypes: []string{"inference.ttft_breach"}}}}}, t.Stages...)
	return t
}

// InferenceLatencyTemplate is the TTFT scenario: diagnose, mitigate with
// one approved action, verify TTFT against the SLO, review.
func InferenceLatencyTemplate() work.Template {
	return work.Template{ID: "inference-latency", Version: 1, Kind: work.KindIncident, Stages: []work.Stage{
		{ID: "investigate", Type: work.StageRun, Phase: "investigate", Run: &work.RunSpec{Goal: "Time-to-first-token on ${subjects} " +
			"breaches its SLO (\"${title}\"). ${goal.description}\nUse inference.diagnose first, then gpu.status, " +
			"prometheus.query and gitops.history/argocd change events to confirm the cause (gpu_fault, kv_cache_pressure, " +
			"capacity or prefill_bound/regression). Propose exactly one mitigation as " +
			"{\"proposed_action\":{\"name\":...,\"target\":...,\"args\":...}}: deployment.scale (capacity, KV pressure), " +
			"deployment.rollback (a deploy preceded the breach), traffic.shift (canary regression) or node.cordon (GPU fault). " +
			"Do not perform production writes yourself."}},
		{ID: "mitigate", Type: work.StageAction, Phase: "mitigate", OnFailure: "investigate", Action: &work.ActionSpec{
			FromProposal: "investigate", Allowed: []string{"deployment.scale", "deployment.rollback", "traffic.shift", "node.cordon"}}},
		{ID: "verify", Type: work.StageVerify, Phase: "verify", OnFailure: "investigate", OnTimeout: work.OnFailBlock,
			Verify: &work.VerifySpec{RecheckEvery: cpkit.Duration(time.Minute), Timeout: cpkit.Duration(30 * time.Minute)}},
		{ID: "review", Type: work.StageRun, Phase: "review", OnFailure: work.OnFailSkip, Run: &work.RunSpec{Goal: "Write a " +
			"postmortem candidate for the TTFT breach on ${subjects}: timeline, cause class, mitigation and its measured effect, " +
			"capacity follow-ups (replica floor, KV cache sizing, autoscaling on queue depth).\nInvestigation: ${stage.investigate.summary}"}},
	}}
}

// TTFTSuccess is the success spec of a TTFT Work: p95 TTFT under the SLO
// for 10 minutes, queue drained (used by the demo and as the API example).
func TTFTSuccess(ref string, slo float64) outcome.Spec {
	return outcome.Spec{Version: 1, StableFor: cpkit.Duration(10 * time.Minute), AllOf: []outcome.Objective{
		{ID: "ttft", Kind: outcome.KindMetric, TargetRef: ref, Window: cpkit.Duration(10 * time.Minute), MinSamples: 3,
			Metric: &outcome.MetricExpr{Source: "inference", Query: "ttft_p95", Aggregate: outcome.AggMax, Operator: "<", Threshold: slo}},
		{ID: "queue", Kind: outcome.KindMetric, TargetRef: ref, Window: cpkit.Duration(10 * time.Minute), Optional: true,
			Metric: &outcome.MetricExpr{Source: "inference", Query: "queue_depth", Aggregate: outcome.AggAvg, Operator: "<", Threshold: 4}},
	}}
}

// ================= Training jobs =================

type progress struct {
	value float64
	since time.Time
}

type training struct {
	capability.Base
	cfg   TrainingConfig
	k     *kube
	p     promAPI
	group string

	mu       sync.Mutex
	progress map[string]progress
}

func (t *training) ID() string      { return "training" }
func (t *training) Version() string { return "1" }

func (t *training) RegisterWorld(r capability.WorldRegistry) error {
	if t.cfg.CollectEvery <= 0 {
		return nil
	}
	return r.AddCollector("jobs", t.cfg.CollectEvery.D(), capability.CollectorFunc(t.collect))
}

func (t *training) RegisterEvents(r capability.EventRegistry) error {
	return r.AddRule(eventing.Rule{Name: "training-jobs", Match: eventing.Predicate{Sources: []string{"training"},
		EventTypes: []string{"training.failed", "training.stalled"}}, Key: "subject", WorkKind: "incident", Template: "incident",
		Window: cpkit.Duration(6 * time.Hour), Title: "${type} ${subject}", Intent: "restore training progress"})
}

type trainingJob struct {
	Metadata objMeta `json:"metadata"`
	Spec     struct {
		RunPolicy struct {
			Suspend *bool `json:"suspend"`
		} `json:"runPolicy"`
	} `json:"spec"`
	Status struct {
		Conditions []struct {
			Type               string `json:"type"`
			Status             string `json:"status"`
			Reason             string `json:"reason"`
			Message            string `json:"message"`
			LastTransitionTime string `json:"lastTransitionTime"`
		} `json:"conditions"`
		ReplicaStatuses map[string]struct {
			Active    int `json:"active"`
			Succeeded int `json:"succeeded"`
			Failed    int `json:"failed"`
		} `json:"replicaStatuses"`
		StartTime string `json:"startTime"`
	} `json:"status"`
}

// phase is the latest true condition (Created, Running, Restarting,
// Succeeded, Failed, Suspended).
func (j trainingJob) phase() (string, string) {
	phase, msg, at := "Unknown", "", ""
	for _, c := range j.Status.Conditions {
		if c.Status == "True" && c.LastTransitionTime >= at {
			phase, msg, at = c.Type, c.Message, c.LastTransitionTime
		}
	}
	return phase, msg
}

func trainingPath(kind, ns, name string) string {
	p := "/apis/kubeflow.org/v1/namespaces/" + ns + "/" + kind
	if name != "" {
		p += "/" + name
	}
	return p
}

func (t *training) jobs(ctx context.Context) (map[string][]trainingJob, error) {
	out := map[string][]trainingJob{}
	for _, kind := range t.cfg.Kinds {
		for _, ns := range t.k.namespaces() {
			js, err := listAll[trainingJob](ctx, t.k, trainingPath(kind, ns, ""), nil, 500)
			if capability.IsStatus(err, http.StatusNotFound) {
				continue // CRD not installed
			}
			if err != nil {
				return nil, err
			}
			out[kind] = append(out[kind], js...)
		}
	}
	return out, nil
}

func (t *training) collect(ctx context.Context, now time.Time) (capability.Observation, error) {
	all, err := t.jobs(ctx)
	if err != nil {
		return capability.Observation{}, err
	}
	var o capability.Observation
	t2 := world.TrustFirstParty
	seen := map[string]bool{}
	for kind, js := range all {
		for _, j := range js {
			ns, name := j.Metadata.Namespace, j.Metadata.Name
			ref := "trainingjob://" + ns + "/" + name
			phase, msg := j.phase()
			failed := 0
			for _, rs := range j.Status.ReplicaStatuses {
				failed += rs.Failed
			}
			o.Facts = append(o.Facts,
				world.Fact{SubjectRef: ref, Predicate: "phase", Value: phase, TrustTier: t2},
				world.Fact{SubjectRef: ref, Predicate: "kind", Value: kind, TrustTier: t2},
				world.Fact{SubjectRef: ref, Predicate: "failed_replicas", Value: strconv.Itoa(failed), TrustTier: t2})
			lbl := map[string]string{"kind": kind, "phase": phase}
			if phase == "Failed" {
				payload, _ := json.Marshal(map[string]any{"message": cpkit.Truncate(msg, 1000), "failed_replicas": failed})
				o.Events = append(o.Events, eventing.Envelope{ID: "training:failed:" + j.Metadata.UID, Source: "training",
					Type: "training.failed", SubjectRef: ref, DedupKey: "training:failed:" + j.Metadata.UID, OccurredAt: now,
					ObservedAt: now, Confidence: 1, Labels: lbl, Payload: payload})
			}
			if phase != "Running" || t.cfg.ProgressQuery == "" {
				continue
			}
			expr, err := expand(t.cfg.ProgressQuery, map[string]string{"namespace": labelValue(ns), "name": labelValue(name)})
			if err != nil {
				return o, err
			}
			v, ok, err := t.p.scalar(ctx, expr)
			if err != nil || !ok {
				continue
			}
			o.Facts = append(o.Facts, world.Fact{SubjectRef: ref, Predicate: "step", Value: strconv.FormatFloat(v, 'f', 0, 64),
				TrustTier: world.TrustTelemetry})
			key := ref
			seen[key] = true
			t.mu.Lock()
			pr, had := t.progress[key]
			if !had || v > pr.value {
				t.progress[key] = progress{value: v, since: now}
			} else if now.Sub(pr.since) >= t.cfg.StallAfter.D() {
				bucket := pr.since.UTC().Format("200601021504")
				payload, _ := json.Marshal(map[string]any{"step": v, "stalled_since": pr.since})
				o.Events = append(o.Events, eventing.Envelope{ID: "training:stalled:" + j.Metadata.UID + ":" + bucket,
					Source: "training", Type: "training.stalled", SubjectRef: ref,
					DedupKey: "training:stalled:" + j.Metadata.UID + ":" + bucket, OccurredAt: now, ObservedAt: now,
					Confidence: 0.8, Labels: lbl, Payload: payload})
			}
			t.mu.Unlock()
		}
	}
	t.mu.Lock()
	for k := range t.progress {
		if !seen[k] {
			delete(t.progress, k)
		}
	}
	t.mu.Unlock()
	return o, nil
}

func (t *training) RegisterTools(r capability.ToolRegistry) error {
	return r.AddTool(&tools.Func{Desc: tools.Descriptor{Name: "training.status", Version: "1", Risk: domain.RiskR0, Trust: "T2",
		Source: "native", TargetArg: "namespace,name",
		Description: "Training jobs (Kubeflow) with phase, replica status and last condition message; optionally one job.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"namespace":{"type":"string"},"name":{"type":"string"}},"additionalProperties":false}`)},
		Fn: t.toolStatus})
}

func (t *training) toolStatus(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct{ Namespace, Name string }
	_ = json.Unmarshal(args, &a)
	all, err := t.jobs(ctx)
	if err != nil {
		return nil, err
	}
	type job struct {
		Kind, Namespace, Name, Phase, Message string
		Replicas                              any
		Suspended                             bool
	}
	out := []job{}
	for kind, js := range all {
		for _, j := range js {
			if (a.Namespace != "" && j.Metadata.Namespace != a.Namespace) || (a.Name != "" && j.Metadata.Name != a.Name) {
				continue
			}
			phase, msg := j.phase()
			out = append(out, job{kind, j.Metadata.Namespace, j.Metadata.Name, phase, cpkit.Truncate(msg, 500),
				j.Status.ReplicaStatuses, j.Spec.RunPolicy.Suspend != nil && *j.Spec.RunPolicy.Suspend})
			if len(out) >= 100 {
				break
			}
		}
	}
	sort.Slice(out, func(x, y int) bool { return out[x].Namespace+out[x].Name < out[y].Namespace+out[y].Name })
	return json.Marshal(map[string]any{"jobs": out})
}

func (t *training) RegisterActions(r capability.ActionRegistry) error {
	return r.AddAction(&trainingSuspend{t})
}

// trainingSuspend suspends (or resumes) a training job through the
// operator's runPolicy.suspend: pods are deleted, the job is kept and can
// resume from its last checkpoint.
type trainingSuspend struct{ t *training }

func (a *trainingSuspend) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "training.suspend", Version: "1", ResourceKinds: []string{"trainingjob"}, Risk: domain.RiskR2,
		Description: "Suspend (suspend=true) or resume (false) a Kubeflow training job; frees its GPUs, keeps the job.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"suspend":{"type":"boolean"},"kind":{"type":"string"}},` +
			`"required":["suspend"],"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, SupportsRollback: true, BlastRadius: "job", Timeout: time.Minute}
}

type suspendArgs struct {
	Suspend bool   `json:"suspend"`
	Kind    string `json:"kind"`
}

func (a *trainingSuspend) resolve(ctx context.Context, p action.Plan) (string, string, string, trainingJob, suspendArgs, error) {
	var args suspendArgs
	ns, name, err := a.t.k.nsName(p.TargetRef, "trainingjob")
	if err != nil {
		return "", "", "", trainingJob{}, args, capability.NotApplied("%v", err)
	}
	if err := parseArgs(p, &args); err != nil {
		return "", "", "", trainingJob{}, args, err
	}
	kind := args.Kind
	if kind == "" {
		kind = a.t.cfg.Kinds[0]
	}
	allowed := false
	for _, k := range a.t.cfg.Kinds {
		allowed = allowed || k == kind
	}
	if !allowed {
		return "", "", "", trainingJob{}, args, capability.NotApplied("kind %q is not configured", kind)
	}
	var j trainingJob
	if err := a.t.k.c.Get(ctx, trainingPath(kind, ns, name), nil, &j); err != nil {
		return ns, name, kind, j, args, capability.BeforeWrite(err)
	}
	return ns, name, kind, j, args, nil
}

func (a *trainingSuspend) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	_, _, _, j, args, err := a.resolve(ctx, p)
	if err != nil {
		return refused(err.Error()), nil
	}
	cur := j.Spec.RunPolicy.Suspend != nil && *j.Spec.RunPolicy.Suspend
	if cur == args.Suspend {
		return refused(fmt.Sprintf("job is already suspend=%v", cur)), nil
	}
	if phase, _ := j.phase(); phase == "Succeeded" || phase == "Failed" {
		return refused("job is " + phase), nil
	}
	return action.PreflightResult{Allowed: true, TargetVersion: strconv.FormatInt(j.Metadata.Generation, 10),
		EvidenceRefs: []string{p.TargetRef + "@" + j.Metadata.ResourceVersion}}, nil
}

func (a *trainingSuspend) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	ns, name, kind, j, args, err := a.resolve(ctx, p)
	if err != nil {
		return nil, capability.BeforeWrite(err)
	}
	if annotated(j.Metadata, key) {
		return json.Marshal(map[string]any{"already_applied": true})
	}
	if p.TargetVersion != "" && p.TargetVersion != strconv.FormatInt(j.Metadata.Generation, 10) {
		return nil, capability.NotApplied("job changed: generation %d, plan pinned %s", j.Metadata.Generation, p.TargetVersion)
	}
	body := map[string]any{"metadata": map[string]any{"annotations": map[string]string{effectKeyAnno: key},
		"resourceVersion": j.Metadata.ResourceVersion}, "spec": map[string]any{"runPolicy": map[string]any{"suspend": args.Suspend}}}
	if err := a.t.k.patch(ctx, trainingPath(kind, ns, name), "application/merge-patch+json", body, nil); err != nil {
		return nil, capability.ActionError(err)
	}
	return json.Marshal(map[string]any{"job": ns + "/" + name, "kind": kind, "suspend": args.Suspend, "dry_run": a.t.k.dryRun})
}

func (a *trainingSuspend) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	_, _, _, j, _, err := a.resolve(ctx, p)
	if err != nil {
		return action.InspectResult{}, err
	}
	ok := annotated(j.Metadata, p.EffectKey)
	return action.InspectResult{Applied: &ok}, nil
}
