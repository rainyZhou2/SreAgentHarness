package sre

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

// TrafficConfig configures traffic management on Istio VirtualServices,
// through the same API server client (and namespace allowlist and dry-run
// default) as the Kubernetes capability.
type TrafficConfig struct {
	// MaxStep bounds the weight moved onto any one destination per action
	// (default 100: a full shift is allowed, but stays R2 + approval).
	MaxStep int `json:"max_step,omitempty"`
}

type traffic struct {
	capability.Base
	cfg TrafficConfig
	k   *kube
}

func newTraffic(cfg TrafficConfig, k *kube) *traffic {
	if cfg.MaxStep <= 0 || cfg.MaxStep > 100 {
		cfg.MaxStep = 100
	}
	return &traffic{cfg: cfg, k: k}
}

func (t *traffic) ID() string      { return "traffic" }
func (t *traffic) Version() string { return "1" }

func vsPath(ns, name string) string {
	return "/apis/networking.istio.io/v1beta1/namespaces/" + ns + "/virtualservices/" + name
}

// virtualService keeps the raw object so updates preserve fields we do not
// model.
type virtualService struct {
	raw  map[string]any
	meta objMeta
}

func (t *traffic) get(ctx context.Context, ns, name string) (virtualService, error) {
	var raw map[string]any
	if err := t.k.c.Get(ctx, vsPath(ns, name), nil, &raw); err != nil {
		return virtualService{}, err
	}
	var vs virtualService
	vs.raw = raw
	b, _ := json.Marshal(raw["metadata"])
	_ = json.Unmarshal(b, &vs.meta)
	return vs, nil
}

// destination is one weighted route destination.
type destination struct {
	Host   string `json:"host"`
	Subset string `json:"subset,omitempty"`
	Weight int    `json:"weight"`
}

func (d destination) key() string {
	if d.Subset != "" {
		return d.Subset
	}
	return d.Host
}

// httpRoutes returns the http routes and, for each, its destinations.
func (vs virtualService) httpRoutes() ([]map[string]any, [][]destination) {
	spec, _ := vs.raw["spec"].(map[string]any)
	list, _ := spec["http"].([]any)
	var routes []map[string]any
	var dests [][]destination
	for _, r := range list {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		var ds []destination
		rs, _ := m["route"].([]any)
		for _, x := range rs {
			rm, _ := x.(map[string]any)
			dm, _ := rm["destination"].(map[string]any)
			d := destination{}
			d.Host, _ = dm["host"].(string)
			d.Subset, _ = dm["subset"].(string)
			if w, ok := rm["weight"].(float64); ok {
				d.Weight = int(w)
			} else if len(rs) == 1 {
				d.Weight = 100
			}
			ds = append(ds, d)
		}
		routes = append(routes, m)
		dests = append(dests, ds)
	}
	return routes, dests
}

func (t *traffic) RegisterTools(r capability.ToolRegistry) error {
	return r.AddTool(&tools.Func{Desc: tools.Descriptor{Name: "traffic.routes", Version: "1", Risk: domain.RiskR0, Trust: "T2",
		Source: "native", TargetArg: "namespace,name",
		Description: "HTTP routes of an Istio VirtualService with destination weights.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"namespace":{"type":"string"},"name":{"type":"string"}},` +
			`"required":["namespace","name"],"additionalProperties":false}`)},
		Fn: t.toolRoutes})
}

func (t *traffic) toolRoutes(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct{ Namespace, Name string }
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrNotDispatched, err)
	}
	ns, name, err := t.k.nsName("virtualservice://"+a.Namespace+"/"+a.Name, "virtualservice")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrNotDispatched, err)
	}
	vs, err := t.get(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	routes, dests := vs.httpRoutes()
	type route struct {
		Name         string        `json:"name,omitempty"`
		Destinations []destination `json:"destinations"`
	}
	out := []route{}
	for i, r := range routes {
		n, _ := r["name"].(string)
		out = append(out, route{n, dests[i]})
	}
	return json.Marshal(map[string]any{"virtualservice": ns + "/" + name, "generation": vs.meta.Generation, "routes": out})
}

func (t *traffic) RegisterActions(r capability.ActionRegistry) error {
	return r.AddAction(&trafficShift{t})
}

// ---- traffic.shift ----

type trafficShift struct{ t *traffic }

func (a *trafficShift) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "traffic.shift", Version: "1", ResourceKinds: []string{"virtualservice"}, Risk: domain.RiskR2,
		Description: "Set destination weights (summing to 100) on one HTTP route of an Istio VirtualService.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"route":{"type":"string"},` +
			`"weights":{"type":"object","additionalProperties":{"type":"integer","minimum":0,"maximum":100}}},` +
			`"required":["weights"],"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, SupportsRollback: true, BlastRadius: "service",
		Timeout: time.Minute}
}

type shiftArgs struct {
	Route   string         `json:"route"`
	Weights map[string]int `json:"weights"`
}

// plan resolves the route and checks the new weights against it.
func (a *trafficShift) plan(ctx context.Context, p action.Plan) (string, string, virtualService, int, shiftArgs, error) {
	var args shiftArgs
	ns, name, err := a.t.k.nsName(p.TargetRef, "virtualservice")
	if err != nil {
		return "", "", virtualService{}, 0, args, capability.NotApplied("%v", err)
	}
	if err := parseArgs(p, &args); err != nil {
		return ns, name, virtualService{}, 0, args, err
	}
	vs, err := a.t.get(ctx, ns, name)
	if err != nil {
		return ns, name, vs, 0, args, capability.BeforeWrite(err)
	}
	if p.TargetVersion != "" && p.TargetVersion != strconv.FormatInt(vs.meta.Generation, 10) {
		return ns, name, vs, 0, args, capability.NotApplied("virtualservice changed: generation %d, plan pinned %s",
			vs.meta.Generation, p.TargetVersion)
	}
	routes, dests := vs.httpRoutes()
	idx := -1
	for i, r := range routes {
		if n, _ := r["name"].(string); n == args.Route || (args.Route == "" && len(routes) == 1) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ns, name, vs, 0, args, capability.NotApplied("route %q not found (or ambiguous: %d routes)", args.Route, len(routes))
	}
	sum := 0
	for _, w := range args.Weights {
		if w < 0 || w > 100 {
			return ns, name, vs, idx, args, capability.NotApplied("weights must be 0-100")
		}
		sum += w
	}
	if sum != 100 {
		return ns, name, vs, idx, args, capability.NotApplied("weights sum to %d, not 100", sum)
	}
	cur := map[string]int{}
	for _, d := range dests[idx] {
		cur[d.key()] = d.Weight
	}
	if len(cur) != len(args.Weights) {
		return ns, name, vs, idx, args, capability.NotApplied("weights must name every destination of the route: %v", keys(cur))
	}
	for k, w := range args.Weights {
		old, ok := cur[k]
		if !ok {
			return ns, name, vs, idx, args, capability.NotApplied("route has no destination %q (have %v)", k, keys(cur))
		}
		if w-old > a.t.cfg.MaxStep {
			return ns, name, vs, idx, args, capability.NotApplied("moving %d%% onto %s exceeds max_step %d", w-old, k, a.t.cfg.MaxStep)
		}
	}
	return ns, name, vs, idx, args, nil
}

func keys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (a *trafficShift) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	_, _, vs, _, _, err := a.plan(ctx, p)
	if err != nil {
		return refused(err.Error()), nil
	}
	return action.PreflightResult{Allowed: true, TargetVersion: strconv.FormatInt(vs.meta.Generation, 10),
		EvidenceRefs: []string{p.TargetRef + "@" + vs.meta.ResourceVersion}}, nil
}

func (a *trafficShift) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	ns, name, vs, idx, args, err := a.plan(ctx, p)
	if err != nil {
		return nil, capability.BeforeWrite(err)
	}
	if annotated(vs.meta, key) {
		return json.Marshal(map[string]any{"already_applied": true})
	}
	routes, _ := vs.httpRoutes()
	rs, _ := routes[idx]["route"].([]any)
	for _, x := range rs {
		rm, _ := x.(map[string]any)
		dm, _ := rm["destination"].(map[string]any)
		d := destination{}
		d.Host, _ = dm["host"].(string)
		d.Subset, _ = dm["subset"].(string)
		rm["weight"] = args.Weights[d.key()]
	}
	md, _ := vs.raw["metadata"].(map[string]any)
	ann, _ := md["annotations"].(map[string]any)
	if ann == nil {
		ann = map[string]any{}
		md["annotations"] = ann
	}
	ann[effectKeyAnno] = key
	// PUT with the resourceVersion read above: a concurrent change fails
	// with 409 (refused, nothing applied) instead of being overwritten.
	err = a.t.k.c.Do(ctx, capability.Request{Method: http.MethodPut, Path: vsPath(ns, name), Query: a.t.k.writeQuery(),
		Body: vs.raw}, nil)
	if err != nil {
		return nil, capability.ActionError(err)
	}
	return json.Marshal(map[string]any{"virtualservice": ns + "/" + name, "weights": args.Weights, "dry_run": a.t.k.dryRun})
}

func (a *trafficShift) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	ns, name, err := a.t.k.nsName(p.TargetRef, "virtualservice")
	if err != nil {
		return action.InspectResult{}, err
	}
	vs, err := a.t.get(ctx, ns, name)
	if err != nil {
		return action.InspectResult{}, err
	}
	ok := annotated(vs.meta, p.EffectKey)
	return action.InspectResult{Applied: &ok}, nil
}
