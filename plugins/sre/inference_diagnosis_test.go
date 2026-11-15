package sre

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/action/actiontest"
	"go-sre-harness/internal/capability"
)

func f64(v float64) *float64 { return &v }

// §14.4: TTFT is governed by prefill, TPOT by decode. The diagnosis tells
// the two apart and names the pool to scale.
func TestDiagnosePrefillVersusDecode(t *testing.T) {
	s := InferenceService{Ref: "deployment://prod/llama", QueueLimit: 16, TTFTSLO: 0.5, TPOTSLO: 0.05,
		Prefill: &ServingPool{Ref: "deployment://prod/llama-prefill"}, Decode: &ServingPool{Ref: "deployment://prod/llama-decode"}}
	d := diagnose(s, snapshot{TTFTp95: f64(1.4), TPOTp95: f64(0.03), PrefillQueue: f64(40), DecodeQueue: f64(1)}, nil)
	if d.Cause != "prefill_saturation" || !strings.Contains(d.Suggested, "serving.scale_prefill") {
		t.Fatalf("prefill backlog: %+v", d)
	}
	d = diagnose(s, snapshot{TTFTp95: f64(1.4), TPOTp95: f64(0.12), PrefillQueue: f64(40), DecodeQueue: f64(30)}, nil)
	if d.Cause != "decode_backpressure" || !strings.Contains(d.Suggested, "serving.scale_decode") {
		t.Fatalf("a TPOT breach is a decode problem even with a prefill queue: %+v", d)
	}
	// Without PD signals the single-pool classification still applies.
	d = diagnose(InferenceService{QueueLimit: 16}, snapshot{Queue: f64(30), KVCache: f64(0.95)}, nil)
	if d.Cause != "kv_cache_pressure" {
		t.Fatalf("fallback: %+v", d)
	}
	// A GPU fault outranks every capacity reading.
	d = diagnose(s, snapshot{PrefillQueue: f64(40)}, []*gpuRow{{Host: "n1", Index: "0", XID: 79}})
	if d.Cause != "gpu_fault" {
		t.Fatalf("gpu fault: %+v", d)
	}
	spec := InferenceSuccess(InferenceService{Ref: "deployment://prod/llama", TTFTSLO: 0.5, TPOTSLO: 0.05,
		Cost: &CostModel{GPUHourUSD: 4, MaxHourlyUSD: 64}})
	if err := spec.Normalize(); err != nil || len(spec.AllOf) != 3 {
		t.Fatalf("success spec must cover latency and cost: %d %v", len(spec.AllOf), err)
	}
}

// PR-19: pool scaling is bounded, needs idle GPUs and stays inside the
// cost budget; it passes the executor contract.
func TestServingScalePoolGuardsAndContract(t *testing.T) {
	var mu sync.Mutex
	freeGPUs, usedGPUs, queue := 8.0, 6.0, 0.0
	prom := serve(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		q := r.URL.Query().Get("query")
		v := -1.0
		switch {
		case strings.Contains(q, `pod=""`):
			v = freeGPUs
		case strings.HasPrefix(q, "count(DCGM_FI_DEV_GPU_UTIL"):
			v = usedGPUs
		case strings.Contains(q, `role="prefill"`):
			v = queue
		}
		result := []map[string]any{}
		if v >= 0 {
			result = append(result, vec(fmt.Sprint(v), map[string]string{}))
		}
		writeJSON(w, map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}})
	})
	fk := &fakeKube{}
	reset := func() {
		fk.mu.Lock()
		fk.image, fk.gen, fk.anno, fk.patches, fk.queries = "checkout:v2", 7, map[string]string{revisionAnno: "2"}, nil, nil
		fk.mu.Unlock()
		mu.Lock()
		freeGPUs, usedGPUs, queue = 8, 6, 0
		mu.Unlock()
	}
	reset()
	fl := &faulty{next: fk.handler}
	ksrv := serve(t, fl.handler)
	off := false
	set, err := Build(Config{Tenant: "acme",
		Kubernetes: &KubernetesConfig{HTTPConfig: capability.HTTPConfig{URL: ksrv.URL}, Namespaces: []string{"prod"}, DryRun: &off},
		AIInfra: &AIInfraConfig{Prometheus: capability.HTTPConfig{URL: prom.URL}, Inference: []InferenceService{{
			Ref: "deployment://prod/llama", Engine: "sglang", Selector: `namespace="prod"`, TTFTSLO: 0.5, GPUsPerReplica: 2,
			Cost:    &CostModel{GPUHourUSD: 4, MaxHourlyUSD: 48},
			Prefill: &ServingPool{Ref: "deployment://prod/checkout", Selector: `namespace="prod",role="prefill"`, MinReplicas: 2, MaxReplicas: 8},
		}}}}, capability.Options{})
	if err != nil {
		t.Fatal(err)
	}
	reg := action.NewRegistry()
	var scale action.Executor
	for _, a := range set.Actions() {
		_ = reg.Register(a)
		if a.Descriptor().Name == "serving.scale_prefill" {
			scale = a
		}
	}
	if scale == nil {
		t.Fatal("serving.scale_prefill is not registered")
	}
	for _, a := range set.Actions() {
		if a.Descriptor().Name == "serving.scale_decode" {
			t.Fatal("serving.scale_decode must not exist without a decode pool")
		}
	}
	mk := func(replicas int, key string) action.Plan {
		p := plan("serving.scale_prefill", "deployment://prod/llama", map[string]int{"replicas": replicas})
		p.EffectKey = key
		return p
	}
	refused := func(name string, p action.Plan, want string) {
		t.Helper()
		pf, err := scale.Preflight(ctx, p)
		if err != nil || pf.Allowed || !strings.Contains(strings.Join(pf.Reasons, " "), want) {
			t.Errorf("%s: allowed=%v reasons=%v err=%v", name, pf.Allowed, pf.Reasons, err)
		}
	}
	// The pool runs 3 replicas (the fake deployment), 2 GPUs each.
	refused("above the pool maximum", mk(9, "a"), "within [2, 8]")
	refused("below the pool minimum", mk(1, "b"), "within [2, 8]")
	refused("more than one step", mk(6, "c"), "exceeds 2 replicas per action")
	refused("another workload", plan("serving.scale_prefill", "deployment://prod/checkout", map[string]int{"replicas": 4}), "not a configured inference service")
	mu.Lock()
	freeGPUs = 1
	mu.Unlock()
	refused("no idle GPUs", mk(4, "d"), "idle GPUs")
	mu.Lock()
	freeGPUs, usedGPUs = 8, 10 // (10 + 4) GPUs x $4 = $56 > $48
	mu.Unlock()
	refused("over the cost budget", mk(5, "e"), "hourly cost would be")
	mu.Lock()
	usedGPUs, queue = 6, 20
	mu.Unlock()
	refused("scale-in while backed up", mk(2, "f"), "queued requests")
	if len(fk.patches) != 0 {
		t.Fatalf("a refused scaling patched the deployment %d time(s)", len(fk.patches))
	}

	n := 0
	rep := actiontest.Certify(t, actiontest.Case{Executor: scale, Registry: reg, Inject: fl.inject, Reset: reset,
		Plan: func() action.Plan { n++; return mk(4, fmt.Sprintf("scale-%d", n)) },
		State: func() string {
			fk.mu.Lock()
			defer fk.mu.Unlock()
			return fmt.Sprintf("gen=%d patches=%d", fk.gen, len(fk.patches))
		},
		ViolateGuard: func() {
			mu.Lock()
			freeGPUs = 0
			mu.Unlock()
		}})
	if len(rep.Checked) < 6 {
		t.Fatalf("contract checks: %v skipped: %v", rep.Checked, rep.Skipped)
	}
}
