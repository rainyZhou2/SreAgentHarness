package sre

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/world"
)

// PR-17 / F16: a GPU is followed by its UUID. The slot is an alias of the
// card in it; replacing the card or rebuilding its MIG layout is seen and
// recorded as a topology change, not mistaken for the same device.
func TestGPUIdentityFollowsUUIDAndMIG(t *testing.T) {
	var mu sync.Mutex
	uuid, mig := "GPU-aaaa1111", map[string]string{"1": "1g.10gb", "2": "1g.10gb"}
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		q := r.URL.Query().Get("query")
		var result []map[string]any
		lbl := func(gi string) map[string]string {
			l := map[string]string{"Hostname": "gpu-node-1", "gpu": "0", "UUID": uuid, "modelName": "H200"}
			if gi != "" {
				l["GPU_I_ID"], l["GPU_I_PROFILE"] = gi, mig[gi]
			}
			return l
		}
		switch {
		case strings.Contains(q, "DCGM_FI_DEV_GPU_UTIL"):
			for gi := range mig {
				result = append(result, vec("40", lbl(gi)))
			}
			if len(mig) == 0 {
				result = append(result, vec("40", lbl("")))
			}
		case strings.Contains(q, "DCGM_FI_DEV_ECC_DBE_VOL_TOTAL"):
			result = append(result, vec("0", lbl("")))
		}
		if result == nil {
			result = []map[string]any{}
		}
		writeJSON(w, map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}})
	})
	set, err := Build(Config{Tenant: "acme", AIInfra: &AIInfraConfig{Prometheus: capability.HTTPConfig{URL: srv.URL}, GPU: &GPUConfig{}}},
		capability.Options{})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	env := rec.env()
	var entities []world.Entity
	env.UpsertEntity = func(_ context.Context, e world.Entity) error { entities = append(entities, e); return nil }
	collect := func() {
		t.Helper()
		rec.mu.Lock()
		rec.events = nil
		rec.mu.Unlock()
		entities = nil
		if err := set.CollectOnce(ctx, env, "gpu", "dcgm"); err != nil {
			t.Fatal(err)
		}
	}
	collect()
	const slot, canon = "gpu://gpu-node-1/0", "gpu://gpu-node-1/gpu-aaaa1111"
	var alias, card *world.Entity
	migs := 0
	for i := range entities {
		switch e := &entities[i]; {
		case e.Ref == slot:
			alias = e
		case e.Ref == canon:
			card = e
		case strings.HasPrefix(e.Ref, "mig://gpu-node-1/gpu-aaaa1111/"):
			migs++
		}
	}
	if alias == nil || card == nil || alias.Attributes[action.AttrIdentity] != canon || card.Attributes[action.AttrUID] != "GPU-aaaa1111" || migs != 2 {
		t.Fatalf("entities: alias=%+v card=%+v migs=%d", alias, card, migs)
	}
	if !rec.hasRelation("node://gpu-node-1", "has_gpu", canon) || !rec.hasRelation(canon, "has_mig", "mig://gpu-node-1/gpu-aaaa1111/1") {
		t.Fatalf("relations: %+v", rec.relations)
	}
	if f, ok := rec.fact(slot, "gpu_uuid"); !ok || f.Value != "GPU-aaaa1111" {
		t.Fatalf("slot uuid fact: %+v", f)
	}
	if _, ok := rec.event("topology.gpu_replaced"); ok {
		t.Fatal("the first observation is not a replacement")
	}

	mu.Lock()
	mig = map[string]string{"1": "2g.20gb"} // the layout was rebuilt
	mu.Unlock()
	collect()
	if e, ok := rec.event("topology.gpu_mig_changed"); !ok || e.SubjectRef != "node://gpu-node-1" {
		t.Fatalf("a rebuilt MIG layout must be recorded: %+v", e)
	}

	mu.Lock()
	uuid, mig = "GPU-bbbb2222", map[string]string{} // the card was swapped
	mu.Unlock()
	collect()
	e, ok := rec.event("topology.gpu_replaced")
	if !ok || e.Labels["before"] != "GPU-aaaa1111" || e.Labels["after"] != "GPU-bbbb2222" {
		t.Fatalf("a swapped card must be recorded: %+v", e)
	}
	for i := range entities {
		if entities[i].Ref == slot && entities[i].Attributes[action.AttrIdentity] != "gpu://gpu-node-1/gpu-bbbb2222" {
			t.Fatalf("the slot must now point at the new card: %+v", entities[i])
		}
	}
	// Neither change is a fault: no incident event is raised for it.
	if _, ok := rec.event("gpu.xid_error"); ok {
		t.Fatal("a topology change raised a fault event")
	}
}
