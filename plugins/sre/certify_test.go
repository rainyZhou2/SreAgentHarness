package sre

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/action/actiontest"
	"go-sre-harness/internal/capability"
)

// faulty wraps a fake downstream with the failure modes of the contract
// suite: writes refused, a write applied whose response is lost, and a
// downstream that cannot be reached at all.
type faulty struct {
	next  http.HandlerFunc
	fault atomic.Int32
}

func (f *faulty) inject(x actiontest.Fault) { f.fault.Store(int32(x)) }

func (f *faulty) handler(w http.ResponseWriter, r *http.Request) {
	write := r.Method != http.MethodGet && r.Method != http.MethodHead
	switch actiontest.Fault(f.fault.Load()) {
	case actiontest.Unreachable:
		http.Error(w, `{"message":"upstream unavailable"}`, http.StatusServiceUnavailable)
		return
	case actiontest.Refuse:
		if write {
			http.Error(w, `{"message":"forbidden"}`, http.StatusForbidden)
			return
		}
	case actiontest.LoseResponse:
		if write {
			f.next(httptest.NewRecorder(), r) // the effect happens
			http.Error(w, `{"message":"gateway timeout"}`, http.StatusGatewayTimeout)
			return
		}
	}
	f.next(w, r)
}

// PR-08: the Kubernetes rollback adapter passes the executor contract.
func TestCertifyDeploymentRollback(t *testing.T) {
	fk := &fakeKube{}
	reset := func() {
		fk.mu.Lock()
		fk.image, fk.gen, fk.anno, fk.patches, fk.queries = "checkout:v2", 7, map[string]string{revisionAnno: "2"}, nil, nil
		fk.mu.Unlock()
	}
	reset()
	fl := &faulty{next: fk.handler}
	srv := serve(t, fl.handler)
	off := false
	set, err := Build(Config{Tenant: "acme", Kubernetes: &KubernetesConfig{HTTPConfig: capability.HTTPConfig{URL: srv.URL},
		Namespaces: []string{"prod"}, DryRun: &off}}, capability.Options{})
	if err != nil {
		t.Fatal(err)
	}
	reg := action.NewRegistry()
	var rb action.Executor
	for _, a := range set.Actions() {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
		if a.Descriptor().Name == "deployment.rollback" {
			rb = a
		}
	}
	n := 0
	rep := actiontest.Certify(t, actiontest.Case{Executor: rb, Registry: reg, Inject: fl.inject, Reset: reset,
		Plan: func() action.Plan {
			n++
			fk.mu.Lock()
			defer fk.mu.Unlock()
			p := plan("deployment.rollback", "deployment://prod/checkout", map[string]any{})
			p.TargetVersion, p.EffectKey = strconv.FormatInt(fk.gen, 10), fmt.Sprintf("effect-%d", n)
			return p
		},
		State: func() string {
			fk.mu.Lock()
			defer fk.mu.Unlock()
			return fmt.Sprintf("%s@%d patches=%d", fk.image, fk.gen, len(fk.patches))
		},
		Stale: func() {
			fk.mu.Lock()
			fk.gen++ // someone else changed the deployment
			fk.mu.Unlock()
		}})
	if len(rep.Checked) < 6 {
		t.Fatalf("contract checks ran: %v (skipped %v)", rep.Checked, rep.Skipped)
	}
}
