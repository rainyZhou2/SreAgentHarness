package sretools

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

func TestPrometheusQueryBoundsOutput(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.URL.Query().Get("query") == "bad(" {
			w.WriteHeader(400)
			_, _ = io.WriteString(w, `{"status":"error","errorType":"bad_data","error":"parse error"}`)
			return
		}
		var res []string
		for i := 0; i < 30; i++ {
			res = append(res, `{"metric":{"pod":"p`+string(rune('a'+i%26))+`"},"value":[1,"0.5"]}`)
		}
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[`+strings.Join(res, ",")+`]}}`)
	}))
	defer srv.Close()
	p, err := NewPrometheus(PrometheusConfig{URL: srv.URL, MaxSeries: 5})
	if err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry(p.Tools()...)
	q, _ := reg.Lookup("prom.query")
	if q.Descriptor().Risk != domain.RiskR0 {
		t.Fatal("prom.query must be R0")
	}
	out, err := q.Invoke(context.Background(), "k", json.RawMessage(`{"query":"up"}`))
	if err != nil {
		t.Fatal(err)
	}
	var o struct {
		Series      []any `json:"series"`
		SeriesTotal int   `json:"series_total"`
		Truncated   bool  `json:"truncated"`
	}
	_ = json.Unmarshal(out, &o)
	if len(o.Series) != 5 || o.SeriesTotal != 30 || !o.Truncated || gotPath != "/api/v1/query" {
		t.Fatalf("%s", out)
	}
	if _, err := q.Invoke(context.Background(), "k", json.RawMessage(`{"query":"up","range_minutes":30}`)); err != nil || gotPath != "/api/v1/query_range" {
		t.Fatalf("range query: %v %s", err, gotPath)
	}
	_, err = q.Invoke(context.Background(), "k", json.RawMessage(`{"query":"bad("}`))
	var re *domain.RetryableError
	if err == nil || errors.As(err, &re) || !strings.Contains(err.Error(), "parse error") {
		t.Fatalf("bad query must be a permanent error: %v", err)
	}
}

type fakeK8s struct {
	mu          sync.Mutex
	annotations map[string]string
	patches     int
	dryRuns     int
}

func (f *fakeK8s) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer tok" {
		w.WriteHeader(401)
		return
	}
	if r.URL.Path != "/apis/apps/v1/namespaces/prod/deployments/checkout" {
		w.WriteHeader(404)
		return
	}
	if r.Method == http.MethodPatch {
		if r.Header.Get("Content-Type") != "application/strategic-merge-patch+json" {
			w.WriteHeader(415)
			return
		}
		var p struct {
			Spec struct {
				Template struct {
					Metadata struct {
						Annotations map[string]string `json:"annotations"`
					} `json:"metadata"`
				} `json:"template"`
			} `json:"spec"`
		}
		_ = json.NewDecoder(r.Body).Decode(&p)
		if r.URL.Query().Get("dryRun") == "All" {
			f.dryRuns++
		} else {
			f.patches++
			for k, v := range p.Spec.Template.Metadata.Annotations {
				f.annotations[k] = v
			}
		}
	}
	anno, _ := json.Marshal(f.annotations)
	_, _ = io.WriteString(w, `{"metadata":{"name":"checkout","namespace":"prod","generation":`+
		map[bool]string{true: "8", false: "7"}[f.patches > 0]+`,"annotations":{"deployment.kubernetes.io/revision":"12"}},`+
		`"spec":{"replicas":3,"template":{"metadata":{"annotations":`+string(anno)+`}}},`+
		`"status":{"readyReplicas":3,"updatedReplicas":3,"availableReplicas":3}}`)
}

func TestK8sRestartDryRunAllowlistAndIdempotency(t *testing.T) {
	f := &fakeK8s{annotations: map[string]string{}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	dry, err := NewKubernetes(KubernetesConfig{APIServer: srv.URL, Token: "tok", Namespaces: []string{"prod"}, HTTP: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry(dry.Tools()...)
	rs, _ := reg.Lookup("k8s.rollout_restart")
	d := rs.Descriptor()
	if d.Risk != domain.RiskR2 || !strings.Contains(d.Description, "DRY-RUN") {
		t.Fatalf("%+v", d)
	}
	args := json.RawMessage(`{"namespace":"prod","deployment":"checkout","reason":"pool exhaustion"}`)
	if tgt := tools.Target(d, args); tgt != "prod/checkout" {
		t.Fatalf("target = %q", tgt)
	}
	out, err := rs.Invoke(context.Background(), "key-1", args)
	if err != nil || f.dryRuns != 1 || f.patches != 0 || !strings.Contains(string(out), "rollout undo deployment/checkout -n prod --to-revision=12") {
		t.Fatalf("dry run: %s %v dry=%d live=%d", out, err, f.dryRuns, f.patches)
	}
	_, err = rs.Invoke(context.Background(), "key-1", json.RawMessage(`{"namespace":"kube-system","deployment":"coredns"}`))
	if !errors.Is(err, domain.ErrNotDispatched) {
		t.Fatalf("namespace outside allowlist: %v", err)
	}

	no := false
	live, _ := NewKubernetes(KubernetesConfig{APIServer: srv.URL, Token: "tok", Namespaces: []string{"prod"}, DryRun: &no, HTTP: srv.Client()})
	lr := live.Tools()[1]
	if _, err := lr.Invoke(context.Background(), "key-2", args); err != nil || f.patches != 1 {
		t.Fatalf("live restart: %v patches=%d", err, f.patches)
	}
	out, err = lr.Invoke(context.Background(), "key-2", args)
	if err != nil || f.patches != 1 || !strings.Contains(string(out), "already applied") {
		t.Fatalf("retry must not restart twice: %s %v patches=%d", out, err, f.patches)
	}
	if _, err := lr.Invoke(context.Background(), "key-3", json.RawMessage(`{"namespace":"prod","deployment":"missing"}`)); !errors.Is(err, domain.ErrNotDispatched) {
		t.Fatalf("missing deployment: %v", err)
	}
}
