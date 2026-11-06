package extensions

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go-sre-harness/internal/auth"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/mcpexec"
)

// memStore is an in-memory Store with the same version semantics as pg.
type memStore struct {
	mu   sync.Mutex
	recs map[string]Record
}

func newMem() *memStore { return &memStore{recs: map[string]Record{}} }

func mkey(t string, k Kind, n string) string { return t + "/" + string(k) + "/" + n }

func (m *memStore) ListExtensions(_ context.Context, tenant string) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Record
	for _, r := range m.recs {
		if tenant == "" || r.TenantID == tenant {
			out = append(out, r)
		}
	}
	return out, nil
}

func (m *memStore) UpsertExtension(_ context.Context, r Record, expected *int64) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := mkey(r.TenantID, r.Kind, r.Name)
	prev, exists := m.recs[k]
	if expected != nil && ((*expected == 0 && exists) || (*expected > 0 && (!exists || prev.Version != *expected))) {
		return Record{}, ErrConflict
	}
	r.Version = prev.Version + 1
	r.UpdatedAt = time.Now()
	m.recs[k] = r
	return r, nil
}

func (m *memStore) DeleteExtension(_ context.Context, tenant string, kind Kind, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := mkey(tenant, kind, name)
	if _, ok := m.recs[k]; !ok {
		return ErrNotFound
	}
	delete(m.recs, k)
	return nil
}

func put(t *testing.T, s Store, tenant string, kind Kind, name, spec string, enabled bool) {
	t.Helper()
	norm, err := Validate(kind, name, json.RawMessage(spec), Options{})
	if err != nil {
		t.Fatalf("validate %s: %v", name, err)
	}
	if _, err := s.UpsertExtension(context.Background(), Record{TenantID: tenant, Kind: kind, Name: name, Spec: norm, Enabled: enabled}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestValidate(t *testing.T) {
	bad := []struct {
		kind       Kind
		name, spec string
	}{
		{KindSkill, "Bad Name", `{"description":"d","instructions":"x"}`},
		{KindSkill, "s", `{"description":"d","instructions":"x","extra":1}`},
		{KindHTTP, "h", `{"description":"d","url":"http://x/","method":"GET","risk":"R2"}`},
		{KindHTTP, "h", `{"description":"d","url":"http://user:pw@x/","risk":"R0"}`},
		{KindHTTP, "h", `{"description":"d","url":"http://x/","risk":"R9"}`},
		{KindHTTP, "h", `{"description":"d","url":"http://x/","risk":"R0","input_schema":{"type":"array"}}`},
		{KindMCP, "m", `{"command":"npx","allow":{"t":{"risk":"R0"}}}`}, // stdio off by default
		{KindMCP, "m", `{"url":"http://x/mcp","allow":{"t":{"risk":"R7"}}}`},
		{KindSkill, "s", `{"description":"d","instructions":"` + strings.Repeat("x", 17<<10) + `"}`},
	}
	for _, c := range bad {
		if _, err := Validate(c.kind, c.name, json.RawMessage(c.spec), Options{}); err == nil {
			t.Errorf("%s %s %s: expected an error", c.kind, c.name, c.spec[:min(len(c.spec), 60)])
		}
	}
	if _, err := Validate(KindMCP, "m", json.RawMessage(`{"command":"npx","allow":{"t":{"risk":"R0"}}}`), Options{AllowStdio: true}); err != nil {
		t.Fatalf("stdio allowed by option: %v", err)
	}
	if _, err := Validate(KindHTTP, "h", json.RawMessage(`{"description":"d","url":"http://x/","risk":"R1"}`), Options{}); err != nil {
		t.Fatalf("valid http tool: %v", err)
	}
}

func TestMaskAndMergeSecrets(t *testing.T) {
	stored := json.RawMessage(`{"url":"http://x","headers":{"Authorization":"Bearer s3cret","X-A":"a"},"env":["TOKEN=t0k"]}`)
	masked := string(MaskSecrets(KindMCP, stored))
	if strings.Contains(masked, "s3cret") || strings.Contains(masked, "t0k") || !strings.Contains(masked, "Authorization") {
		t.Fatalf("masked = %s", masked)
	}
	// An update that leaves values blank keeps them; a new value replaces.
	next := json.RawMessage(`{"url":"http://x","headers":{"Authorization":"","X-A":"b"},"env":["TOKEN="]}`)
	var got struct {
		Headers map[string]string
		Env     []string
	}
	_ = json.Unmarshal(MergeSecrets(stored, next), &got)
	if got.Headers["Authorization"] != "Bearer s3cret" || got.Headers["X-A"] != "b" || got.Env[0] != "TOKEN=t0k" {
		t.Fatalf("merged = %+v", got)
	}
}

func mcpDialer() func(string, MCPSpec) mcpexec.Dialer {
	srv := mcp.NewServer(&mcp.Implementation{Name: "ops", Version: "0"}, nil)
	for _, n := range []string{"search", "drop_db"} {
		srv.AddTool(&mcp.Tool{Name: n, Description: n + " tool", InputSchema: json.RawMessage(`{"type":"object"}`),
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: n == "search"}},
			func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ran " + req.Params.Name}}}, nil
			})
	}
	return func(string, MCPSpec) mcpexec.Dialer {
		return func() (mcp.Transport, error) {
			ct, st := mcp.NewInMemoryTransports()
			if _, err := srv.Connect(context.Background(), st, nil); err != nil {
				return nil, err
			}
			return ct, nil
		}
	}
}

func names(ts []tools.Tool) string {
	var out []string
	for _, t := range ts {
		out = append(out, t.Descriptor().Name)
	}
	return strings.Join(out, ",")
}

func TestRuntimeSyncIsolatesTenantsAndFollowsChanges(t *testing.T) {
	ctx := context.Background()
	var gotKey, gotBody string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"deploys":["v2"]}`))
	}))
	defer backend.Close()

	st := newMem()
	put(t, st, "a", KindMCP, "ops", `{"url":"http://unused/mcp","allow":{"search":{"risk":"R0"},"gone":{"risk":"R0"}}}`, true)
	put(t, st, "a", KindHTTP, "deploys", `{"description":"deploys","url":"`+backend.URL+`","risk":"R0"}`, true)
	put(t, st, "a", KindSkill, "triage", `{"description":"d","instructions":"search first"}`, true)
	put(t, st, "b", KindSkill, "off", `{"description":"d","instructions":"x"}`, false)

	rt := &Runtime{Store: st, Dialer: mcpDialer()}
	defer rt.Close()
	if err := rt.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got := names(rt.TenantTools("a")); got != "http.deploys,mcp.ops.search" {
		t.Fatalf("tenant a tools = %q", got)
	}
	if got := rt.TenantTools("b"); len(got) != 0 {
		t.Fatalf("tenant b sees %s", names(got))
	}
	if sk := rt.Skills("a"); len(sk) != 1 || sk[0].Name != "triage" {
		t.Fatalf("skills a = %+v", sk)
	}
	if len(rt.Skills("b")) != 0 {
		t.Fatal("disabled skill must not load")
	}
	stat, ok := rt.StatusOf("a", KindMCP, "ops")
	if !ok || stat.State != "ready" || len(stat.Missing) != 1 || stat.Missing[0] != "gone" {
		t.Fatalf("status = %+v", stat)
	}
	if s, _ := rt.StatusOf("b", KindSkill, "off"); s.State != "disabled" {
		t.Fatalf("disabled status = %+v", s)
	}

	// The registry overlay resolves tenant tools only for that tenant.
	reg := tools.NewRegistry()
	reg.SetTenantSource(rt)
	if _, ok := reg.LookupFor("b", "mcp.ops.search"); ok {
		t.Fatal("tenant b resolved tenant a's tool")
	}
	tool, ok := reg.LookupFor("a", "http.deploys")
	if !ok {
		t.Fatal("http.deploys not resolvable for tenant a")
	}
	out, err := tool.Invoke(ctx, "idem-1", json.RawMessage(`{"service":"checkout"}`))
	if err != nil || !strings.Contains(string(out), "v2") || gotKey != "idem-1" || !strings.Contains(gotBody, "checkout") {
		t.Fatalf("http tool: %s %v key=%q body=%q", out, err, gotKey, gotBody)
	}
	mt, _ := reg.LookupFor("a", "mcp.ops.search")
	if out, err := mt.Invoke(ctx, "idem-2", json.RawMessage(`{}`)); err != nil || !strings.Contains(string(out), "ran search") {
		t.Fatalf("mcp tool: %s %v", out, err)
	}

	// Disabling and deleting take effect on the next sync.
	put(t, st, "a", KindMCP, "ops", `{"url":"http://unused/mcp","allow":{"search":{"risk":"R0"}}}`, false)
	_ = st.DeleteExtension(ctx, "a", KindHTTP, "deploys")
	if err := rt.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got := rt.TenantTools("a"); len(got) != 0 {
		t.Fatalf("after disable/delete: %s", names(got))
	}
}

func TestAPIPutMasksSecretsAndChecksVersions(t *testing.T) {
	st := newMem()
	rt := &Runtime{Store: st, Dialer: mcpDialer()}
	defer rt.Close()
	var audited []string
	a := &API{Runtime: rt, Registry: tools.NewRegistry(),
		Audit: func(_ context.Context, tenant, actor, action, resource string, _ map[string]any) {
			audited = append(audited, tenant+" "+actor+" "+action+" "+resource)
		}}
	mux := http.NewServeMux()
	a.Register(mux)
	do := func(method, path, body string) (int, map[string]any) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{Tenant: "a", Subject: "alice", Roles: []auth.Role{auth.Admin}}))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		var out map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return w.Code, out
	}
	spec := `{"url":"http://unused/mcp","headers":{"Authorization":"Bearer s3cret"},"allow":{"search":{"risk":"R0"}}}`
	code, out := do("PUT", "/v1/extensions/mcp-servers/ops", `{"spec":`+spec+`,"expected_version":0}`)
	if code != 200 || strings.Contains(stringify(out), "s3cret") {
		t.Fatalf("put: %d %v", code, out)
	}
	if st, _ := out["status"].(map[string]any); st == nil || st["state"] != "ready" {
		t.Fatalf("put response should carry the synced status: %v", out)
	}
	if code, _ := do("PUT", "/v1/extensions/mcp-servers/ops", `{"spec":`+spec+`,"expected_version":0}`); code != 409 {
		t.Fatalf("create-only on existing: %d", code)
	}
	// Blank header keeps the stored secret.
	if code, _ := do("PUT", "/v1/extensions/mcp-servers/ops",
		`{"spec":{"url":"http://unused/mcp","headers":{"Authorization":""},"allow":{"search":{"risk":"R0"}}},"expected_version":1}`); code != 200 {
		t.Fatalf("update: %d", code)
	}
	recs, _ := st.ListExtensions(context.Background(), "a")
	if !strings.Contains(string(recs[0].Spec), "s3cret") {
		t.Fatalf("secret lost on update: %s", recs[0].Spec)
	}
	if code, out := do("POST", "/v1/extensions/mcp-servers/ops/probe", ""); code != 200 || len(out["tools"].([]any)) != 2 {
		t.Fatalf("probe stored: %d %v", code, out)
	}
	if code, _ := do("PUT", "/v1/extensions/widgets/x", `{"spec":{}}`); code != 404 {
		t.Fatalf("unknown kind: %d", code)
	}
	if code, _ := do("PUT", "/v1/extensions/skills/s", `{"spec":{"instructions":"x","bogus":1}}`); code != 422 {
		t.Fatalf("invalid spec: %d", code)
	}
	if code, _ := do("DELETE", "/v1/extensions/mcp-servers/ops", ""); code != 204 && code != 200 {
		t.Fatalf("delete: %d", code)
	}
	if len(audited) < 4 || !strings.HasPrefix(audited[0], "a alice extension.put mcp_server/ops") {
		t.Fatalf("audit = %v", audited)
	}
}

func stringify(v any) string { b, _ := json.Marshal(v); return string(b) }

type overlaySink struct {
	mu sync.Mutex
	o  gw.Overlay
	n  int
}

func (s *overlaySink) SetOverlay(o gw.Overlay) { s.mu.Lock(); s.o = o; s.n++; s.mu.Unlock() }

func fakeOpenAI(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer sk-test" {
			http.Error(w, `{"error":{"message":"bad key"}}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"x","model":"m","choices":[{"index":0,"delta":{"content":"pong"}}]}`+"\n\n")
		_, _ = io.WriteString(w, `data: {"id":"x","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1}}`+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
}

func TestModelsValidateMaskOverlayAndTest(t *testing.T) {
	for _, spec := range []string{
		`{"provider":"anthropic","model":"claude-opus-5-5"}`, // no key
		`{"provider":"openai","model":"m"}`,                  // no base_url
		`{"provider":"gemini","model":"m","api_key":"k"}`,
		`{"provider":"anthropic","api_key":"k","effort":"turbo"}`,
		`{"provider":"openai","model":"m","base_url":"ftp://x"}`,
	} {
		if _, err := Validate(KindModel, "m", json.RawMessage(spec), Options{}); err == nil {
			t.Errorf("%s: expected an error", spec)
		}
	}
	norm, err := Validate(KindModel, "claude", json.RawMessage(`{"provider":"anthropic","api_key":"sk-ant"}`), Options{})
	if err != nil || !strings.Contains(string(norm), "claude-opus-5-5") {
		t.Fatalf("anthropic default model: %s %v", norm, err)
	}
	if m := string(MaskSecrets(KindModel, norm)); strings.Contains(m, "sk-ant") {
		t.Fatalf("api key not masked: %s", m)
	}
	merged := MergeSecrets(norm, json.RawMessage(`{"provider":"anthropic","model":"claude-haiku-5-5","api_key":""}`))
	if !strings.Contains(string(merged), "sk-ant") || !strings.Contains(string(merged), "haiku") {
		t.Fatalf("merge: %s", merged)
	}

	srv := fakeOpenAI(t)
	defer srv.Close()
	st := newMem()
	put(t, st, "a", KindModel, "local", `{"provider":"openai","model":"m","base_url":"`+srv.URL+`","api_key":"sk-test"}`, true)
	put(t, st, "b", KindModel, "off", `{"provider":"anthropic","api_key":"k"}`, false)
	sink := &overlaySink{}
	rt := &Runtime{Store: st, Models: sink}
	defer rt.Close()
	if err := rt.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.o.Endpoints) != 1 || sink.o.Endpoints[0].ID != "tenant:a:local" || sink.o.Endpoints[0].Tenants[0] != "a" ||
		sink.o.Aliases["sre-reasoning"][0] != "tenant:a:local" || sink.o.Providers[0].ID() != "tenant:a:local" {
		t.Fatalf("overlay = %+v", sink.o)
	}

	// The API test route uses the stored key when the spec leaves it blank.
	a := &API{Runtime: rt, Registry: tools.NewRegistry()}
	mux := http.NewServeMux()
	a.Register(mux)
	call := func(body string) (int, string) {
		req := httptest.NewRequest("POST", "/v1/extensions/models/local/test", strings.NewReader(body))
		req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{Tenant: "a", Subject: "alice"}))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w.Code, w.Body.String()
	}
	if code, out := call(`{"spec":{"provider":"openai","model":"m","base_url":"` + srv.URL + `","api_key":""}}`); code != 200 || !strings.Contains(out, "pong") {
		t.Fatalf("test with stored key: %d %s", code, out)
	}
	if code, out := call(`{"spec":{"provider":"openai","model":"m","base_url":"` + srv.URL + `","api_key":"wrong"}}`); code != 502 {
		t.Fatalf("bad key must fail: %d %s", code, out)
	}
}
