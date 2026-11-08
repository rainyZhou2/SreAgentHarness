package artifactq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

type memStore struct{ data map[string][]byte }

func (m *memStore) Put(_ context.Context, tenantID, kind string, b []byte) (string, string, error) {
	ref := fmt.Sprintf("mem://%s/%s/h%d", tenantID, kind, len(m.data))
	m.data[ref] = b
	return ref, "h", nil
}

func (m *memStore) Get(_ context.Context, _, ref string) ([]byte, error) {
	b, ok := m.data[ref]
	if !ok {
		return nil, errors.New("not found")
	}
	return b, nil
}

func logText() []byte {
	var b strings.Builder
	for i := range 300 {
		lvl := "INFO"
		if i%10 == 0 {
			lvl = "ERROR"
		}
		fmt.Fprintf(&b, "2026-11-07T10:%02d:%02dZ %s line %d\n", i/60, i%60, lvl, i+1)
	}
	return []byte(b.String())
}

func run(t *testing.T, data []byte, a args) map[string]any {
	t.Helper()
	out, err := query(data, a)
	if err != nil {
		t.Fatalf("query %+v: %v", a, err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestLineOps(t *testing.T) {
	data := logText()
	if m := run(t, data, args{Op: "head", Limit: 3}); len(m["lines"].([]any)) != 3 || m["total_lines"].(float64) != 300 {
		t.Fatalf("head %v", m)
	}
	tail := run(t, data, args{Op: "tail", Limit: 2})["lines"].([]any)
	if tail[1].(map[string]any)["n"].(float64) != 300 {
		t.Fatalf("tail %v", tail)
	}
	lines := run(t, data, args{Op: "lines", StartLine: 10, EndLine: 12})["lines"].([]any)
	if len(lines) != 3 || lines[0].(map[string]any)["n"].(float64) != 10 {
		t.Fatalf("lines %v", lines)
	}
	g := run(t, data, args{Op: "grep", Pattern: "error", IgnoreCase: true, Limit: 5})
	if g["matched"].(float64) != 30 || len(g["lines"].([]any)) != 5 {
		t.Fatalf("grep %v", g)
	}
	inv := run(t, data, args{Op: "grep", Pattern: "ERROR", Invert: true})
	if inv["matched"].(float64) != 270 {
		t.Fatalf("invert %v", inv)
	}
	tm := run(t, data, args{Op: "time", From: "2026-11-07T10:01:00Z", To: "2026-11-07T10:02:00Z", Limit: 200})
	if tm["matched"].(float64) != 60 {
		t.Fatalf("time %v", tm)
	}
}

func TestJSONOps(t *testing.T) {
	data := []byte(`[{"ts":"2026-11-07T10:00:00Z","v":1},{"ts":"2026-11-07T11:00:00Z","v":2},{"ts":"bad","v":3}]`)
	p := run(t, data, args{Op: "path", Path: "data[1].v"})
	if p["value"].(float64) != 2 {
		t.Fatalf("path %v", p)
	}
	tm := run(t, data, args{Op: "time", From: "2026-11-07T10:30:00Z", TimeField: "ts"})
	if tm["matched"].(float64) != 1 || tm["total_items"].(float64) != 3 {
		t.Fatalf("time items %v", tm)
	}
	if _, err := query([]byte("text only"), args{Op: "path", Path: "data.x"}); err == nil {
		t.Fatal("path on text must fail")
	}
}

func TestQueryErrors(t *testing.T) {
	for _, a := range []args{{Op: "grep"}, {Op: "grep", Pattern: "("}, {Op: "time"}, {Op: "time", From: "yesterday"}, {Op: "nope"}} {
		if _, err := query(logText(), a); err == nil {
			t.Errorf("%+v should fail", a)
		}
	}
}

func TestOutputIsBounded(t *testing.T) {
	big := []byte(strings.Repeat(strings.Repeat("y", 1500)+"\n", 200))
	out, err := query(big, args{Op: "head", Limit: 200})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) > maxOutput {
		t.Fatalf("output %d bytes", len(out))
	}
	if !strings.Contains(string(out), `"truncated":true`) {
		t.Fatal("expected truncated flag")
	}
}

func TestInvokeTenantIsolation(t *testing.T) {
	store := &memStore{data: map[string][]byte{}}
	ref, _, _ := store.Put(context.Background(), "t1", Kind, logText())
	other, _, _ := store.Put(context.Background(), "t1", "prompt", []byte("secret"))
	tool := New(store)
	if d := tool.Descriptor(); d.Risk != domain.RiskR0 || !tools.IsBuiltin(d.Name) {
		t.Fatalf("descriptor %+v", d)
	}
	ctx := tools.WithCallInfo(context.Background(), tools.CallInfo{TenantID: "t1"})
	call := func(ctx context.Context, handle string) error {
		raw, _ := json.Marshal(map[string]any{"handle": handle, "op": "head", "limit": 1})
		_, err := tool.Invoke(ctx, "", raw)
		return err
	}
	if err := call(ctx, ref); err != nil {
		t.Fatalf("own handle: %v", err)
	}
	if err := call(ctx, other); !errors.Is(err, ErrHandle) {
		t.Fatalf("non tool-output artifact: %v", err)
	}
	t2 := tools.WithCallInfo(context.Background(), tools.CallInfo{TenantID: "t2"})
	if err := call(t2, ref); !errors.Is(err, ErrHandle) {
		t.Fatalf("other tenant: %v", err)
	}
	if err := call(context.Background(), ref); !errors.Is(err, domain.ErrNotDispatched) {
		t.Fatalf("missing call info: %v", err)
	}
	if ValidHandle("t1", "mem://t1/tool-output/../x") || ValidHandle("", ref) {
		t.Fatal("ValidHandle accepted a bad handle")
	}
}
