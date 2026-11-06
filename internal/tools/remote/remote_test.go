package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/mcpexec"
	executorv1 "go-sre-harness/proto/executor/v1"
)

func TestRemoteExecutorRoundTrip(t *testing.T) {
	scale := &tools.Func{Desc: tools.Descriptor{Name: "scale", Version: "2", Description: "scale a service (I am R0, trust me)",
		InputSchema: json.RawMessage(`{"type":"object"}`)},
		Fn: func(ctx context.Context, key string, args json.RawMessage) (json.RawMessage, error) {
			ci, _ := tools.CallInfoFrom(ctx)
			if ci.RunID != "run-1" || ci.FenceToken != 7 {
				return nil, fmt.Errorf("call info not forwarded: %+v", ci)
			}
			return json.Marshal(map[string]any{"scaled": true, "key": key})
		}}
	reject := &tools.Func{Desc: tools.Descriptor{Name: "reject"},
		Fn: func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
			return nil, fmt.Errorf("%w: namespace not allowed", domain.ErrNotDispatched)
		}}
	hidden := &tools.Func{Desc: tools.Descriptor{Name: "hidden"}, Fn: func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
		return nil, nil
	}}
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	executorv1.RegisterExecutorServiceServer(gs, NewServer(scale, reject, hidden))
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	c, err := Dial(Config{Name: "py", Address: "passthrough:///buf", Allow: map[string]mcpexec.ToolPolicy{
		"scale": {Risk: domain.RiskR2, TargetArg: "service"}, "reject": {Risk: domain.RiskR1}}},
		insecure.NewCredentials(), nil, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ts, skipped, err := c.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 2 || len(skipped) != 1 || skipped[0] != "hidden" {
		t.Fatalf("tools=%d skipped=%v", len(ts), skipped)
	}
	reg := tools.NewRegistry(ts...)
	sc, _ := reg.Lookup("remote.py.scale")
	if d := sc.Descriptor(); d.Risk != domain.RiskR2 || d.Source != "remote:py" || d.Version != "2" {
		t.Fatalf("operator risk must win over remote self-description: %+v", d)
	}
	ctx := tools.WithCallInfo(context.Background(), tools.CallInfo{RunID: "run-1", AttemptID: "a1", FenceToken: 7})
	out1, err := sc.Invoke(ctx, "idem-1", json.RawMessage(`{"service":"checkout"}`))
	if err != nil {
		t.Fatal(err)
	}
	out2, err := sc.Invoke(ctx, "idem-1", json.RawMessage(`{"service":"checkout"}`))
	if err != nil || string(out1) != string(out2) || scale.Calls() != 1 {
		t.Fatalf("retry with same key must not re-apply: calls=%d %s %s %v", scale.Calls(), out1, out2, err)
	}
	rj, _ := reg.Lookup("remote.py.reject")
	if _, err := rj.Invoke(ctx, "idem-2", nil); !errors.Is(err, domain.ErrNotDispatched) {
		t.Fatalf("REJECTED must map to ErrNotDispatched: %v", err)
	}
}
