package sandboxexec

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/sandbox"
	"go-sre-harness/internal/tools"
)

type fakeBackend struct {
	got sandbox.Request
	err error
}

func (f *fakeBackend) Info() sandbox.Info {
	return sandbox.Info{Backend: "fake", Isolation: "container", Image: "img", Network: "none", Workdir: "/workspace"}
}
func (f *fakeBackend) Exec(_ context.Context, r sandbox.Request) (sandbox.Result, error) {
	f.got = r
	return sandbox.Result{ExitCode: 0, Stdout: "ok"}, f.err
}
func (f *fakeBackend) Destroy(context.Context, string, string) error { return nil }

func isRetryable(err error) bool {
	var r *domain.RetryableError
	return errors.As(err, &r)
}

func TestTool(t *testing.T) {
	b := &fakeBackend{}
	tool := Tool{Backend: b}
	d := tool.Descriptor()
	if d.Risk != domain.RiskR0 || d.Trust != "T4" || d.Name != Name {
		t.Fatalf("descriptor: %+v", d)
	}
	ctx := tools.WithCallInfo(context.Background(), tools.CallInfo{TenantID: "t", RunID: "r"})
	out, err := tool.Invoke(ctx, "", json.RawMessage(`{"command":"echo hi","files":{"a.txt":"x"},"outputs":["a.txt"],"timeout_seconds":5}`))
	if err != nil || b.got.Argv[0] != "sh" || b.got.Argv[2] != "echo hi" || b.got.RunID != "r" || b.got.Timeout.Seconds() != 5 {
		t.Fatalf("invoke: %v %+v", err, b.got)
	}
	var res sandbox.Result
	if json.Unmarshal(out, &res) != nil || res.Stdout != "ok" {
		t.Fatal(string(out))
	}
	if _, err := tool.Invoke(ctx, "", json.RawMessage(`{"command":"x","argv":["y"]}`)); err == nil {
		t.Fatal("command and argv")
	}
	if _, err := tool.Invoke(context.Background(), "", json.RawMessage(`{"command":"x"}`)); err == nil {
		t.Fatal("no run")
	}
	b.err = errors.New("daemon down")
	if _, err := tool.Invoke(ctx, "", json.RawMessage(`{"command":"x"}`)); !isRetryable(err) {
		t.Fatalf("backend failure retries: %v", err)
	}
	b.err = sandbox.ErrInvalid
	if _, err := tool.Invoke(ctx, "", json.RawMessage(`{"command":"x"}`)); isRetryable(err) {
		t.Fatal("invalid request is permanent")
	}
}
