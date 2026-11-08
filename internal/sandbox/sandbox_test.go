package sandbox

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCleanPathAndValidate(t *testing.T) {
	for _, bad := range []string{"", "/etc/passwd", "../x", "a/../../x", "..", "a\\b", "/workspace/../x"} {
		if _, err := CleanPath(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
	if p, err := CleanPath("/workspace/a/./b.txt"); err != nil || p != "a/b.txt" {
		t.Fatalf("clean: %q %v", p, err)
	}
	ok := Request{TenantID: "t", RunID: "r", Argv: []string{"sh"}}
	if ok.Validate() != nil {
		t.Fatal("valid")
	}
	for _, r := range []Request{
		{RunID: "r", Argv: []string{"sh"}},
		{TenantID: "t", RunID: "r"},
		{TenantID: "t", RunID: "r", Argv: []string{"sh"}, Files: map[string]string{"../x": ""}},
		{TenantID: "t", RunID: "r", Argv: []string{"sh"}, Files: map[string]string{"big": strings.Repeat("x", MaxFileBytes+1)}},
		{TenantID: "t", RunID: "r", Argv: []string{"sh"}, Env: map[string]string{"A=B": "c"}},
	} {
		if err := r.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("expected invalid: %+v", r)
		}
	}
	if WorkspaceKey("t", "r") == WorkspaceKey("t2", "r") || strings.Contains(WorkspaceKey("tenant", "run"), "tenant") {
		t.Fatal("workspace keys")
	}
}

func TestRunArgsSafetyFloor(t *testing.T) {
	c, err := NewContainer(ContainerConfig{Runtime: "sh", Image: "img@sha256:abc", Root: t.TempDir(),
		ExtraArgs: []string{"--runtime=runsc"}})
	if err != nil {
		t.Fatal(err)
	}
	os.Setenv("HARNESS_DATABASE_URL", "postgres://secret")
	defer os.Unsetenv("HARNESS_DATABASE_URL")
	args := strings.Join(c.runArgs("n1", "/host/ws", Request{Argv: []string{"python", "-c", "1"}, Env: map[string]string{"B": "2", "A": "1"}}), " ")
	for _, want := range []string{"--network none", "--read-only", "--cap-drop ALL", "--security-opt no-new-privileges",
		"--pids-limit 128", "--memory 512m", "--memory-swap 512m", "--cpus 1", "--volume /host/ws:/workspace:rw",
		"--env A=1 --env B=2", "--runtime=runsc --stop-timeout 1 img@sha256:abc python -c 1", "--tmpfs /tmp:rw,nosuid,nodev,size=64m"} {
		if !strings.Contains(args, want) {
			t.Fatalf("missing %q in %s", want, args)
		}
	}
	if strings.Contains(args, "postgres://secret") || strings.Contains(args, "--privileged") {
		t.Fatal("harness environment leaked into the container")
	}
	if c.Info().Network != "none" {
		t.Fatal("default network")
	}
}

// fakeRuntime stands in for docker: it runs the command on the host in the
// mounted workspace with only the requested environment.
const fakeRuntime = `#!/bin/sh
if [ "$1" = "kill" ]; then exit 0; fi
shift
ws=""; envs=""
while [ $# -gt 0 ]; do
  case "$1" in
    --volume) ws="${2%%:/workspace:rw}"; shift 2;;
    --env) envs="$envs $2"; shift 2;;
    --stop-timeout) shift 3; break;;
    --rm|-i|--read-only) shift;;
    *) shift 2;;
  esac
done
cd "$ws" || exit 125
exec env -i PATH=/usr/bin:/bin $envs "$@"
`

func fakeContainer(t *testing.T, limits Limits) (*Container, string) {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-docker")
	if err := os.WriteFile(bin, []byte(fakeRuntime), 0o755); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "root")
	c, err := NewContainer(ContainerConfig{Runtime: bin, Image: "img", Root: root, Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	return c, root
}

func TestExecWorkspaceFilesAndLimits(t *testing.T) {
	c, root := fakeContainer(t, Limits{MaxOutputBytes: 64, Timeout: 2 * time.Second})
	ctx := context.Background()
	t.Setenv("HARNESS_SECRET", "s3cret")
	res, err := c.Exec(ctx, Request{TenantID: "t", RunID: "r", Argv: []string{"sh", "-c",
		"wc -l < in/data.txt > count.txt; echo $GREETING; env | grep -c SECRET; exit 3"},
		Files: map[string]string{"in/data.txt": "a\nb\nc\n"}, Outputs: []string{"count.txt", "nope.txt"},
		Env: map[string]string{"GREETING": "hi"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.ExitCode != 3 || !strings.HasPrefix(res.Stdout, "hi\n0") {
		t.Fatalf("result: %+v", res)
	}
	if len(res.Files) != 2 || strings.TrimSpace(res.Files[0].Content) != "3" || !res.Files[1].Missing {
		t.Fatalf("files: %+v", res.Files)
	}
	// The workspace persists across calls of the run, not across runs.
	res, _ = c.Exec(ctx, Request{TenantID: "t", RunID: "r", Argv: []string{"cat", "count.txt"}})
	if strings.TrimSpace(res.Stdout) != "3" {
		t.Fatalf("persisted: %q", res.Stdout)
	}
	res, _ = c.Exec(ctx, Request{TenantID: "t", RunID: "other", Argv: []string{"sh", "-c", "ls"}})
	if strings.TrimSpace(res.Stdout) != "" {
		t.Fatalf("another run sees nothing: %q", res.Stdout)
	}
	res, _ = c.Exec(ctx, Request{TenantID: "t", RunID: "r", Argv: []string{"sh", "-c", "yes | head -c 1000"}})
	if len(res.Stdout) != 64 || !res.StdoutTruncated {
		t.Fatalf("cap: %d %v", len(res.Stdout), res.StdoutTruncated)
	}
	res, _ = c.Exec(ctx, Request{TenantID: "t", RunID: "r", Argv: []string{"sleep", "5"}, Timeout: 300 * time.Millisecond})
	if !res.TimedOut || res.DurationMS > 4000 {
		t.Fatalf("timeout: %+v", res)
	}

	// A planted symlink cannot make the harness read or write host files.
	outside := filepath.Join(t.TempDir(), "host-secret")
	_ = os.WriteFile(outside, []byte("host data"), 0o600)
	res, _ = c.Exec(ctx, Request{TenantID: "t", RunID: "r", Argv: []string{"sh", "-c",
		"ln -s " + outside + " leak.txt && ln -s " + filepath.Dir(outside) + " hostdir"}, Outputs: []string{"leak.txt"}})
	if len(res.Files) != 1 || !res.Files[0].Missing || res.Files[0].Content != "" {
		t.Fatalf("symlink read: %+v", res.Files)
	}
	if _, err := c.Exec(ctx, Request{TenantID: "t", RunID: "r", Argv: []string{"true"},
		Files: map[string]string{"hostdir/pwned": "x"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("symlink write: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(outside), "pwned")); err == nil {
		t.Fatal("wrote outside the workspace")
	}
	if err := c.Destroy(ctx, "t", "r"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, WorkspaceKey("t", "r"))); !os.IsNotExist(err) {
		t.Fatal("destroyed")
	}
}

func TestExecRuntimeFailure(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "broken")
	_ = os.WriteFile(bin, []byte("#!/bin/sh\necho 'docker: Cannot connect to the Docker daemon' >&2\nexit 125\n"), 0o755)
	c, _ := NewContainer(ContainerConfig{Runtime: bin, Image: "img", Root: filepath.Join(dir, "r")})
	if _, err := c.Exec(context.Background(), Request{TenantID: "t", RunID: "r", Argv: []string{"true"}}); err == nil {
		t.Fatal("a runtime failure is an error, not a result")
	}
	if _, err := NewContainer(ContainerConfig{Runtime: "no-such-runtime-xyz", Image: "i", Root: dir}); err == nil {
		t.Fatal("missing runtime")
	}
}

// TestDockerIntegration runs against a real Docker daemon when one is up.
func TestDockerIntegration(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("no docker")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon not running")
	}
	img := os.Getenv("HARNESS_SANDBOX_TEST_IMAGE")
	if img == "" {
		img = "alpine:3.20"
	}
	if err := exec.Command("docker", "image", "inspect", img).Run(); err != nil {
		t.Skip("image " + img + " not present locally")
	}
	c, err := NewContainer(ContainerConfig{Image: img, Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Exec(context.Background(), Request{TenantID: "t", RunID: "r", Argv: []string{"sh", "-c",
		"id -u; wget -q -T 2 -O- http://1.1.1.1 >/dev/null 2>&1 && echo net || echo nonet; touch /etc/x 2>/dev/null || echo rofs; echo ok > out"},
		Outputs: []string{"out"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Stdout, "nonet") || !strings.Contains(res.Stdout, "rofs") || strings.HasPrefix(res.Stdout, "0\n") ||
		strings.TrimSpace(res.Files[0].Content) != "ok" {
		t.Fatalf("docker: %+v", res)
	}
}
