package sandbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// ContainerConfig configures the container backend.
type ContainerConfig struct {
	// Runtime is the container CLI (docker or podman; a path or a name on
	// PATH).
	Runtime string `json:"runtime"`
	// Image runs every command. Pin it by digest in production.
	Image string `json:"image"`
	// Root holds the run workspaces on the host (created 0700).
	Root string `json:"root"`
	// Network is "none" (default). Anything else is passed to --network and
	// reported to the agent; egress control is then the network's job.
	Network string `json:"network,omitempty"`
	Limits  Limits `json:"limits"`
	// WorkspaceTTL removes workspaces untouched this long (default 24h).
	WorkspaceTTL time.Duration `json:"workspace_ttl,omitempty"`
	// ExtraArgs are appended to `run` before the image (operator-owned,
	// e.g. --runtime=runsc for gVisor).
	ExtraArgs []string     `json:"extra_args,omitempty"`
	Log       *slog.Logger `json:"-"`
}

// Container runs each command in a fresh container with the run's
// workspace mounted at /workspace.
type Container struct {
	cfg      ContainerConfig
	runtime  string
	mu       sync.Mutex
	runLocks map[string]*sync.Mutex
	lastGC   time.Time
}

const workdir = "/workspace"

// NewContainer checks the configuration and the runtime binary.
func NewContainer(cfg ContainerConfig) (*Container, error) {
	if cfg.Runtime == "" {
		cfg.Runtime = "docker"
	}
	if strings.TrimSpace(cfg.Image) == "" {
		return nil, errors.New("sandbox: image is required")
	}
	if cfg.Root == "" {
		return nil, errors.New("sandbox: root directory is required")
	}
	if cfg.Network == "" {
		cfg.Network = "none"
	}
	if cfg.WorkspaceTTL <= 0 {
		cfg.WorkspaceTTL = 24 * time.Hour
	}
	cfg.Limits = cfg.Limits.Merge(DefaultLimits)
	if cfg.Limits.Timeout > MaxTimeout {
		cfg.Limits.Timeout = MaxTimeout
	}
	bin, err := exec.LookPath(cfg.Runtime)
	if err != nil {
		return nil, fmt.Errorf("sandbox: container runtime %q not found: %w", cfg.Runtime, err)
	}
	if err := os.MkdirAll(cfg.Root, 0o700); err != nil {
		return nil, fmt.Errorf("sandbox: %w", err)
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Container{cfg: cfg, runtime: bin, runLocks: map[string]*sync.Mutex{}}, nil
}

func (c *Container) Info() Info {
	return Info{Backend: "container", Isolation: "container", Image: c.cfg.Image, Network: c.cfg.Network, Workdir: workdir}
}

// runLock serializes commands of one run: they share a workspace.
func (c *Container) runLock(key string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.runLocks[key]
	if !ok {
		l = &sync.Mutex{}
		c.runLocks[key] = l
	}
	return l
}

// user is the container user: the harness's own uid (so the bind mount is
// writable) unless that is root.
func user() string {
	uid, gid := os.Getuid(), os.Getgid()
	if uid <= 0 {
		return "65534:65534"
	}
	return fmt.Sprintf("%d:%d", uid, gid)
}

// runArgs is the full `run` command line.
func (c *Container) runArgs(name, hostDir string, req Request) []string {
	l := c.cfg.Limits
	args := []string{"run", "--rm", "-i", "--name", name,
		"--network", c.cfg.Network,
		"--read-only",
		"--tmpfs", fmt.Sprintf("/tmp:rw,nosuid,nodev,size=%dm", l.TmpMB),
		"--cpus", strconv.FormatFloat(l.CPUs, 'f', -1, 64),
		"--memory", fmt.Sprintf("%dm", l.MemoryMB),
		"--memory-swap", fmt.Sprintf("%dm", l.MemoryMB),
		"--pids-limit", strconv.Itoa(l.Pids),
		"--cap-drop", "ALL",
		"--security-opt", "no-new-privileges",
		"--user", user(),
		"--workdir", workdir,
		"--volume", hostDir + ":" + workdir + ":rw",
		"--label", "harness.sandbox=1",
		"--env", "HOME=/tmp",
	}
	keys := make([]string, 0, len(req.Env))
	for k := range req.Env {
		keys = append(keys, k)
	}
	sortStrings(keys)
	for _, k := range keys {
		args = append(args, "--env", k+"="+req.Env[k])
	}
	args = append(args, c.cfg.ExtraArgs...)
	args = append(args, "--stop-timeout", "1", c.cfg.Image)
	return append(args, req.Argv...)
}

func (c *Container) Exec(ctx context.Context, req Request) (Result, error) {
	if err := req.Validate(); err != nil {
		return Result{}, err
	}
	c.gc()
	key := WorkspaceKey(req.TenantID, req.RunID)
	lock := c.runLock(key)
	lock.Lock()
	defer lock.Unlock()

	hostDir := filepath.Join(c.cfg.Root, key)
	if err := os.MkdirAll(hostDir, 0o700); err != nil {
		return Result{}, err
	}
	now := time.Now()
	_ = os.Chtimes(hostDir, now, now)
	root, err := os.OpenRoot(hostDir)
	if err != nil {
		return Result{}, err
	}
	defer root.Close()
	for p, content := range req.Files {
		rel, _ := CleanPath(p)
		if dir := filepath.Dir(rel); dir != "." {
			if err := root.MkdirAll(dir, 0o755); err != nil {
				return Result{}, fmt.Errorf("%w: %s: %v", ErrInvalid, p, err)
			}
		}
		if err := root.WriteFile(rel, []byte(content), 0o644); err != nil {
			return Result{}, fmt.Errorf("%w: %s: %v", ErrInvalid, p, err)
		}
	}

	timeout := c.cfg.Limits.Timeout
	if req.Timeout > 0 && req.Timeout < timeout {
		timeout = req.Timeout
	}
	name := "harness-sbx-" + randHex(8)
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, c.runtime, c.runArgs(name, hostDir, req)...)
	// The CLI does not stop the container when it is killed: kill it by name.
	cmd.Cancel = func() error {
		kctx, kcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer kcancel()
		_ = exec.CommandContext(kctx, c.runtime, "kill", name).Run()
		return cmd.Process.Kill()
	}
	cmd.WaitDelay = 5 * time.Second
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")} // runtime CLI config only
	stdout := &capped{max: c.cfg.Limits.MaxOutputBytes}
	stderr := &capped{max: c.cfg.Limits.MaxOutputBytes}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.Stdin = strings.NewReader(req.Stdin)
	start := time.Now()
	runErr := cmd.Run()
	res := Result{Environment: c.Info(), DurationMS: time.Since(start).Milliseconds(),
		Limits: Summary{CPUs: c.cfg.Limits.CPUs, MemoryMB: c.cfg.Limits.MemoryMB, TimeoutMS: timeout.Milliseconds()}}
	res.Stdout, res.StdoutTruncated = string(bytes.ToValidUTF8(stdout.buf, []byte("�"))), stdout.truncated
	res.Stderr, res.StderrTruncated = string(bytes.ToValidUTF8(stderr.buf, []byte("�"))), stderr.truncated
	var exitErr *exec.ExitError
	switch {
	case runCtx.Err() == context.DeadlineExceeded:
		res.TimedOut, res.ExitCode = true, -1
	case ctx.Err() != nil:
		return Result{}, ctx.Err()
	case errors.As(runErr, &exitErr):
		res.ExitCode = exitErr.ExitCode()
		if res.ExitCode == 125 && runtimeFailure(res.Stderr) {
			// 125: the runtime itself failed (image, daemon); not the command.
			return Result{}, fmt.Errorf("sandbox runtime: %s", strings.TrimSpace(res.Stderr))
		}
	case runErr != nil:
		return Result{}, fmt.Errorf("sandbox runtime: %w", runErr)
	}
	for _, p := range req.Outputs {
		res.Files = append(res.Files, readOutput(root, p, c.cfg.Limits.MaxOutputBytes))
	}
	return res, nil
}

// readOutput reads a workspace file through the root, so a symlink the
// command planted cannot point the harness at a host file.
func readOutput(root *os.Root, p string, max int) File {
	rel, _ := CleanPath(p)
	f := File{Path: rel}
	st, err := root.Lstat(rel)
	if err != nil || !st.Mode().IsRegular() {
		f.Missing = true
		return f
	}
	f.Bytes = st.Size()
	fh, err := root.Open(rel)
	if err != nil {
		f.Missing = true
		return f
	}
	defer fh.Close()
	buf := make([]byte, max+1)
	n, _ := fh.Read(buf)
	if n > max {
		n, f.Truncated = max, true
	}
	data := buf[:n]
	if !utf8.Valid(data) && !f.Truncated || bytes.IndexByte(data, 0) >= 0 {
		f.Binary = true
		return f
	}
	f.Content = string(bytes.ToValidUTF8(data, nil))
	return f
}

// Destroy removes a run's workspace.
func (c *Container) Destroy(_ context.Context, tenantID, runID string) error {
	key := WorkspaceKey(tenantID, runID)
	lock := c.runLock(key)
	lock.Lock()
	defer lock.Unlock()
	return os.RemoveAll(filepath.Join(c.cfg.Root, key))
}

// gc removes workspaces untouched for WorkspaceTTL (at most once a minute).
func (c *Container) gc() {
	c.mu.Lock()
	if time.Since(c.lastGC) < time.Minute {
		c.mu.Unlock()
		return
	}
	c.lastGC = time.Now()
	c.mu.Unlock()
	entries, err := os.ReadDir(c.cfg.Root)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-c.cfg.WorkspaceTTL)
	for _, e := range entries {
		if !e.IsDir() || e.Type()&fs.ModeSymlink != 0 {
			continue
		}
		if info, err := e.Info(); err == nil && info.ModTime().Before(cutoff) {
			if err := os.RemoveAll(filepath.Join(c.cfg.Root, e.Name())); err == nil {
				c.cfg.Log.Info("sandbox workspace expired", "workspace", e.Name())
			}
		}
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func sortStrings(s []string) { sort.Strings(s) }

// runtimeFailure recognizes the runtime's own errors (exit 125): no daemon,
// missing image, bad flags.
func runtimeFailure(stderr string) bool {
	s := strings.TrimSpace(stderr)
	return strings.Contains(s, "Error response from daemon") || strings.HasPrefix(s, "docker:") ||
		strings.HasPrefix(s, "Error: ") || strings.Contains(s, "Cannot connect to the Docker daemon") ||
		strings.Contains(s, "failed to connect to the docker API")
}
