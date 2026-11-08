// Package sandbox runs agent-requested commands in an isolated, per-run
// environment. A backend (container runtime today; gVisor, microVMs or a
// remote executor later) provides the isolation; this package defines the
// contract and the safety floor every backend must meet:
//
//   - one workspace per run, never shared across runs or tenants;
//   - no harness credentials or environment: only the variables a request
//     sets explicitly;
//   - no network unless the backend is configured otherwise;
//   - CPU, memory, process, time and output limits;
//   - non-root, no extra capabilities, read-only root file system.
package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

// Limits bound one execution.
type Limits struct {
	CPUs           float64       `json:"cpus"`
	MemoryMB       int           `json:"memory_mb"`
	Pids           int           `json:"pids"`
	Timeout        time.Duration `json:"timeout"`
	MaxOutputBytes int           `json:"max_output_bytes"` // per stream and per returned file
	TmpMB          int           `json:"tmp_mb"`
}

// DefaultLimits apply where a backend sets none.
var DefaultLimits = Limits{CPUs: 1, MemoryMB: 512, Pids: 128, Timeout: 60 * time.Second, MaxOutputBytes: 64 << 10, TmpMB: 64}

// MaxTimeout caps what a request may ask for.
const MaxTimeout = 10 * time.Minute

// Merge fills zero fields from def.
func (l Limits) Merge(def Limits) Limits {
	if l.CPUs <= 0 {
		l.CPUs = def.CPUs
	}
	if l.MemoryMB <= 0 {
		l.MemoryMB = def.MemoryMB
	}
	if l.Pids <= 0 {
		l.Pids = def.Pids
	}
	if l.Timeout <= 0 {
		l.Timeout = def.Timeout
	}
	if l.MaxOutputBytes <= 0 {
		l.MaxOutputBytes = def.MaxOutputBytes
	}
	if l.TmpMB <= 0 {
		l.TmpMB = def.TmpMB
	}
	return l
}

// Request is one command in a run's workspace.
type Request struct {
	TenantID string
	RunID    string
	Argv     []string
	Stdin    string
	// Files are written into the workspace before the command runs
	// (relative paths).
	Files map[string]string
	// Outputs are workspace files returned after the command (relative
	// paths).
	Outputs []string
	// Env is the command's entire environment besides what the image sets.
	Env     map[string]string
	Timeout time.Duration // 0: the backend's limit; never above it
}

// File is a workspace file returned to the caller.
type File struct {
	Path      string `json:"path"`
	Bytes     int64  `json:"bytes"`
	Content   string `json:"content,omitempty"` // UTF-8 text only
	Binary    bool   `json:"binary,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
	Missing   bool   `json:"missing,omitempty"`
}

// Result of one execution. A non-zero exit or a timeout is a result, not
// an error: the agent reads it as an observation.
type Result struct {
	ExitCode        int     `json:"exit_code"`
	Stdout          string  `json:"stdout"`
	Stderr          string  `json:"stderr"`
	StdoutTruncated bool    `json:"stdout_truncated,omitempty"`
	StderrTruncated bool    `json:"stderr_truncated,omitempty"`
	TimedOut        bool    `json:"timed_out,omitempty"`
	DurationMS      int64   `json:"duration_ms"`
	Files           []File  `json:"files,omitempty"`
	Environment     Info    `json:"environment"`
	Limits          Summary `json:"limits"`
}

// Summary is the limits as reported to the agent.
type Summary struct {
	CPUs      float64 `json:"cpus"`
	MemoryMB  int     `json:"memory_mb"`
	TimeoutMS int64   `json:"timeout_ms"`
}

// Info describes the isolation a backend provides.
type Info struct {
	Backend   string `json:"backend"`
	Isolation string `json:"isolation"` // container | gvisor | microvm | remote
	Image     string `json:"image,omitempty"`
	Network   string `json:"network"` // none | allowlist | full
	Workdir   string `json:"workdir"`
}

// Backend executes requests in isolation.
type Backend interface {
	Info() Info
	Exec(ctx context.Context, req Request) (Result, error)
	// Destroy removes a run's workspace.
	Destroy(ctx context.Context, tenantID, runID string) error
}

// ErrInvalid marks a request the backend refuses (bad paths, too large).
var ErrInvalid = errors.New("invalid sandbox request")

// Request bounds.
const (
	MaxFiles        = 64
	MaxFileBytes    = 1 << 20
	MaxInputBytes   = 4 << 20
	MaxArgvBytes    = 64 << 10
	MaxOutputsFiles = 16
)

// Validate checks a request before any backend touches it.
func (r Request) Validate() error {
	if r.TenantID == "" || r.RunID == "" {
		return fmt.Errorf("%w: tenant and run are required", ErrInvalid)
	}
	if len(r.Argv) == 0 || strings.TrimSpace(r.Argv[0]) == "" {
		return fmt.Errorf("%w: empty command", ErrInvalid)
	}
	n := 0
	for _, a := range r.Argv {
		n += len(a)
		if strings.ContainsRune(a, 0) {
			return fmt.Errorf("%w: NUL in command", ErrInvalid)
		}
	}
	if n > MaxArgvBytes {
		return fmt.Errorf("%w: command longer than %d bytes", ErrInvalid, MaxArgvBytes)
	}
	if len(r.Files) > MaxFiles {
		return fmt.Errorf("%w: more than %d files", ErrInvalid, MaxFiles)
	}
	total := len(r.Stdin)
	for p, c := range r.Files {
		if _, err := CleanPath(p); err != nil {
			return err
		}
		if len(c) > MaxFileBytes {
			return fmt.Errorf("%w: file %s larger than %d bytes", ErrInvalid, p, MaxFileBytes)
		}
		total += len(c)
	}
	if total > MaxInputBytes {
		return fmt.Errorf("%w: inputs larger than %d bytes", ErrInvalid, MaxInputBytes)
	}
	if len(r.Outputs) > MaxOutputsFiles {
		return fmt.Errorf("%w: more than %d outputs", ErrInvalid, MaxOutputsFiles)
	}
	for _, p := range r.Outputs {
		if _, err := CleanPath(p); err != nil {
			return err
		}
	}
	for k := range r.Env {
		if k == "" || strings.ContainsAny(k, "=\x00") {
			return fmt.Errorf("%w: bad environment name %q", ErrInvalid, k)
		}
	}
	return nil
}

// CleanPath turns a workspace path into a clean relative path, refusing
// anything that escapes the workspace.
func CleanPath(p string) (string, error) {
	p = strings.TrimPrefix(strings.TrimSpace(p), "/workspace/")
	if p == "" || strings.ContainsRune(p, 0) || strings.HasPrefix(p, "/") || strings.Contains(p, "\\") {
		return "", fmt.Errorf("%w: path %q must be relative to the workspace", ErrInvalid, p)
	}
	c := path.Clean(p)
	if c == "." || c == ".." || strings.HasPrefix(c, "../") {
		return "", fmt.Errorf("%w: path %q escapes the workspace", ErrInvalid, p)
	}
	return c, nil
}

// WorkspaceKey names a run's workspace without exposing ids in paths.
func WorkspaceKey(tenantID, runID string) string {
	h := sha256.Sum256([]byte(tenantID + "\x00" + runID))
	return hex.EncodeToString(h[:12])
}

// capped collects at most max bytes.
type capped struct {
	buf       []byte
	max       int
	truncated bool
}

func (c *capped) Write(p []byte) (int, error) {
	if room := c.max - len(c.buf); room > 0 {
		if len(p) > room {
			c.buf = append(c.buf, p[:room]...)
			c.truncated = true
		} else {
			c.buf = append(c.buf, p...)
		}
	} else if len(p) > 0 {
		c.truncated = true
	}
	return len(p), nil
}
