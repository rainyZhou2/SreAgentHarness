package localruntime

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// startCore runs the existing harness in combined API+worker+scheduler mode.
func (m *Manager) startCore(ctx context.Context, dbURL, tokenFile string) (string, error) {
	port, err := freePort()
	if err != nil {
		return "", err
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	values := map[string]string{
		"HARNESS_DATABASE_URL":            dbURL,
		"HARNESS_HTTP_ADDR":               addr,
		"HARNESS_AUTH_MODE":               "token",
		"HARNESS_AUTH_TOKENS_FILE":        tokenFile,
		"HARNESS_DEFAULT_TENANT":          "local",
		"HARNESS_DEFAULT_PROFILE":         "sre-diagnosis",
		"HARNESS_DEFAULT_PROFILE_VERSION": "7",
		"HARNESS_ARTIFACT_DIR":            filepath.Join(m.cfg.DataDir, "artifacts"),
		"HARNESS_AUTO_MIGRATE":            "true",
		"HARNESS_WORKER_MAX_INFLIGHT":     "8",
		"HARNESS_SSE_MODE":                "poll",
		"HARNESS_NATS_URL":                "",
	}
	for k, v := range m.cfg.ModelEnv {
		if v != "" {
			values[k] = v
		}
	}
	cmd := exec.CommandContext(ctx, m.cfg.HarnessBin, "all")
	cmd.Env = mergeEnv(os.Environ(), values)
	cmd.Stdout = m.logWriter("core.log")
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("start harness core: %w", err)
	}
	m.mu.Lock()
	m.core = cmd
	m.mu.Unlock()
	base := "http://" + addr
	if err := waitHTTP(ctx, base+"/healthz", m.cfg.StartupTimeout); err != nil {
		_ = m.stopCore()
		return "", err
	}
	return base, nil
}

func (m *Manager) stopCore() error {
	m.mu.Lock()
	cmd := m.core
	m.core = nil
	m.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
	return nil
}

// mergeEnv overrides existing keys instead of appending duplicate HARNESS_*
// entries, whose lookup order differs between platforms.
func mergeEnv(base []string, values map[string]string) []string {
	out := make([]string, 0, len(base)+len(values))
	for _, kv := range base {
		k, _, _ := strings.Cut(kv, "=")
		if _, replaced := values[k]; replaced {
			continue
		}
		out = append(out, kv)
	}
	for k, v := range values {
		out = append(out, k+"="+v)
	}
	return out
}

func waitHTTP(ctx context.Context, url string, timeout time.Duration) error {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err == nil {
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout waiting for %s", url)
}
