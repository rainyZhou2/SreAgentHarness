package localruntime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

// startPostgres initialises the data directory on first use and starts a
// loopback-only PostgreSQL. It returns a URL the harness core can use.
func (m *Manager) startPostgres(ctx context.Context) (string, error) {
	data := filepath.Join(m.cfg.DataDir, "pgdata")
	if _, err := os.Stat(filepath.Join(data, "PG_VERSION")); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(data, 0o700); err != nil {
			return "", err
		}
		args := []string{"-D", data, "-U", "harness", "--auth=trust", "--encoding=UTF8", "--no-locale"}
		if m.cfg.PgShareDir != "" {
			args = append(args, "-L", m.cfg.PgShareDir)
		}
		cmd := exec.CommandContext(ctx, m.cfg.InitDBBin, args...)
		cmd.Env = m.pgEnv()
		cmd.Stdout = m.logWriter("initdb.log")
		cmd.Stderr = cmd.Stdout
		if err := cmd.Run(); err != nil {
			return "", fmt.Errorf("initdb: %w", err)
		}
	}
	sock := filepath.Join(m.cfg.DataDir, "pgsock")
	if err := os.MkdirAll(sock, 0o700); err != nil {
		return "", err
	}
	port, err := freePort()
	if err != nil {
		return "", err
	}
	pg := exec.CommandContext(ctx, m.cfg.PostgresBin, "-D", data, "-h", "127.0.0.1",
		"-p", strconv.Itoa(port), "-k", sock)
	pg.Env = m.pgEnv()
	pg.Stdout = m.logWriter("postgres.log")
	pg.Stderr = pg.Stdout
	if err := pg.Start(); err != nil {
		return "", fmt.Errorf("start postgres: %w", err)
	}
	m.mu.Lock()
	m.pg, m.pgPort = pg, port
	m.mu.Unlock()
	if err := waitTCP(ctx, fmt.Sprintf("127.0.0.1:%d", port), m.cfg.StartupTimeout); err != nil {
		_ = m.stopPostgres()
		return "", err
	}
	return fmt.Sprintf("postgres://harness@127.0.0.1:%d/postgres?sslmode=disable", port), nil
}

func (m *Manager) pgEnv() []string {
	env := os.Environ()
	if m.cfg.PgLibDir != "" {
		key := "LD_LIBRARY_PATH"
		if runtime.GOOS == "darwin" {
			key = "DYLD_LIBRARY_PATH"
		}
		env = append(env, key+"="+m.cfg.PgLibDir)
	}
	if m.cfg.PgShareDir != "" {
		env = append(env, "PGSHAREDIR="+m.cfg.PgShareDir)
	}
	return env
}

func (m *Manager) stopPostgres() error {
	m.mu.Lock()
	pg := m.pg
	m.pg = nil
	m.mu.Unlock()
	if pg == nil || pg.Process == nil {
		return nil
	}
	_ = pg.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = pg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = pg.Process.Kill()
		<-done
	}
	return nil
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func waitTCP(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}
