// Command harness-local runs the single-process, SQLite-backed harness used by
// the desktop local runtime. It intentionally has no PostgreSQL, NATS or
// plugin-host dependency.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"go-sre-harness/internal/localapp"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dataDir := os.Getenv("HARNESS_LOCAL_DATA")
	if dataDir == "" {
		if dir, err := os.UserConfigDir(); err == nil {
			dataDir = filepath.Join(dir, "sre-harness-desktop", "local")
		} else {
			dataDir = filepath.Join(os.TempDir(), "sre-harness-local")
		}
	}
	cfg := localapp.Config{
		DataDir:   dataDir,
		DBPath:    os.Getenv("HARNESS_LOCAL_DB"),
		HTTPAddr:  os.Getenv("HARNESS_HTTP_ADDR"),
		TokenFile: os.Getenv("HARNESS_AUTH_TOKENS_FILE"),
		Log:       log,
	}
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join(dataDir, "harness.db")
	}
	if err := localapp.Run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "harness-local:", err)
		os.Exit(1)
	}
}
