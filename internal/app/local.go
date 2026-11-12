package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"

	"go-sre-harness/internal/localapp"
)

// defaultLocalAddr is where `harness local` listens: loopback only.
const defaultLocalAddr = "127.0.0.1:8080"

// RunLocal starts the single-process local harness (SQLite, no PostgreSQL,
// NATS or config file needed):
//
//	harness local [-data DIR] [-addr HOST:PORT]
//	harness local init|doctor|backup|restore|prune   (see runLocalSub)
//
// It creates the data directory and a private bearer token on first run and
// prints how to call the API. Without a model API key the built-in
// rule-based model answers, so the full path works offline.
func RunLocal(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer, log *slog.Logger) int {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		return runLocalSub(ctx, args[0], args[1:], getenv, os.Stdin, stdout, stderr)
	}
	fs := flag.NewFlagSet("local", flag.ContinueOnError)
	fs.SetOutput(stderr)
	data := fs.String("data", localapp.DataDirFromEnv(getenv), "data directory (database, artifacts, token)")
	addr := fs.String("addr", firstNonEmpty(getenv("HARNESS_HTTP_ADDR"), defaultLocalAddr), "listen address (loopback only unless HARNESS_LOCAL_ALLOW_REMOTE=true)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if !isLoopback(*addr) && !strings.EqualFold(getenv("HARNESS_LOCAL_ALLOW_REMOTE"), "true") {
		fmt.Fprintf(stderr, "harness local: refusing to listen on %q: local mode is for one user; "+
			"use a loopback address, or set HARNESS_LOCAL_ALLOW_REMOTE=true to override\n", *addr)
		return 2
	}
	if set, err := localapp.LoadEnvFile(*data); err != nil {
		fmt.Fprintln(stderr, "harness local:", err)
		return 1
	} else if len(set) > 0 {
		fmt.Fprintf(stdout, "settings from %s: %s\n", filepath.Join(*data, localapp.EnvFileName), strings.Join(set, ", "))
	}
	// Take the data directory first: a second instance must fail before it
	// prints a banner that claims it is serving.
	release, err := localapp.AcquireLock(*data)
	if err != nil {
		fmt.Fprintln(stderr, "harness local:", err)
		return 1
	}
	defer release()
	tokenFile, _, err := localapp.EnsureToken(*data)
	if err != nil {
		fmt.Fprintln(stderr, "harness local:", err)
		return 1
	}
	cfg := localapp.Config{DataDir: *data, DBPath: filepath.Join(*data, "harness.db"),
		HTTPAddr: *addr, TokenFile: tokenFile, Log: log, LockHeld: true,
		OnReady: func() { fmt.Fprint(stdout, localBanner(*addr, *data, getenv)) }}
	if err := localapp.Run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(stderr, "harness local:", err)
		return 1
	}
	return 0
}

func localBanner(addr, data string, getenv func(string) string) string {
	model := "built-in rule-based model (no API key set; set ANTHROPIC_API_KEY, GEMINI_API_KEY or HARNESS_OPENAI_BASE_URL for a real model)"
	switch {
	case getenv("ANTHROPIC_API_KEY") != "":
		model = "Anthropic (ANTHROPIC_API_KEY)"
	case getenv("HARNESS_OPENAI_BASE_URL") != "":
		model = "OpenAI-compatible (HARNESS_OPENAI_BASE_URL)"
	case getenv("GEMINI_API_KEY") != "":
		model = "Gemini (GEMINI_API_KEY)"
	}
	tokenPath := filepath.Join(data, "token")
	return fmt.Sprintf(`harness local
  api:    http://%[1]s
  data:   %[2]s
  token:  %[3]s   (mode 0600; do not share)
  model:  %[4]s

try it:
  curl -H "Authorization: Bearer $(cat %[3]s)" http://%[1]s/v1/whoami

`, addr, data, tokenPath, model)
}

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}
