package app

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/localapp"
)

const localSubUsage = `usage: harness local [flags]          run the local harness
       harness local init              set up a model provider (writes local.env, tests the key)
       harness local doctor            check the installation
       harness local backup [-out F]   write a backup archive (no credentials inside)
       harness local restore FILE      restore a backup (the old data is kept aside)
       harness local prune             delete old replay-only artifacts (-older-than 30d, -yes to apply)
`

func runLocalSub(ctx context.Context, sub string, args []string, getenv func(string) string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("local "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	data := fs.String("data", localapp.DataDirFromEnv(getenv), "data directory")
	switch sub {
	case "init":
		provider := fs.String("provider", "", "anthropic | openai | gemini | none (default: ask)")
		if fs.Parse(args) != nil {
			return 2
		}
		return localInit(ctx, *data, *provider, getenv, stdin, stdout, stderr)
	case "doctor":
		addr := fs.String("addr", firstNonEmpty(getenv("HARNESS_HTTP_ADDR"), defaultLocalAddr), "address the local harness listens on")
		offline := fs.Bool("offline", false, "do not contact model providers")
		if fs.Parse(args) != nil {
			return 2
		}
		return localDoctor(ctx, *data, *addr, *offline, getenv, stdout)
	case "backup":
		out := fs.String("out", "", "archive path (default: ./harness-backup-<time>.tar.gz)")
		if fs.Parse(args) != nil {
			return 2
		}
		if *out == "" {
			*out = "harness-backup-" + time.Now().UTC().Format("20060102T150405Z") + ".tar.gz"
		}
		m, err := localapp.Backup(*data, *out)
		if err != nil {
			fmt.Fprintln(stderr, "harness local backup:", err)
			return 1
		}
		fmt.Fprintf(stdout, "backup written: %s (%d files, %s of data; no tokens or API keys inside)\n", *out, m.Files, localapp.FormatBytes(m.Bytes))
		return 0
	case "restore":
		if fs.Parse(args) != nil || fs.NArg() != 1 {
			fmt.Fprintln(stderr, "usage: harness local restore [-data DIR] FILE")
			return 2
		}
		prev, m, err := localapp.Restore(*data, fs.Arg(0))
		if err != nil {
			fmt.Fprintln(stderr, "harness local restore:", err)
			return 1
		}
		fmt.Fprintf(stdout, "restored %s (made %s)\n", *data, m.CreatedAt.Format(time.RFC3339))
		if prev != "" {
			fmt.Fprintf(stdout, "previous data kept at %s (delete it when you no longer need it)\n", prev)
		}
		return 0
	case "prune":
		age := fs.String("older-than", "30d", "delete replay-only artifacts older than this (e.g. 30d, 72h)")
		yes := fs.Bool("yes", false, "apply; without it only report what would be deleted")
		if fs.Parse(args) != nil {
			return 2
		}
		d, err := parseAge(*age)
		if err != nil {
			fmt.Fprintln(stderr, "harness local prune:", err)
			return 2
		}
		r, err := localapp.Prune(*data, d, !*yes)
		if err != nil {
			fmt.Fprintln(stderr, "harness local prune:", err)
			return 1
		}
		verb := "deleted"
		if r.DryRun {
			verb = "would delete"
		}
		fmt.Fprintf(stdout, "%s %d files (%s): model prompts, raw responses and manifests older than %s. Results, steps, tool output, memories and event logs are kept.\n",
			verb, r.Files, localapp.FormatBytes(r.Bytes), *age)
		if r.DryRun && r.Files > 0 {
			fmt.Fprintln(stdout, "re-run with -yes to apply")
		}
		return 0
	}
	fmt.Fprintf(stderr, "harness local: unknown command %q\n%s", sub, localSubUsage)
	return 2
}

// parseAge accepts Go durations and a "d" (days) suffix.
func parseAge(s string) (time.Duration, error) {
	if n, ok := strings.CutSuffix(s, "d"); ok {
		days, err := strconv.Atoi(n)
		if err != nil || days <= 0 {
			return 0, fmt.Errorf("invalid age %q", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid age %q (use 30d or 72h)", s)
	}
	return d, nil
}

func localDoctor(ctx context.Context, data, addr string, offline bool, getenv func(string) string, out io.Writer) int {
	// Settings in local.env apply to the doctor as they do to the server.
	_, _ = localapp.LoadEnvFile(data)
	checks := localapp.Doctor(ctx, localapp.DoctorOptions{DataDir: data, Addr: addr, Getenv: os.Getenv, SkipModels: offline})
	fails := 0
	for _, c := range checks {
		fmt.Fprintf(out, "[%-4s] %-16s %s\n", c.Level, c.Name, c.Detail)
		if c.Fix != "" && (c.Level == localapp.Warn || c.Level == localapp.Fail) {
			fmt.Fprintf(out, "       fix: %s\n", c.Fix)
		}
		if c.Level == localapp.Fail {
			fails++
		}
	}
	if fails > 0 {
		fmt.Fprintf(out, "\n%d problem(s) found\n", fails)
		return 1
	}
	return 0
}

func localInit(ctx context.Context, data, provider string, getenv func(string) string, stdin io.Reader, stdout, stderr io.Writer) int {
	in := bufio.NewReader(stdin)
	interactive := isTerminal(stdin)
	ask := func(prompt, def string) string {
		if def != "" {
			fmt.Fprintf(stdout, "%s [%s]: ", prompt, def)
		} else {
			fmt.Fprintf(stdout, "%s: ", prompt)
		}
		line, _ := in.ReadString('\n')
		if line = strings.TrimSpace(line); line != "" {
			return line
		}
		return def
	}
	secret := func(prompt string) string {
		fmt.Fprintf(stdout, "%s: ", prompt)
		if interactive {
			restore := hideEcho()
			defer func() { restore(); fmt.Fprintln(stdout) }()
		}
		line, _ := in.ReadString('\n')
		return strings.TrimSpace(line)
	}
	if provider == "" {
		fmt.Fprintln(stdout, "Model provider:\n  1) Anthropic\n  2) OpenAI-compatible (OpenAI, DeepSeek, vLLM, Ollama, ...)\n  3) Gemini\n  4) none (built-in rule-based model, works offline)")
		switch ask("choose", "1") {
		case "1", "anthropic":
			provider = localapp.ProviderAnthropic
		case "2", "openai":
			provider = localapp.ProviderOpenAI
		case "3", "gemini":
			provider = localapp.ProviderGemini
		default:
			provider = "none"
		}
	}
	upd := map[string]string{}
	var probe *localapp.Provider
	// A key already in the environment is offered first, so it never has to
	// be typed or put on a command line.
	keyFor := func(env, label string) string {
		if k := getenv(env); k != "" {
			if strings.HasPrefix(strings.ToLower(ask("use "+env+" from the environment? (y/n)", "y")), "y") {
				return k
			}
		}
		return secret(label)
	}
	switch provider {
	case localapp.ProviderAnthropic:
		k := keyFor("ANTHROPIC_API_KEY", "Anthropic API key")
		upd["ANTHROPIC_API_KEY"] = k
		probe = &localapp.Provider{Kind: provider, BaseURL: getenv("HARNESS_ANTHROPIC_BASE_URL"), Key: k}
	case localapp.ProviderGemini:
		k := keyFor("GEMINI_API_KEY", "Gemini API key")
		upd["GEMINI_API_KEY"] = k
		probe = &localapp.Provider{Kind: provider, BaseURL: getenv("HARNESS_GEMINI_BASE_URL"), Key: k}
	case localapp.ProviderOpenAI:
		base := ask("base URL (including /v1)", firstNonEmpty(getenv("HARNESS_OPENAI_BASE_URL"), "https://api.openai.com/v1"))
		k := keyFor("HARNESS_OPENAI_API_KEY", "API key (empty if the endpoint needs none)")
		model := ask("model name", getenv("HARNESS_OPENAI_MODEL"))
		upd["HARNESS_OPENAI_BASE_URL"], upd["HARNESS_OPENAI_API_KEY"], upd["HARNESS_OPENAI_MODEL"] = base, k, model
		probe = &localapp.Provider{Kind: provider, BaseURL: base, Key: k}
	case "none":
	default:
		fmt.Fprintf(stderr, "harness local init: unknown provider %q\n", provider)
		return 2
	}
	if probe != nil {
		if probe.Key == "" && probe.Kind != localapp.ProviderOpenAI {
			fmt.Fprintln(stderr, "harness local init: an API key is required")
			return 2
		}
		fmt.Fprint(stdout, "testing the connection… ")
		if err := localapp.ProbeProvider(ctx, nil, probe.Kind, probe.BaseURL, probe.Key); err != nil {
			fmt.Fprintln(stdout, "failed")
			fmt.Fprintln(stderr, "harness local init:", err)
			fmt.Fprintln(stderr, "nothing was saved; check the key and endpoint and run init again")
			return 1
		}
		fmt.Fprintln(stdout, "ok")
	}
	if prom := ask("Prometheus URL for read-only metrics (empty to skip)", getenv("HARNESS_LOCAL_PROMETHEUS_URL")); prom != "" {
		upd["HARNESS_LOCAL_PROMETHEUS_URL"] = prom
	}
	if err := localapp.SaveEnv(data, upd); err != nil {
		fmt.Fprintln(stderr, "harness local init:", err)
		return 1
	}
	fmt.Fprintf(stdout, "saved to %s (mode 0600)\nnext: harness local   then, in another terminal: harness chat\n", filepath.Join(data, localapp.EnvFileName))
	return 0
}

func isTerminal(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

// hideEcho turns terminal echo off (stty, so no extra dependency) and returns
// the function that restores it.
func hideEcho() func() {
	off := exec.Command("stty", "-echo")
	off.Stdin = os.Stdin
	if off.Run() != nil {
		return func() {}
	}
	return func() {
		on := exec.Command("stty", "echo")
		on.Stdin = os.Stdin
		_ = on.Run()
	}
}
