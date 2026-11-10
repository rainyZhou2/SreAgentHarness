package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/workbench"
	"go-sre-harness/internal/workeval"
)

const workBenchUsage = `usage:
  harness work-bench run <case.json|dir> [-baseline summary.json] [-out results.json] [-write-baseline summary.json]
  harness work-bench record -tenant <id> -work <id> [-out case.json]     (needs HARNESS_DATABASE_URL)
  harness work-bench replay -tenant <id> -work <id> [-out result.json]   (needs HARNESS_DATABASE_URL)
  harness work-bench load [-store sqlite|postgres] [-works N] [-events N] [-snapshots N] [-concurrency N]
                          [-replicas N] [-budget budget.json] [-out report.json]

run executes benchmark cases on a simulated clock against an in-memory
control plane and exits 1 when a case fails or the summary regresses
against the baseline. record exports a Work as a replayable case; replay
records it, runs it and compares the replay with the original scorecard.
load measures p50/p95/p99 of Work creation, event correlation, the wake
lag of due Works and world snapshots on the real clock, and exits 1 when a
series breaks the budget. -store postgres uses HARNESS_DATABASE_URL, which
must be a migrated scratch database: load writes under tenant "load".`

// WorkBench runs the work-bench subcommand and returns the exit code.
func WorkBench(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, workBenchUsage)
		return 2
	}
	fs := flag.NewFlagSet("work-bench "+args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	baseline := fs.String("baseline", "", "baseline summary JSON (run)")
	writeBaseline := fs.String("write-baseline", "", "write the run summary as a new baseline (run)")
	out := fs.String("out", "", "write the JSON output to this file instead of stdout")
	tenant := fs.String("tenant", "", "tenant id (record, replay)")
	workID := fs.String("work", "", "Work id (record, replay)")
	lo := workbench.DefaultLoadOptions()
	storeKind := fs.String("store", "sqlite", "load: sqlite (temporary file) or postgres (HARNESS_DATABASE_URL)")
	budget := fs.String("budget", "", "load: JSON budget, e.g. {\"work_create\":{\"p95_ms\":50}}")
	fs.IntVar(&lo.Works, "works", lo.Works, "load: Works to create")
	fs.IntVar(&lo.Events, "events", lo.Events, "load: events to ingest")
	fs.IntVar(&lo.Subjects, "subjects", lo.Subjects, "load: distinct subjects")
	fs.IntVar(&lo.Snapshots, "snapshots", lo.Snapshots, "load: snapshots to build")
	fs.IntVar(&lo.Concurrency, "concurrency", lo.Concurrency, "load: concurrent clients")
	fs.IntVar(&lo.Replicas, "replicas", lo.Replicas, "load: orchestrator replicas")
	cmd, rest := args[0], args[1:]
	var path string
	if cmd == "run" && len(rest) > 0 && rest[0] != "" && rest[0][0] != '-' {
		path, rest = rest[0], rest[1:]
	}
	if err := fs.Parse(rest); err != nil {
		return 2
	}
	emit := func(v any) error {
		b := workbench.MarshalIndent(v)
		if *out != "" {
			return os.WriteFile(*out, b, 0o600)
		}
		_, err := stdout.Write(b)
		return err
	}
	runner := &workbench.Runner{}
	switch cmd {
	case "run":
		if path == "" {
			fmt.Fprintln(stderr, workBenchUsage)
			return 2
		}
		cases, err := workbench.LoadCases(path)
		if err != nil {
			fmt.Fprintln(stderr, "work-bench:", err)
			return 2
		}
		var base *workeval.Summary
		if *baseline != "" {
			b, err := os.ReadFile(*baseline)
			if err != nil {
				fmt.Fprintln(stderr, "work-bench:", err)
				return 2
			}
			base = &workeval.Summary{}
			if err := json.Unmarshal(b, base); err != nil {
				fmt.Fprintln(stderr, "work-bench: baseline:", err)
				return 2
			}
		}
		suite, err := runner.RunSuite(ctx, cases, base)
		if err != nil {
			fmt.Fprintln(stderr, "work-bench:", err)
			return 1
		}
		for _, r := range suite.Results {
			mark := "PASS"
			if !r.Passed {
				mark = "FAIL"
			}
			fmt.Fprintf(stderr, "%s  %-36s status=%-10s gates=%v runs=%d sim=%.0fs\n", mark, r.Case, r.Status,
				r.Scorecard.FailedGates, r.Scorecard.Runs, r.SimulatedSeconds)
			for _, f := range r.Failures {
				fmt.Fprintf(stderr, "      - %s\n", f)
			}
		}
		for _, reg := range suite.Regressions {
			fmt.Fprintf(stderr, "REGRESSION  %s\n", reg)
		}
		fmt.Fprintf(stderr, "%d passed, %d failed\n", suite.Passed, suite.Failed)
		if err := emit(suite); err != nil {
			fmt.Fprintln(stderr, "work-bench:", err)
			return 1
		}
		if *writeBaseline != "" {
			if err := os.WriteFile(*writeBaseline, workbench.MarshalIndent(suite.Summary), 0o600); err != nil {
				fmt.Fprintln(stderr, "work-bench:", err)
				return 1
			}
		}
		if !suite.OK() {
			return 1
		}
		return 0
	case "load":
		return workBenchLoad(ctx, *storeKind, lo, *budget, getenv, stderr, emit)
	case "record", "replay":
		if *tenant == "" || *workID == "" {
			fmt.Fprintln(stderr, workBenchUsage)
			return 2
		}
		store, closeStore, err := openControlPlane(ctx, getenv)
		if err != nil {
			fmt.Fprintln(stderr, "work-bench:", err)
			return 2
		}
		defer closeStore()
		tctx := pg.WithTenant(ctx, *tenant)
		if cmd == "record" {
			c, err := workbench.Record(tctx, store, *tenant, *workID)
			if err != nil {
				fmt.Fprintln(stderr, "work-bench:", err)
				return 1
			}
			if err := emit(c); err != nil {
				fmt.Fprintln(stderr, "work-bench:", err)
				return 1
			}
			return 0
		}
		cmp, err := workbench.Replay(tctx, runner, store, &workeval.Evaluator{Ledger: store}, *tenant, *workID)
		if err != nil {
			fmt.Fprintln(stderr, "work-bench:", err)
			return 1
		}
		for _, d := range cmp.Diffs {
			fmt.Fprintln(stderr, "DIFF ", d)
		}
		for _, f := range cmp.Replay.Failures {
			fmt.Fprintln(stderr, "FAIL ", f)
		}
		if err := emit(cmp); err != nil {
			fmt.Fprintln(stderr, "work-bench:", err)
			return 1
		}
		if !cmp.Match {
			return 1
		}
		return 0
	}
	fmt.Fprintln(stderr, workBenchUsage)
	return 2
}

// openControlPlane opens the v2 store from HARNESS_DATABASE_URL (RLS on).
func openControlPlane(ctx context.Context, getenv func(string) string) (*cpsql.Store, func(), error) {
	url := getenv("HARNESS_DATABASE_URL")
	if url == "" {
		return nil, nil, errors.New("HARNESS_DATABASE_URL is required")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, nil, err
	}
	pg.ConfigureRLS(cfg)
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	return cpsql.OpenPostgres(pool), pool.Close, nil
}

func workBenchLoad(ctx context.Context, kind string, opt workbench.LoadOptions, budgetPath string,
	getenv func(string) string, stderr io.Writer, emit func(any) error) int {
	var b workbench.LoadBudget
	if budgetPath != "" {
		raw, err := os.ReadFile(budgetPath)
		if err == nil {
			err = json.Unmarshal(raw, &b)
		}
		if err != nil {
			fmt.Fprintln(stderr, "work-bench: budget:", err)
			return 2
		}
	}
	var store *cpsql.Store
	switch kind {
	case "sqlite":
		dir, err := os.MkdirTemp("", "harness-load-")
		if err != nil {
			fmt.Fprintln(stderr, "work-bench:", err)
			return 1
		}
		defer os.RemoveAll(dir)
		s, err := cpsql.OpenSQLite(filepath.Join(dir, "load.db"))
		if err != nil {
			fmt.Fprintln(stderr, "work-bench:", err)
			return 1
		}
		defer s.Close()
		store = s
	case "postgres":
		url := getenv("HARNESS_DATABASE_URL")
		if url == "" {
			fmt.Fprintln(stderr, "work-bench: HARNESS_DATABASE_URL is required for -store postgres")
			return 2
		}
		cfg, err := pgxpool.ParseConfig(url)
		if err != nil {
			fmt.Fprintln(stderr, "work-bench:", err)
			return 2
		}
		cfg.MaxConns = int32(max(opt.Concurrency, 1) + 4*max(opt.Replicas, 1) + 2)
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			fmt.Fprintln(stderr, "work-bench:", err)
			return 2
		}
		defer pool.Close()
		store = cpsql.OpenPostgres(pool)
	default:
		fmt.Fprintln(stderr, "work-bench: -store must be sqlite or postgres")
		return 2
	}
	rep, err := workbench.Load(ctx, store, kind, opt)
	if err != nil {
		fmt.Fprintln(stderr, "work-bench:", err)
		return 1
	}
	rep.Violations = b.Check(rep)
	for _, name := range []string{workbench.LoadWorkCreate, workbench.LoadCorrelation, workbench.LoadWakeLag, workbench.LoadSnapshot} {
		p := rep.Metrics[name]
		fmt.Fprintf(stderr, "%-12s n=%-6d p50=%7.2fms p95=%7.2fms p99=%7.2fms max=%7.2fms %6.0f/s errors=%d\n",
			name, p.Count, p.P50, p.P95, p.P99, p.Max, p.PerSecond, p.Errors)
	}
	for _, v := range rep.Violations {
		fmt.Fprintln(stderr, "BUDGET ", v)
	}
	if err := emit(rep); err != nil {
		fmt.Fprintln(stderr, "work-bench:", err)
		return 1
	}
	if len(rep.Violations) > 0 {
		return 1
	}
	for _, p := range rep.Metrics {
		if p.Errors > 0 {
			return 1
		}
	}
	return 0
}
