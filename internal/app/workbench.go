package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

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

run executes benchmark cases on a simulated clock against an in-memory
control plane and exits 1 when a case fails or the summary regresses
against the baseline. record exports a Work as a replayable case; replay
records it, runs it and compares the replay with the original scorecard.`

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
