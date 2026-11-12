package app

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"go-sre-harness/internal/compacteval"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/localapp"
)

// RunCompactEval is `harness compact-eval`: it folds a fixed set of
// realistic investigation histories with each compactor and reports what
// survives (see package compacteval). With a model configured
// (ANTHROPIC_API_KEY, GEMINI_API_KEY or HARNESS_OPENAI_BASE_URL, or
// `harness local init`) the model-backed compactor is measured for real.
// Without one, only the deterministic baseline and the model's input
// coverage (an upper bound on its recall) are reported, and the output says
// so: it never presents a number it did not measure.
func RunCompactEval(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("compact-eval", flag.ContinueOnError)
	fs.SetOutput(stderr)
	data := fs.String("data", localapp.DataDirFromEnv(os.Getenv), "read settings from <data>/local.env")
	alias := fs.String("alias", "sre-reasoning", "model alias used for summaries")
	asJSON := fs.Bool("json", false, "machine-readable output")
	runs := fs.Int("runs", 1, "repeat the model-backed compactor this many times (shows variance)")
	if fs.Parse(args) != nil {
		return 2
	}
	_, _ = localapp.LoadEnvFile(*data)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	g, real, cleanup, err := localapp.NewEvalGateway(log)
	if err != nil {
		fmt.Fprintln(stderr, "harness compact-eval:", err)
		return 1
	}
	defer cleanup()

	cases := compacteval.DefaultCases()
	type row struct {
		Compactor   string                `json:"compactor"`
		ModelCalled bool                  `json:"model_called"`
		Summary     compacteval.Summary   `json:"summary"`
		Cases       []compacteval.Result  `json:"cases"`
		Runs        []compacteval.Summary `json:"runs,omitempty"`
	}
	det := compacteval.Run(ctx, kernel.DeterministicCompactor{}, cases)
	rows := []row{{Compactor: "deterministic", Summary: compacteval.Summarize(det), Cases: det}}

	llm := row{Compactor: "llm:" + *alias, ModelCalled: real}
	for i := 0; i < max(*runs, 1); i++ {
		c := &kernel.LLMCompactor{Alias: *alias, Log: log}
		if real {
			c.Gateway = g
		}
		llm.Cases = compacteval.Run(ctx, c, cases)
		llm.Runs = append(llm.Runs, compacteval.Summarize(llm.Cases))
		if !real {
			break // nothing varies without a model
		}
	}
	llm.Summary = llm.Runs[len(llm.Runs)-1]
	rows = append(rows, llm)

	if *asJSON {
		b, _ := json.MarshalIndent(map[string]any{"model_configured": real, "results": rows}, "", "  ")
		fmt.Fprintln(stdout, string(b))
		return 0
	}
	pct := func(v float64) string { return fmt.Sprintf("%3.0f%%", v*100) }
	fmt.Fprintf(stdout, "%-18s %12s %15s %10s %12s %6s\n", "compactor", "fact recall", "model input", "citations", "size ratio", "leaks")
	for _, r := range rows {
		recall, cov := pct(r.Summary.FactRecall), "-"
		if r.Summary.HasCoverage {
			cov = pct(r.Summary.InputCoverage)
		}
		if r.Compactor != "deterministic" && !r.ModelCalled {
			recall = "n/a"
		}
		cite, ratio := pct(r.Summary.CitationsKept), fmt.Sprintf("%.0f%%", r.Summary.Ratio*100)
		if r.Compactor != "deterministic" && !r.ModelCalled {
			cite, ratio = "n/a", "n/a" // the fallback's numbers, not the model's
		}
		fmt.Fprintf(stdout, "%-18s %12s %15s %10s %12s %6d\n", r.Compactor, recall, cov, cite, ratio, r.Summary.Leaks)
	}
	fmt.Fprintln(stdout, "\nfact recall: key facts still in the summary. model input: key facts present in what the model is shown (an upper bound on its recall).")
	if !real {
		fmt.Fprintln(stdout, "\nno model is configured, so the model-backed compactor was NOT run: its recall is not measured. Run `harness local init`")
		fmt.Fprintln(stdout, "(or set ANTHROPIC_API_KEY / GEMINI_API_KEY / HARNESS_OPENAI_BASE_URL) and re-run to measure it.")
	} else if len(llm.Runs) > 1 {
		lo, hi := llm.Runs[0].FactRecall, llm.Runs[0].FactRecall
		for _, s := range llm.Runs {
			lo, hi = min(lo, s.FactRecall), max(hi, s.FactRecall)
		}
		fmt.Fprintf(stdout, "\nmodel recall over %d runs: %s to %s\n", len(llm.Runs), pct(lo), pct(hi))
	}
	fmt.Fprintln(stdout, "\nper case:")
	for _, r := range rows {
		for _, c := range r.Cases {
			if c.Err != nil {
				fmt.Fprintf(stdout, "  %-18s %-22s error: %v\n", r.Compactor, c.Case, c.Err)
				continue
			}
			miss := ""
			if len(c.Missing) > 0 && (r.ModelCalled || r.Compactor == "deterministic") {
				miss = "  missing: " + fmt.Sprint(c.Missing)
			}
			if len(c.Leaked) > 0 {
				miss += "  LEAKED: " + fmt.Sprint(c.Leaked)
			}
			if r.Compactor != "deterministic" && !r.ModelCalled {
				fmt.Fprintf(stdout, "  %-18s %-22s model input %s\n", r.Compactor, c.Case, pct(c.InputCoverage))
				continue
			}
			fmt.Fprintf(stdout, "  %-18s %-22s recall %s%s\n", r.Compactor, c.Case, pct(c.FactRecall), miss)
		}
	}
	for _, r := range rows {
		if r.Summary.Leaks > 0 || r.Summary.Errors > 0 {
			return 1 // a leak or an error is a failure, not a score
		}
	}
	return 0
}
