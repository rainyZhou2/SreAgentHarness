package localapp

import (
	"log/slog"
	"os"
	"strings"

	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/netguard"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/sretools"
	"go-sre-harness/internal/tools/web"
)

// registerLocalTools adds the read-only tools a local run needs to be useful
// out of the box, next to the demo tools:
//
//   - web.fetch (R0): reads a public page as untrusted text. Loopback,
//     link-local and cloud-metadata addresses stay blocked. Disable with
//     HARNESS_LOCAL_WEB=off.
//   - prom.query / prom.alerts (R0): only when HARNESS_LOCAL_PROMETHEUS_URL
//     is set.
//
// Nothing here has a side effect, so no approval rule is relaxed.
func registerLocalTools(reg *tools.Registry, getenv func(string) string, log *slog.Logger) {
	if !strings.EqualFold(getenv("HARNESS_LOCAL_WEB"), "off") {
		reg.Register(web.NewFetchTool(web.FetchConfig{Policy: netguard.NetPolicy{}}))
	}
	if url := getenv("HARNESS_LOCAL_PROMETHEUS_URL"); url != "" {
		p, err := sretools.NewPrometheus(sretools.PrometheusConfig{URL: url})
		if err != nil {
			log.Warn("prometheus tools disabled", "err", err)
			return
		}
		for _, t := range p.Tools() {
			reg.Register(t)
		}
	}
}

// localCompactor returns the model-backed compactor when a real model
// provider is configured. With only the built-in rule-based model it keeps
// the default deterministic compactor: that model cannot write summaries.
// HARNESS_LOCAL_LLM_COMPACT=off forces the deterministic one.
func localCompactor(g kernel.Generator, log *slog.Logger) kernel.Compactor {
	if strings.EqualFold(os.Getenv("HARNESS_LOCAL_LLM_COMPACT"), "off") || !hasRealModel(os.Getenv) {
		return nil // the worker default (deterministic)
	}
	return &kernel.LLMCompactor{Gateway: g, Alias: "sre-reasoning", Log: log.With("component", "compactor")}
}

func hasRealModel(getenv func(string) string) bool {
	return getenv("ANTHROPIC_API_KEY") != "" || getenv("HARNESS_OPENAI_BASE_URL") != "" || getenv("GEMINI_API_KEY") != ""
}
