package localapp

import (
	"log/slog"
	"os"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/gateway"
)

// NewEvalGateway builds the model gateway from the environment, for tools
// that call a model outside a run (evaluation). real reports whether a
// provider other than the built-in rule-based model is configured; the
// returned cleanup removes its scratch directory.
func NewEvalGateway(log *slog.Logger) (gw *gateway.Gateway, real bool, cleanup func(), err error) {
	dir, err := os.MkdirTemp("", "harness-eval-*")
	if err != nil {
		return nil, false, nil, err
	}
	cleanup = func() { os.RemoveAll(dir) }
	g, err := buildGateway(&artifact.FS{Root: dir}, gateway.NewMemLedger(), log)
	if err != nil {
		cleanup()
		return nil, false, nil, err
	}
	return g, hasRealModel(os.Getenv), cleanup, nil
}
