package localruntime

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// ResolveBins finds the bundled local harness executable. Release builds put
// it in Contents/Resources/local/bin (macOS) or <exe-dir>/resources/local/bin
// (Linux/Windows); development builds can override it with
// HARNESS_HARNESS_BIN.
func ResolveBins() (Config, error) {
	cfg := Config{HarnessBin: os.Getenv("HARNESS_HARNESS_BIN")}
	if cfg.HarnessBin != "" {
		return cfg, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return cfg, err
	}
	base := filepath.Join(filepath.Dir(exe), "resources", "local", "bin")
	if runtime.GOOS == "darwin" {
		base = filepath.Join(filepath.Dir(exe), "..", "Resources", "local", "bin")
	}
	cfg.HarnessBin = filepath.Join(base, "harness")
	if _, err := os.Stat(cfg.HarnessBin); err != nil {
		return cfg, fmt.Errorf("missing local harness %s: %w", cfg.HarnessBin, err)
	}
	return cfg, nil
}
