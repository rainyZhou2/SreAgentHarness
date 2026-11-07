package localruntime

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// ResolveBins finds the bundled harness and PostgreSQL executables. Release
// builds place them in Contents/Resources/local/bin (macOS) or
// <exe-dir>/resources/local/bin (Linux/Windows). Development builds can point
// at any binaries with HARNESS_HARNESS_BIN / HARNESS_PG_BIN / HARNESS_INITDB_BIN.
func firstEnv(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func ResolveBins() (Config, error) {
	cfg := Config{
		HarnessBin:  os.Getenv("HARNESS_HARNESS_BIN"),
		PostgresBin: os.Getenv("HARNESS_PG_BIN"),
		InitDBBin:   os.Getenv("HARNESS_INITDB_BIN"),
	}
	if cfg.HarnessBin != "" && cfg.PostgresBin != "" && cfg.InitDBBin != "" {
		return cfg, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return cfg, err
	}
	root := filepath.Join(filepath.Dir(exe), "resources", "local")
	if runtime.GOOS == "darwin" {
		root = filepath.Join(filepath.Dir(exe), "..", "Resources", "local")
	}
	base := filepath.Join(root, "bin")
	cfg.PgLibDir = firstEnv("HARNESS_PG_LIB_DIR", filepath.Join(root, "lib"))
	share := filepath.Join(root, "share")
	if _, err := os.Stat(filepath.Join(share, "postgresql@16", "postgres.bki")); err == nil {
		share = filepath.Join(share, "postgresql@16")
	}
	cfg.PgShareDir = firstEnv("HARNESS_PG_SHARE_DIR", share)
	if cfg.HarnessBin == "" {
		cfg.HarnessBin = filepath.Join(base, "harness")
	}
	if cfg.PostgresBin == "" {
		cfg.PostgresBin = filepath.Join(base, "postgres")
	}
	if cfg.InitDBBin == "" {
		cfg.InitDBBin = filepath.Join(base, "initdb")
	}
	for _, p := range []string{cfg.HarnessBin, cfg.PostgresBin, cfg.InitDBBin} {
		if _, err := os.Stat(p); err != nil {
			return cfg, fmt.Errorf("missing local binary %s: %w", p, err)
		}
	}
	return cfg, nil
}
