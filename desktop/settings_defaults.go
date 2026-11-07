package main

import (
	"os"
	"path/filepath"

	"go-sre-harness/desktop/harness"
)

// defaultSettings is the first-run configuration. Local mode is the default:
// the app starts its own core instead of asking for a server URL.
func defaultSettings() Settings {
	return Settings{
		Mode:         "local",
		LocalDataDir: defaultLocalDataDir(),
		ServerURL:    "http://127.0.0.1:8080",
		Creds:        harness.Credentials{Mode: "dev", Tenant: "tenant-a", Actor: "alice", Roles: "operator,approver"},
	}
}

func defaultLocalDataDir() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		dir = os.TempDir()
	}
	return filepath.Join(dir, "sre-harness-desktop", "local")
}
