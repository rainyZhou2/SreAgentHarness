package localapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go-sre-harness/internal/localstore"
)

// Level of a doctor check.
type Level string

const (
	OK   Level = "ok"
	Warn Level = "warn"
	Fail Level = "FAIL"
	Info Level = "info"
)

// Check is one doctor finding.
type Check struct {
	Name   string
	Level  Level
	Detail string
	Fix    string // what to do (warn / fail only)
}

// DoctorOptions configures Doctor.
type DoctorOptions struct {
	DataDir string
	Addr    string // where `harness local` listens (to detect a running one)
	Getenv  func(string) string
	HTTP    *http.Client
	// ProbeModels contacts the configured model providers (default true).
	SkipModels bool
}

// Doctor inspects the local installation and returns its findings. It never
// modifies anything and never prints secrets.
func Doctor(ctx context.Context, o DoctorOptions) []Check {
	var cs []Check
	add := func(c Check) { cs = append(cs, c) }
	hc := o.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}

	// data directory
	st, err := os.Stat(o.DataDir)
	switch {
	case err != nil:
		add(Check{"data directory", Info, o.DataDir + " does not exist yet (created on first run)", ""})
	case runtime.GOOS != "windows" && st.Mode().Perm()&0o077 != 0:
		add(Check{"data directory", Warn, fmt.Sprintf("%s is accessible to other users (mode %v)", o.DataDir, st.Mode().Perm()),
			"chmod 700 " + o.DataDir})
	default:
		add(Check{"data directory", OK, o.DataDir, ""})
	}

	// credentials
	jsonPath, plainPath := filepath.Join(o.DataDir, tokenFileName), filepath.Join(o.DataDir, tokenPlain)
	if _, ok := readValidPair(jsonPath, plainPath); ok {
		c := Check{"api token", OK, plainPath, ""}
		if runtime.GOOS != "windows" {
			if pst, err := os.Stat(plainPath); err == nil && pst.Mode().Perm()&0o077 != 0 {
				c = Check{"api token", Warn, fmt.Sprintf("%s has mode %v", plainPath, pst.Mode().Perm()), "chmod 600 " + plainPath}
			}
		}
		add(c)
	} else if st != nil {
		add(Check{"api token", Warn, "no valid token pair (a new one is created on the next start)", "harness local"})
	}
	if runtime.GOOS != "windows" {
		if est, err := os.Stat(filepath.Join(o.DataDir, EnvFileName)); err == nil && est.Mode().Perm()&0o077 != 0 {
			add(Check{EnvFileName, Warn, "holds API keys but is readable by other users", "chmod 600 " + filepath.Join(o.DataDir, EnvFileName)})
		}
	}

	// database
	dbPath := filepath.Join(o.DataDir, "harness.db")
	if _, err := os.Stat(dbPath); err == nil {
		add(databaseCheck(dbPath))
	} else {
		add(Check{"database", Info, "not created yet", ""})
	}

	// running instance / port
	if rel, err := AcquireLock(o.DataDir); err != nil {
		running := Check{"server", Info, "a harness is running on this data directory", ""}
		if o.Addr != "" {
			if resp, err := hc.Get("http://" + o.Addr + "/healthz"); err == nil {
				resp.Body.Close()
				running.Detail = "running at http://" + o.Addr
			}
		}
		add(running)
	} else {
		rel()
		if o.Addr != "" {
			if l, err := net.Listen("tcp", o.Addr); err != nil {
				add(Check{"listen address", Fail, fmt.Sprintf("%s is not available: %v", o.Addr, err), "use -addr with a free port"})
			} else {
				l.Close()
				add(Check{"listen address", OK, o.Addr + " is free", ""})
			}
		}
	}

	// disk use
	if u, err := Usage(o.DataDir); err == nil && u.Total > 0 {
		parts := []string{}
		for _, a := range areas(u.ByArea) {
			if len(parts) == 3 {
				break
			}
			parts = append(parts, fmt.Sprintf("%s %s", a, FormatBytes(u.ByArea[a])))
		}
		c := Check{"disk use", OK, FormatBytes(u.Total) + " (" + strings.Join(parts, ", ") + ")", ""}
		if u.Total > 1<<30 {
			c.Level, c.Fix = Warn, "harness local prune -older-than 30d"
			c.Detail += ": the whole working set is loaded into memory at start, so a large database slows startup"
		}
		add(c)
	}

	// model providers
	provs := ConfiguredProviders(o.Getenv)
	if len(provs) == 0 {
		add(Check{"model", Info, "built-in rule-based model (no provider configured)", "harness local init"})
	}
	if !o.SkipModels {
		for _, p := range provs {
			if err := ProbeProvider(ctx, hc, p.Kind, p.BaseURL, p.Key); err != nil {
				add(Check{"model " + p.Kind, Fail, err.Error(), "harness local init"})
			} else {
				add(Check{"model " + p.Kind, OK, "reachable, key accepted", ""})
			}
		}
	}

	// optional integrations
	if u := o.Getenv("HARNESS_LOCAL_PROMETHEUS_URL"); u != "" {
		resp, err := hc.Get(strings.TrimRight(u, "/") + "/-/healthy")
		switch {
		case err != nil:
			add(Check{"prometheus", Fail, err.Error(), "check HARNESS_LOCAL_PROMETHEUS_URL"})
		default:
			resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				add(Check{"prometheus", OK, u, ""})
			} else {
				add(Check{"prometheus", Warn, fmt.Sprintf("%s answered HTTP %d on /-/healthy", u, resp.StatusCode), ""})
			}
		}
	}
	add(Check{"kubernetes", Info, "local mode has no Kubernetes tools yet, so there is no kubeconfig to check", ""})
	return cs
}

// databaseCheck opens the database read-only: schema version and a quick
// integrity check.
func databaseCheck(path string) Check {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(3000)")
	if err != nil {
		return Check{"database", Fail, err.Error(), "restore a backup: harness local restore FILE"}
	}
	defer db.Close()
	var res string
	if err := db.QueryRow(`PRAGMA quick_check`).Scan(&res); err != nil || res != "ok" {
		return Check{"database", Fail, fmt.Sprintf("integrity check failed: %v %s", err, res), "restore a backup: harness local restore FILE"}
	}
	ver := "1 (pre-versioning)"
	var v string
	if err := db.QueryRow(`SELECT value FROM meta WHERE key='schema_version'`).Scan(&v); err == nil {
		ver = v
	}
	if v != "" && v != fmt.Sprint(localstore.SchemaVersion) {
		var n int
		fmt.Sscan(v, &n)
		if n > localstore.SchemaVersion {
			return Check{"database", Fail, fmt.Sprintf("schema v%s is newer than this build (v%d)", v, localstore.SchemaVersion), "upgrade the harness"}
		}
		return Check{"database", Info, fmt.Sprintf("schema v%s will be upgraded to v%d on start (a backup copy is made first)", v, localstore.SchemaVersion), ""}
	}
	return Check{"database", OK, "schema v" + ver + ", integrity ok", ""}
}
