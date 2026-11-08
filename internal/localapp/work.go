package localapp

import (
	"context"
	"os"
	"path/filepath"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/workengine"
)

type localWork struct {
	engine *workengine.Engine
	store  *cpsql.Store
}

func (w *localWork) close() { _ = w.store.Close() }

// buildLocalWork assembles the v2 control plane on a separate SQLite file
// (<data dir>/controlplane.db) when HARNESS_WORK_ENGINE_ENABLED is set.
func buildLocalWork(ctx context.Context, cfg Config, local LocalFile, server *api.Server, pol policy.Gate,
	metrics *obs.Metrics) (*localWork, error) {
	flags := workengine.FlagsFromEnv(os.Getenv)
	if !flags.Engine {
		return nil, nil
	}
	path := os.Getenv("HARNESS_CONTROLPLANE_DB")
	if path == "" {
		path = filepath.Join(cfg.DataDir, "controlplane.db")
	}
	store, err := cpsql.OpenSQLite(path)
	if err != nil {
		return nil, err
	}
	exts, err := plugin.BuildAll(ctx, plugin.WorkExt, plugin.NewHost("local", cfg.Log), local.WorkExtensions)
	if err != nil {
		store.Close()
		return nil, err
	}
	eng, err := workengine.New(workengine.Options{Store: store, Server: server, Policy: pol, Extensions: exts,
		Config: local.Work, Flags: flags, Metrics: metrics.Reg, Log: cfg.Log, Owner: workengine.Owner("local")})
	if err != nil {
		store.Close()
		return nil, err
	}
	server.Extend(eng.API)
	cfg.Log.Info("work engine enabled", "db", path)
	return &localWork{engine: eng, store: store}, nil
}
