package localapp

import (
	"context"
	"os"
	"path/filepath"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/notify"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/scheduler"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/workengine"

	"golang.org/x/sync/errgroup"
)

type localWork struct {
	flags     workengine.Flags
	engine    *workengine.Engine
	store     *cpsql.Store
	broker    *notify.Local
	publisher *scheduler.Publisher
}

func (w *localWork) close() { _ = w.store.Close() }

func (w *localWork) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return w.engine.Run(ctx) })
	g.Go(func() error { return w.publisher.Run(ctx) })
	return g.Wait()
}

// buildLocalWork assembles the control plane on a separate SQLite file
// (<data dir>/controlplane.db) when HARNESS_WORK_ENGINE_ENABLED is set.
func buildLocalWork(ctx context.Context, cfg Config, local LocalFile, server *api.Server, pol policy.Gate,
	metrics *obs.Metrics, embedder memory.Embedder) (*localWork, error) {
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
	broker := notify.NewLocal(1024)
	eng, err := workengine.New(workengine.Options{Store: store, Server: server, Policy: pol, Extensions: exts,
		Config: local.Work, Flags: flags, Metrics: metrics.Reg, Log: cfg.Log, Owner: workengine.Owner("local"), Embedder: embedder,
		WorkBroker: broker})
	if err != nil {
		store.Close()
		return nil, err
	}
	server.Extend(eng.API)
	cfg.Log.Info("work engine enabled", "db", path)
	publisher := &scheduler.Publisher{Store: store, Broker: broker, Log: cfg.Log.With("component", "work_outbox")}
	return &localWork{engine: eng, store: store, flags: flags, broker: broker, publisher: publisher}, nil
}
