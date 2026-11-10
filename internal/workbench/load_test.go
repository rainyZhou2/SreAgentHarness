package workbench

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/migrations"
)

func TestLoadSmoke(t *testing.T) {
	stores := map[string]*cpsql.Store{}
	s, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "load.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	stores["sqlite"] = s
	if url := os.Getenv("HARNESS_PG_TEST_URL"); url != "" {
		pool, err := pgxpool.New(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		if err := migrations.Apply(context.Background(), pool); err != nil {
			t.Fatal(err)
		}
		stores["postgres"] = cpsql.OpenPostgres(pool)
	}
	opt := LoadOptions{Works: 30, Events: 60, Subjects: 10, FactsPerSubject: 4, Snapshots: 20, Concurrency: 4, Replicas: 2}
	for name, s := range stores {
		t.Run(name, func(t *testing.T) {
			rep, err := Load(context.Background(), s, name, opt)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range []string{LoadWorkCreate, LoadCorrelation, LoadWakeLag, LoadSnapshot} {
				p := rep.Metrics[m]
				if p.Count == 0 || p.Errors != 0 || p.P50 > p.P95 || p.P95 > p.P99 || p.P99 > p.Max {
					t.Fatalf("%s: %+v", m, p)
				}
			}
			if rep.Metrics[LoadWorkCreate].Count != 30 || rep.Metrics[LoadWakeLag].Count != 30 {
				t.Fatalf("counts: %+v", rep.Metrics)
			}
			t.Logf("%s: %+v", name, rep.Metrics)
		})
	}
}

func TestLoadBudget(t *testing.T) {
	rep := LoadReport{Metrics: map[string]Percentiles{LoadWorkCreate: {Count: 10, P50: 2, P95: 9, P99: 12, Max: 20, PerSecond: 100}}}
	if v := (LoadBudget{LoadWorkCreate: {P95: 10, P99: 15}}).Check(rep); len(v) != 0 {
		t.Fatalf("within budget: %v", v)
	}
	v := LoadBudget{LoadWorkCreate: {P95: 5, PerSecond: 500}, LoadSnapshot: {P95: 1}}.Check(rep)
	if len(v) != 3 {
		t.Fatalf("breaches: %v", v)
	}
}
