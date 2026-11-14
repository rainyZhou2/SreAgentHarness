package workengine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/work"
	"go-sre-harness/migrations"
)

type pausedActivationStore struct {
	api.Store
	workRunStore
	entered     chan struct{}
	release     chan struct{}
	afterCommit error
}

func (s *pausedActivationStore) ActivateWorkRun(ctx context.Context, tenant, run string, owner domain.WorkRunOwner) error {
	close(s.entered)
	select {
	case <-s.release:
		if err := s.workRunStore.ActivateWorkRun(ctx, tenant, run, owner); err != nil {
			return err
		}
		return s.afterCommit
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestKernelActivationOrdersCancellation(t *testing.T) {
	for _, backend := range []string{"local", "postgres"} {
		for _, cancelFirst := range []bool{false, true} {
			name := backend + "/activate_first"
			if cancelFirst {
				name = backend + "/cancel_first"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				var cp *cpsql.Store
				var ks api.Store
				if backend == "local" {
					var err error
					cp, err = cpsql.OpenSQLite(filepath.Join(t.TempDir(), "work.db"))
					if err != nil {
						t.Fatal(err)
					}
					local, err := localstore.Open(filepath.Join(t.TempDir(), "kernel.db"))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = local.Close() })
					ks = local
				} else {
					url := os.Getenv("HARNESS_PG_TEST_URL")
					if url == "" {
						t.Skip("HARNESS_PG_TEST_URL not set")
					}
					config, err := pgxpool.ParseConfig(url)
					if err != nil {
						t.Fatal(err)
					}
					config.ConnConfig.Database = kernelTestDatabase(t, ctx)
					pool, err := pgxpool.NewWithConfig(ctx, config)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(pool.Close)
					if err := migrations.Apply(ctx, pool); err != nil {
						t.Fatal(err)
					}
					cp, ks = cpsql.OpenPostgres(pool), pg.New(pool, pg.Config{})
				}
				t.Cleanup(func() { _ = cp.Close() })
				tenant := uuid.NewString()
				tpl := work.Builtin()[0]
				w, _, err := cp.CreateWork(ctx, work.NewWork{TenantID: tenant, Kind: tpl.Kind, Goal: work.GoalSpec{Intent: "diagnose"}, Template: tpl, IdempotencyKey: "work"})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := ks.(interface {
					RegisterProfile(context.Context, string, string, int64, json.RawMessage) (bool, error)
				}).RegisterProfile(ctx, tenant, "p", 1, []byte(`{"strategy":"llm"}`)); err != nil {
					t.Fatal(err)
				}
				k := KernelRuns{Server: &api.Server{Store: ks, Artifacts: artifact.NewMemory()}, WorkAuthority: cp}
				stage, _ := w.CurrentStage()
				runID, err := k.ReserveRun(ctx, orchestration.RunRequest{TenantID: tenant, WorkID: w.ID, Generation: w.Generation, StageID: stage.ID, Goal: "diagnose", ProfileID: "p", ProfileVersion: 1, IdempotencyKey: "run"})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := cp.ClaimDueWorks(ctx, "activation-driver", time.Now(), time.Minute, 200); err != nil {
					t.Fatal(err)
				}
				w, err = cp.GetWork(ctx, tenant, w.ID)
				if err != nil {
					t.Fatal(err)
				}
				active, cancelled := work.StatusActive, work.StatusCancelled
				w, err = cp.ApplyWork(ctx, work.Mutation{TenantID: tenant, WorkID: w.ID, ExpectedVersion: w.Version, LeaseOwner: "activation-driver", ActorType: "system", ActorID: "activation-driver", Status: &active, Links: []work.RunLink{{RunID: runID, Role: work.RoleOwner, Generation: w.Generation, StageID: stage.ID}}})
				if err != nil {
					t.Fatal(err)
				}
				req := work.RunActivation{TenantID: tenant, WorkID: w.ID, RunID: runID, Generation: w.Generation, StageID: stage.ID, ExpectedVersion: w.Version, LeaseOwner: "activation-driver"}
				cancelWork := func(c context.Context) error {
					_, err := cp.ApplyWork(c, work.Mutation{TenantID: tenant, WorkID: w.ID, ExpectedVersion: w.Version, ActorType: "system", ActorID: "operator", Status: &cancelled})
					return err
				}
				paused := &pausedActivationStore{Store: ks, workRunStore: ks.(workRunStore), entered: make(chan struct{}), release: make(chan struct{})}
				k.Server.Store = paused
				if cancelFirst {
					if err := cancelWork(ctx); err != nil {
						t.Fatal(err)
					}
					if err := k.ActivateRun(ctx, req); err == nil {
						t.Fatal("cancelled Work activated")
					}
					select {
					case <-paused.entered:
						t.Fatal("unauthorized activation callback entered")
					default:
					}
				} else {
					// The Kernel commit can survive a callback error. It must not
					// be replayed, and cancellation must still order after it.
					committedError := errors.New("activation committed; result lost")
					paused.afterCommit = committedError
					done := make(chan error, 1)
					go func() { done <- k.ActivateRun(ctx, req) }()
					select {
					case <-paused.entered:
					case err := <-done:
						t.Fatalf("activation never entered: %v", err)
					case <-time.After(5 * time.Second):
						t.Fatal("activation blocked")
					}
					cancelCtx, stop := context.WithTimeout(ctx, 150*time.Millisecond)
					cancelErr := cancelWork(cancelCtx)
					timedOut := errors.Is(cancelCtx.Err(), context.DeadlineExceeded)
					stop()
					close(paused.release)
					if err := <-done; !errors.Is(err, committedError) {
						t.Fatalf("lost commit result: %v", err)
					}
					if cancelErr == nil || !timedOut {
						t.Fatalf("Work cancellation did not wait behind activation: err=%v deadline=%v", cancelErr, timedOut)
					}
					if err := cancelWork(ctx); err != nil {
						t.Fatal(err)
					}
				}
				run, err := ks.GetRun(ctx, tenant, runID)
				if err != nil {
					t.Fatal(err)
				}
				want := domain.RunQueued
				if cancelFirst {
					want = domain.RunPendingActivation
				}
				if run.Status != want {
					t.Fatalf("status=%s want=%s", run.Status, want)
				}
			})
		}
	}
}

func TestKernelRunDurableHandoff(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	ks, err := localstore.Open(filepath.Join(root, "kernel.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ks.Close() })
	cp, err := cpsql.OpenSQLite(filepath.Join(root, "work.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	k := KernelRuns{Server: &api.Server{Store: ks, Artifacts: &artifact.FS{Root: filepath.Join(root, "artifacts")}}, WorkAuthority: cp}
	tpl := work.Builtin()[0]
	w, _, err := cp.CreateWork(ctx, work.NewWork{TenantID: "t", Kind: tpl.Kind, Title: "handoff", Goal: work.GoalSpec{Intent: "diagnose"}, Template: tpl, IdempotencyKey: "w"})
	if err != nil {
		t.Fatal(err)
	}
	stage, _ := w.CurrentStage()
	req := orchestration.RunRequest{TenantID: "t", WorkID: w.ID, Generation: w.Generation, StageID: stage.ID, Goal: "diagnose", IdempotencyKey: "r"}
	id, err := k.ReserveRun(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ks.ClaimStep(ctx, id, "worker", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
		t.Fatalf("unlinked run claimed: %v", err)
	}
	if _, err := cp.ClaimDueWorks(ctx, "driver", time.Now(), time.Minute, 10); err != nil {
		t.Fatal(err)
	}
	w, err = cp.GetWork(ctx, "t", w.ID)
	if err != nil {
		t.Fatal(err)
	}
	activation := work.RunActivation{TenantID: "t", WorkID: w.ID, RunID: id, Generation: w.Generation, StageID: stage.ID, ExpectedVersion: w.Version, LeaseOwner: "driver"}
	if err := k.ActivateRun(ctx, activation); err == nil {
		t.Fatal("unlinked run activated")
	}
	active := work.StatusActive
	w, err = cp.ApplyWork(ctx, work.Mutation{TenantID: "t", WorkID: w.ID, ExpectedVersion: w.Version, LeaseOwner: "driver", ActorType: "system", ActorID: "driver", Status: &active,
		Links: []work.RunLink{{RunID: id, Role: work.RoleOwner, Generation: w.Generation, StageID: stage.ID}}})
	if err != nil {
		t.Fatal(err)
	}
	activation.ExpectedVersion = w.Version
	// Reopen the kernel store between the two durable commits.
	if err := ks.Close(); err != nil {
		t.Fatal(err)
	}
	ks, err = localstore.Open(filepath.Join(root, "kernel.db"))
	if err != nil {
		t.Fatal(err)
	}
	k.Server.Store = ks
	if retry, err := k.ReserveRun(ctx, req); err != nil || retry != id {
		t.Fatalf("reservation retry: %q %v", retry, err)
	}
	if err := k.ActivateRun(ctx, activation); err != nil {
		t.Fatal(err)
	}
	if err := k.ActivateRun(ctx, activation); err != nil {
		t.Fatal(err)
	}
	if _, err := ks.ClaimStep(ctx, id, "worker", time.Minute); err != nil {
		t.Fatalf("linked activation not claimable: %v", err)
	}
	cancelled := work.StatusCancelled
	if _, err := cp.ApplyWork(ctx, work.Mutation{TenantID: "t", WorkID: w.ID, ExpectedVersion: w.Version, ActorType: "system", ActorID: "operator", Status: &cancelled}); err != nil {
		t.Fatal(err)
	}
	rec := &orchestration.Reconciler{Store: cp, Runs: k, Inventory: k}
	if err := rec.RecoverRuns(ctx); err != nil {
		t.Fatal(err)
	}
	run, err := ks.GetRun(ctx, "t", id)
	if err != nil {
		t.Fatal(err)
	}
	if run.DesiredState != domain.DesiredCancelled {
		t.Fatalf("cancellation not propagated: %+v", run)
	}
	if err := k.ActivateRun(ctx, activation); err == nil {
		t.Fatal("revoked Work reactivated")
	}
}
