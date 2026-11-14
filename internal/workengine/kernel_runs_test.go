package workengine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
)

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
