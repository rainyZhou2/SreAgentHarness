package localapp

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/notify"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/work"
)

func TestLocalWorkPublishesOutboxToBroker(t *testing.T) {
	t.Setenv("HARNESS_WORK_ENGINE_ENABLED", "true")
	t.Setenv("HARNESS_ACTIONS_ENABLED", "false")
	dir := t.TempDir()
	kernelStore, err := localstore.Open(filepath.Join(dir, "kernel.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer kernelStore.Close()
	cfg := Config{DataDir: dir, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	server := &api.Server{Store: kernelStore}
	local, err := buildLocalWork(context.Background(), cfg, LocalFile{}, server, policy.Default(), obs.NewMetrics(), memory.HashingEmbedder{Dims: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer local.close()
	if local.broker == nil || local.publisher == nil || local.engine.Outbox == nil {
		t.Fatal("work broker and outbox publisher must share the control-plane store")
	}
	var tpl work.Template
	for _, candidate := range work.Builtin() {
		if candidate.ID == "incident" {
			tpl = candidate
		}
	}
	created, ok, err := local.store.CreateWork(context.Background(), work.NewWork{TenantID: "local", Kind: work.KindIncident,
		Title: "outbox", Goal: work.GoalSpec{Intent: "inspect"}, Template: tpl, IdempotencyKey: "outbox", Now: time.Now()})
	if err != nil || !ok {
		t.Fatalf("create work: created=%v err=%v", ok, err)
	}
	if n, err := local.publisher.Tick(context.Background()); err != nil || n != 1 {
		t.Fatalf("published=%d err=%v", n, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan string, 1)
	go func() {
		_ = local.broker.RunWork(ctx, func(_ context.Context, id string, done func(bool, time.Duration)) {
			received <- id
			done(true, 0)
		})
	}()
	select {
	case id := <-received:
		if id != created.ID {
			t.Fatalf("work id=%q, want %q", id, created.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("work wake was not delivered")
	}
}

var _ notify.Publisher = (*notify.Local)(nil)
