package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"go-sre-harness/internal/plugin"
)

var appPluginKindID atomic.Uint64

func buildLifecycleHost(t *testing.T, lifecycle plugin.Lifecycle, tasks ...plugin.NamedTask) *plugin.Host {
	t.Helper()
	kind := plugin.NewKind[string](plugin.KindInfo{Name: fmt.Sprintf("app_lifecycle_test_%d", appPluginKindID.Add(1)), Multi: true, Doc: "lifecycle integration test", ContractVersion: "v1.0.0", AllowedServices: []string{}})
	definition := plugin.Define("component", "v1.0.0", "", nil,
		func(context.Context, *plugin.Host, plugin.Config) (plugin.Component[string], error) {
			component := plugin.Static("component")
			component.Lifecycle = lifecycle
			component.Tasks = tasks
			return component, nil
		})
	definition.Descriptor.ResourceBudget.MaxTasks = len(tasks)
	plugin.Register(kind, definition)
	host := plugin.NewHost("api", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := plugin.Build(context.Background(), kind, host, plugin.Spec{Name: "component"}); err != nil {
		t.Fatal(err)
	}
	return host
}

func lifecycleTestApp(host *plugin.Host) *App {
	return &App{
		Mode: "api",
		Host: host,
		env:  Env(func(string) string { return "" }),
		log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestAppDoesNotListenBeforePluginsReady(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	ready := make(chan struct{})
	readyStarted := make(chan struct{})
	stopped := make(chan struct{})
	lifecycle := plugin.LifecycleFuncs{
		OnReady: func(ctx context.Context) error {
			close(readyStarted)
			select {
			case <-ready:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		OnStop: func(context.Context) error {
			close(stopped)
			return nil
		},
	}
	host := buildLifecycleHost(t, lifecycle)
	host.HTTPAddr = addr
	app := lifecycleTestApp(host)
	app.Handler = http.NewServeMux()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	select {
	case <-readyStarted:
	case err := <-done:
		t.Fatalf("app exited before ready: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("plugin ready was not called")
	}
	if conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond); err == nil {
		_ = conn.Close()
		cancel()
		t.Fatal("HTTP listener opened before plugin ready")
	}
	close(ready)
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 30*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("HTTP listener did not open after plugin ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("app did not stop")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("plugin did not stop")
	}
}

func TestAppPropagatesPluginTaskFailure(t *testing.T) {
	release := make(chan struct{})
	task := plugin.TaskFunc(func(context.Context) error {
		<-release
		return errors.New("task failed")
	})
	host := buildLifecycleHost(t, plugin.LifecycleFuncs{}, plugin.NamedTask{Name: "work", Task: task})
	app := lifecycleTestApp(host)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, plugin.ErrTaskFailed) {
			t.Fatalf("expected task failure, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("app ignored plugin task failure")
	}
}
