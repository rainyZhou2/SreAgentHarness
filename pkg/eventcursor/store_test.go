package eventcursor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func cursorKey() Key {
	return Key{ServerURL: "https://Harness.Example.com/", TenantID: "tenant-a", SubjectID: "user-a",
		Stream: "work", ResourceID: "work-1", Purpose: "timeline"}
}

func TestFileStoreSurvivesReopenAndIsolatesKeys(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "cursors")
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := cursorKey()
	if err := store.Advance(ctx, key, 7, "event-7"); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := reopened.Load(ctx, key)
	if err != nil || !ok || got.Sequence != 7 || got.EventID != "event-7" || got.UpdatedAt.IsZero() {
		t.Fatalf("cursor=%+v ok=%v err=%v", got, ok, err)
	}
	other := key
	other.TenantID = "tenant-b"
	if _, ok, err := reopened.Load(ctx, other); err != nil || ok {
		t.Fatalf("cross-tenant cursor ok=%v err=%v", ok, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
	if entries[0].Name() != key.ID()+".json" {
		t.Fatalf("cursor path leaks identity: %s", entries[0].Name())
	}
	info, err := os.Stat(filepath.Join(dir, entries[0].Name()))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%v err=%v", info.Mode(), err)
	}
}

func TestFileStoreRejectsRegressionAndCorruptionWithoutOverwrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := cursorKey()
	if err := store.Advance(ctx, key, 9, "event-9"); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(ctx, key, 8, "event-8"); !errors.Is(err, ErrRegression) {
		t.Fatalf("regression err=%v", err)
	}
	if err := store.Advance(ctx, key, 9, "different-event"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict err=%v", err)
	}
	path := filepath.Join(dir, key.ID()+".json")
	valid, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"sequence":100`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(ctx, key); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt load err=%v", err)
	}
	if err := store.Advance(ctx, key, 10, "event-10"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt advance err=%v", err)
	}
	corrupt, _ := os.ReadFile(path)
	if string(corrupt) == string(valid) {
		t.Fatal("test did not replace the valid cursor")
	}
	if err := os.WriteFile(path, valid, 0o600); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.Load(ctx, key)
	if err != nil || !ok || got.Sequence != 9 {
		t.Fatalf("restored cursor=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestFileStoreConcurrentAdvanceKeepsHighestSequence(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	key := cursorKey()
	const count = 32
	var wg sync.WaitGroup
	for i := 1; i <= count; i++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			err := store.Advance(ctx, key, int64(seq), fmt.Sprintf("event-%d", seq))
			if err != nil && !errors.Is(err, ErrRegression) && !errors.Is(err, ErrConflict) {
				t.Errorf("advance %d: %v", seq, err)
			}
		}(i)
	}
	wg.Wait()
	got, ok, err := store.Load(ctx, key)
	if err != nil || !ok || got.Sequence != count {
		t.Fatalf("cursor=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestKeyValidation(t *testing.T) {
	for _, mutate := range []func(*Key){
		func(k *Key) { k.ServerURL = "file:///tmp/state" },
		func(k *Key) { k.ServerURL = "https://user:password@example.com" },
		func(k *Key) { k.TenantID = "" },
		func(k *Key) { k.ResourceID = "" },
		func(k *Key) { k.Purpose = "" },
	} {
		key := cursorKey()
		mutate(&key)
		store, err := NewFileStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.Load(context.Background(), key); err == nil {
			t.Fatalf("accepted key %+v", key)
		}
	}
}
