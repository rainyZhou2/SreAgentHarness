package cpsql

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/eventing"
)

func TestSourceCheckpointLeaseAndFencing(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		now := t0
		first, acquired, err := s.ClaimSourceCheckpoint(ctx, tn, "alerts", "owner-a", now, time.Minute)
		if err != nil || !acquired || first.FencingToken != 1 || first.Cursor != "" {
			t.Fatalf("first=%+v acquired=%v err=%v", first, acquired, err)
		}
		if _, acquired, err := s.ClaimSourceCheckpoint(ctx, tn, "alerts", "owner-b", now, time.Minute); err != nil || acquired {
			t.Fatalf("concurrent claim acquired=%v err=%v", acquired, err)
		}
		if err := s.HeartbeatSourceCheckpoint(ctx, first, now.Add(2*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if err := s.CommitSourceCheckpoint(ctx, first, "v1:10"); err != nil {
			t.Fatal(err)
		}
		second, acquired, err := s.ClaimSourceCheckpoint(ctx, tn, "alerts", "owner-a", now.Add(3*time.Minute), time.Minute)
		if err != nil || !acquired || second.FencingToken != 2 || second.Cursor != "v1:10" {
			t.Fatalf("second=%+v acquired=%v err=%v", second, acquired, err)
		}
		third, acquired, err := s.ClaimSourceCheckpoint(ctx, tn, "alerts", "owner-b", now.Add(5*time.Minute), time.Minute)
		if err != nil || !acquired || third.FencingToken != 3 {
			t.Fatalf("expired takeover=%+v acquired=%v err=%v", third, acquired, err)
		}
		if err := s.CommitSourceCheckpoint(ctx, second, "v1:11"); !errors.Is(err, eventing.ErrSourceFenced) {
			t.Fatalf("stale commit err=%v", err)
		}
		if err := s.HeartbeatSourceCheckpoint(ctx, second, now.Add(6*time.Minute)); !errors.Is(err, eventing.ErrSourceFenced) {
			t.Fatalf("stale heartbeat err=%v", err)
		}
		state, ok, err := s.SourceCheckpoint(ctx, tn, "alerts")
		if err != nil || !ok || state.Cursor != "v1:10" || state.Owner != "owner-b" {
			t.Fatalf("state=%+v ok=%v err=%v", state, ok, err)
		}
		if err := s.FailSourceCheckpoint(ctx, third, now.Add(10*time.Minute), "source unavailable"); err != nil {
			t.Fatal(err)
		}
		if _, acquired, err := s.ClaimSourceCheckpoint(ctx, tn, "alerts", "owner-c", now.Add(6*time.Minute), time.Minute); err != nil || acquired {
			t.Fatalf("retry backoff acquired=%v err=%v", acquired, err)
		}
		state, ok, err = s.SourceCheckpoint(ctx, tn, "alerts")
		if err != nil || !ok || state.LastError != "source unavailable" || state.Owner != "" || state.Cursor != "v1:10" {
			t.Fatalf("failed state=%+v ok=%v err=%v", state, ok, err)
		}
		fourth, acquired, err := s.ClaimSourceCheckpoint(ctx, tn, "alerts", "owner-c", now.Add(11*time.Minute), time.Minute)
		if err != nil || !acquired || fourth.FencingToken != 4 || fourth.Cursor != "v1:10" {
			t.Fatalf("retry=%+v acquired=%v err=%v", fourth, acquired, err)
		}
		if err := s.ReleaseSourceCheckpoint(ctx, second); !errors.Is(err, eventing.ErrSourceFenced) {
			t.Fatalf("stale release err=%v", err)
		}
		if err := s.ReleaseSourceCheckpoint(ctx, fourth); err != nil {
			t.Fatal(err)
		}
	})
}
