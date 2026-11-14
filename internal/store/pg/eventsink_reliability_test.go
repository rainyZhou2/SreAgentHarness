package pg

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventlog"
	"go-sre-harness/internal/relay"
	"go-sre-harness/migrations"
)

func TestEventSinkRejectsExpiredAndRegressingCursor(t *testing.T) {
	s, _, _ := pgTestStores(t)
	ctx := context.Background()
	id := "cursor-" + uuid.NewString()
	if _, ok, err := s.ClaimEventSink(ctx, id, "a", time.Minute, true); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	first := eventlog.Cursor{TxID: 1, GlobalSeq: 10}
	if err := s.AdvanceEventSink(ctx, id, "a", first, 1, 0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE event_sink_cursors SET lease_until=now()-interval '1 second' WHERE sink_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	later := eventlog.Cursor{TxID: 1, GlobalSeq: 20}
	if err := s.AdvanceEventSink(ctx, id, "a", later, 1, 0, time.Minute); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("expired cursor update: %v", err)
	}
	got, ok, err := s.ClaimEventSink(ctx, id, "b", time.Minute, true)
	if err != nil || !ok || got != first {
		t.Fatalf("takeover: %+v %v %v", got, ok, err)
	}
	if err := s.AdvanceEventSink(ctx, id, "a", later, 1, 0, time.Minute); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("replaced owner: %v", err)
	}
	if err := s.AdvanceEventSink(ctx, id, "b", later, 1, 0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.AdvanceEventSink(ctx, id, "b", first, 1, 0, time.Minute); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("regressing cursor: %v", err)
	}
	var seq, delivered int64
	if err := s.pool.QueryRow(ctx, `SELECT global_seq,delivered FROM event_sink_cursors WHERE sink_id=$1`, id).Scan(&seq, &delivered); err != nil {
		t.Fatal(err)
	}
	if seq != 20 || delivered != 2 {
		t.Fatalf("cursor/counters changed on rejected write: %d %d", seq, delivered)
	}
}

type relayPublisherFunc func(context.Context, domain.Event) error

func (f relayPublisherFunc) Publish(ctx context.Context, e domain.Event) error { return f(ctx, e) }

func relayFixture(t *testing.T) (*Store, string, string, string, []string) {
	t.Helper()
	ctx := context.Background()
	raw := os.Getenv("HARNESS_PG_TEST_URL")
	if raw == "" {
		t.Skip("HARNESS_PG_TEST_URL not set")
	}
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	name := "relay_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	})
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	s := New(pool, Config{})
	id := "relay-" + uuid.NewString()
	tenant := "relay-" + uuid.NewString()
	run, _ := seedRun(t, s, tenant)
	if _, ok, err := s.ClaimEventSink(ctx, id, "a", time.Minute, false); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	ids := []string{uuid.NewString(), uuid.NewString()}
	for i, eid := range ids {
		if _, err := s.pool.Exec(ctx, `INSERT INTO run_events(tenant_id,run_id,event_seq,event_id,event_type,actor_type,actor_id,occurred_at) VALUES($1,$2,$3,$4,'run.completed','system','relay-test',now())`, tenant, run, i+1, eid); err != nil {
			t.Fatal(err)
		}
	}
	return s, id, tenant, run, ids
}

func TestEventSinkExpiryAfterRowLockWait(t *testing.T) {
	s, _, _ := pgTestStores(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := "cursor-wait-" + uuid.NewString()
	if _, ok, err := s.ClaimEventSink(ctx, id, "a", time.Second, true); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	lock, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	var pid int
	if err := lock.QueryRow(ctx, `SELECT pg_backend_pid() FROM event_sink_cursors WHERE sink_id=$1 FOR UPDATE`, id).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- s.AdvanceEventSink(ctx, id, "a", eventlog.Cursor{TxID: 1, GlobalSeq: 1}, 1, 0, time.Minute)
	}()
	for {
		var blocked bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("advance did not wait on the row lock: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if _, err := s.pool.Exec(ctx, `SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02) FROM event_sink_cursors WHERE sink_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err := lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("lease expired during lock wait: %v", err)
	}
}

func TestRelayCheckpointsPublishedPrefix(t *testing.T) {
	s, id, tenant, run, ids := relayFixture(t)
	ctx := context.Background()
	outage := errors.New("broker unavailable")
	attempts := map[string]int{}
	fail := true
	r := relay.Relay{ID: id, Owner: "a", Store: s, LeaseTTL: time.Minute, Publisher: relayPublisherFunc(func(_ context.Context, e domain.Event) error {
		if e.TenantID != tenant {
			t.Fatalf("unexpected concurrent test event: %s", e.TenantID)
		}
		attempts[e.ID]++
		if e.ID == ids[1] && fail {
			return outage
		}
		return nil
	})}
	if n, err := r.Once(ctx); !errors.Is(err, outage) || n != 1 {
		t.Fatalf("partial delivery: n=%d err=%v", n, err)
	}
	cur, ok, err := s.ClaimEventSink(ctx, id, "a", time.Minute, false)
	if err != nil || !ok {
		t.Fatalf("read cursor: %v %v", ok, err)
	}
	remaining, err := s.ReadEventLog(ctx, cur, 10)
	if err != nil || len(remaining) != 1 || remaining[0].Event.ID != ids[1] {
		t.Fatalf("remaining events: %+v %v", remaining, err)
	}
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1`, run).Scan(&count); err != nil || count != 2 {
		t.Fatalf("committed facts: %d %v", count, err)
	}
	fail = false
	if n, err := r.Once(ctx); err != nil || n != 1 {
		t.Fatalf("retry: %d %v", n, err)
	}
	if attempts[ids[0]] != 1 || attempts[ids[1]] != 2 {
		t.Fatalf("delivery attempts: %v", attempts)
	}
	if n, err := r.Once(ctx); err != nil || n != 0 {
		t.Fatalf("drained: %d %v", n, err)
	}
}

type lostCursorCommit struct {
	eventlog.Store
	err error
}

func (s *lostCursorCommit) AdvanceEventSink(ctx context.Context, id, owner string, c eventlog.Cursor, delivered, skipped int, ttl time.Duration) error {
	if err := s.Store.AdvanceEventSink(ctx, id, owner, c, delivered, skipped, ttl); err != nil {
		return err
	}
	err := s.err
	s.err = nil
	return err
}

func TestRelayRecoversLostCursorCommit(t *testing.T) {
	s, id, _, _, ids := relayFixture(t)
	ctx := context.Background()
	lost := errors.New("cursor commit response lost")
	store := &lostCursorCommit{Store: s, err: lost}
	calls := map[string]int{}
	r := relay.Relay{ID: id, Owner: "a", Store: store, Batch: 1, Publisher: relayPublisherFunc(func(_ context.Context, e domain.Event) error {
		calls[e.ID]++
		return nil
	})}
	if n, err := r.Once(ctx); n != 0 || !errors.Is(err, lost) {
		t.Fatalf("lost commit: %d %v", n, err)
	}
	if n, err := r.Once(ctx); n != 1 || err != nil {
		t.Fatalf("recovery: %d %v", n, err)
	}
	if len(calls) != 2 || calls[ids[0]] != 1 || calls[ids[1]] != 1 {
		t.Fatalf("committed prefix replayed: %v", calls)
	}
}

func TestRelayTakeoverAfterPublish(t *testing.T) {
	s, id, _, _, ids := relayFixture(t)
	ctx := context.Background()
	calls := map[string]int{}
	old := relay.Relay{ID: id, Owner: "a", Store: s, Batch: 1, Publisher: relayPublisherFunc(func(_ context.Context, e domain.Event) error {
		calls[e.ID]++
		if _, err := s.pool.Exec(ctx, `UPDATE event_sink_cursors SET lease_until=now()-interval '1 second' WHERE sink_id=$1`, id); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := s.ClaimEventSink(ctx, id, "b", time.Minute, false); err != nil || !ok {
			t.Fatalf("takeover: %v %v", ok, err)
		}
		return nil
	})}
	if n, err := old.Once(ctx); n != 0 || !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale owner advanced: %d %v", n, err)
	}
	replacement := relay.Relay{ID: id, Owner: "b", Store: s, Publisher: relayPublisherFunc(func(_ context.Context, e domain.Event) error {
		calls[e.ID]++
		return nil
	})}
	if n, err := replacement.Once(ctx); n != 2 || err != nil {
		t.Fatalf("replacement delivery: %d %v", n, err)
	}
	if len(calls) != 2 || calls[ids[0]] != 2 || calls[ids[1]] != 1 {
		t.Fatalf("at-least-once recovery lost events: %v", calls)
	}
}
