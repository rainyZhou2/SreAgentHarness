package pg_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/testutil"
)

func TestMigrateProfileOnPausedRun(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	ctx := context.Background()
	if err := s.EnsureProfile(ctx, testutil.Tenant, "sre-diagnosis", 2, []byte(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}
	run := newRun(t, s, "migrate-1")
	if _, err := s.ApplyCommand(ctx, testutil.Tenant, run.ID, domain.CommandRequest{Type: domain.CmdPause,
		IdempotencyKey: "p", ActorID: "alice"}); err != nil {
		t.Fatal(err)
	}
	cur := mustRun(t, s, run.ID)
	if cur.Status != domain.RunPaused {
		t.Fatalf("status = %s", cur.Status)
	}
	payload := json.RawMessage(`{"profile_version":3}`)
	_, err := s.ApplyCommand(ctx, testutil.Tenant, run.ID, domain.CommandRequest{Type: domain.CmdMigrateProfile,
		IdempotencyKey: "m0", ActorID: "alice", ExpectedVersion: &cur.Version, Payload: payload})
	var rej *domain.RejectedError
	if !errors.As(err, &rej) || rej.Reason != "unknown_profile_version" {
		t.Fatalf("err = %v", err)
	}
	cur = mustRun(t, s, run.ID)
	payload = json.RawMessage(`{"profile_version":2}`)
	if _, err := s.ApplyCommand(ctx, testutil.Tenant, run.ID, domain.CommandRequest{Type: domain.CmdMigrateProfile,
		IdempotencyKey: "m1", ActorID: "alice", ExpectedVersion: &cur.Version, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if got := mustRun(t, s, run.ID); got.ProfileVersion != 2 {
		t.Fatalf("profile_version = %d", got.ProfileVersion)
	}
}

func TestV11CommandsRequireExpectedVersion(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	run := newRun(t, s, "ver-1")
	for _, typ := range []domain.CommandType{domain.CmdOverrideResult, domain.CmdRetry, domain.CmdMigrateProfile,
		domain.CmdReplacePendingInput} {
		_, err := s.ApplyCommand(context.Background(), testutil.Tenant, run.ID, domain.CommandRequest{Type: typ,
			IdempotencyKey: "k-" + string(typ), ActorID: "alice", Payload: json.RawMessage(`{}`)})
		var rej *domain.RejectedError
		if !errors.As(err, &rej) || rej.Reason != "expected_version_required" {
			t.Fatalf("%s: err = %v", typ, err)
		}
	}
}
