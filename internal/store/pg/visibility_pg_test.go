package pg

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"go-sre-harness/internal/domain"
)

// The per-user list filters run in SQL, under row-level security.
func TestListRunsAndSessionsPerUserPostgres(t *testing.T) {
	admin, app, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "t-" + uuid.NewString()[:8]
	tctx := WithTenant(ctx, tenant)
	if _, err := admin.pool.Exec(ctx, `INSERT INTO agent_profiles (tenant_id, profile_id, version, spec, spec_hash, status)
		VALUES ($1,'p',3,'{}','h','ACTIVE')`, tenant); err != nil {
		t.Fatal(err)
	}
	run := func(typ string, initiator *string) string {
		id := uuid.NewString()
		if _, err := admin.pool.Exec(ctx, `INSERT INTO agent_runs (id, tenant_id, profile_id, profile_version, status,
			create_idempotency_key, goal_ref, initiator_type, initiator_id) VALUES ($1,$2,'p',3,'COMPLETED',$3,'g',$4,$5)`,
			id, tenant, id, typ, initiator); err != nil {
			t.Fatal(err)
		}
		return id
	}
	alice, bob := "alice", "bob"
	ra, rb, rc := run("user", &alice), run("user", &bob), run("system", nil)

	got, err := app.ListRunsFor(tctx, tenant, "", 50, "alice")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, r := range got {
		seen[r.ID] = true
	}
	if !seen[ra] || seen[rb] || !seen[rc] {
		t.Fatalf("alice sees her runs and shared operational runs, not bob's: %v", seen)
	}
	unk := run("unknown", nil)
	if got, _ := app.ListRunsFor(tctx, tenant, "", 50, "alice"); len(got) != 2 {
		t.Fatalf("a run of unknown origin is private (fails closed): %v (%s)", got, unk)
	}
	if all, _ := app.ListRuns(tctx, tenant, "", 50); len(all) != 4 {
		t.Fatalf("the unfiltered list is unchanged: %d", len(all))
	}

	for _, u := range []string{"alice", "bob"} {
		if _, _, err := app.CreateSession(tctx, domain.CreateSessionRequest{TenantID: tenant, ActorID: u, ProfileID: "p", ProfileVersion: 3, Title: u + " chat", IdempotencyKey: "s-" + u}); err != nil {
			t.Fatal(err)
		}
	}
	mine, err := app.ListSessions(tctx, tenant, SessionFilter{CreatedBy: "alice"})
	if err != nil || len(mine) != 1 || mine[0].CreatedBy != "alice" {
		t.Fatalf("%+v %v", mine, err)
	}
	if every, _ := app.ListSessions(tctx, tenant, SessionFilter{}); len(every) != 2 {
		t.Fatalf("an empty CreatedBy lists the tenant's sessions: %d", len(every))
	}
}
