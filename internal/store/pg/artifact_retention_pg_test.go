package pg

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-sre-harness/internal/retention"
	"go-sre-harness/internal/stream"
)

func TestArtifactRetentionAndStreamSegmentReferencePG(t *testing.T) {
	s, _, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "artifact-" + uuid.NewString()
	runID, stepID := seedRun(t, s, tenant)
	snapshotID := uuid.NewString()
	if _, err := s.pool.Exec(ctx, `INSERT INTO context_snapshots (id,tenant_id,run_id,step_id,profile_id,profile_version,refs)
		VALUES ($1,$2,$3,$4,'p',3,'{}')`, snapshotID, tenant, runID, stepID); err != nil {
		t.Fatal(err)
	}
	requestID := uuid.NewString()
	if _, err := s.pool.Exec(ctx, `INSERT INTO model_requests (id,tenant_id,run_id,step_id,step_attempt_id,fence_token,
		context_snapshot_id,model_alias,route_policy_id,route_policy_version,request_ref,request_hash,manifest_ref,status,max_output_tokens)
		VALUES ($1,$2,$3,$4,$5,1,$6,'m','r',1,'req','hash','manifest','SUCCEEDED',1)`,
		requestID, tenant, runID, stepID, uuid.NewString(), snapshotID); err != nil {
		t.Fatal(err)
	}
	ref := "mem://" + tenant + "/model-stream/" + uuid.NewString()
	seg := stream.Segment{TenantID: tenant, RunID: runID, ModelRequestID: requestID, No: 1, FirstSeq: 1, LastSeq: 1,
		Ref: ref, Hash: "sha256:" + uuid.NewString(), ExpiresAt: time.Now().Add(time.Hour)}
	if err := s.InsertStreamSegment(ctx, seg); err != nil {
		t.Fatal(err)
	}
	var references int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM artifact_references WHERE tenant_id=$1 AND artifact_ref=$2`,
		tenant, ref).Scan(&references); err != nil || references != 1 {
		t.Fatalf("segment and reference must commit together: %d %v", references, err)
	}
	bad := seg
	bad.ModelRequestID = uuid.NewString()
	bad.Ref = "mem://" + tenant + "/model-stream/" + uuid.NewString()
	if err := s.InsertStreamSegment(ctx, bad); err == nil {
		t.Fatal("an invalid business row must fail")
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM artifact_references WHERE tenant_id=$1 AND artifact_ref=$2`,
		tenant, bad.Ref).Scan(&references); err != nil || references != 0 {
		t.Fatalf("reference must roll back with its business row: %d %v", references, err)
	}
	cross := retention.ArtifactReference{TenantID: tenant, ArtifactRef: "mem://other/model-stream/hash",
		ReferrerType: "test", ReferrerID: "cross", ReferrerVersion: 1}
	if err := s.ReferenceArtifact(ctx, cross); err == nil {
		t.Fatal("cross-tenant artifact reference must be rejected")
	}
}

func TestArtifactHoldsAndDeletingReferencePG(t *testing.T) {
	s, _, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "hold-" + uuid.NewString()
	ref := "mem://" + tenant + "/evidence/" + uuid.NewString()
	first := retention.ArtifactHold{TenantID: tenant, ArtifactRef: ref, HoldID: "legal-1", Reason: "investigation", Actor: "alice"}
	second := retention.ArtifactHold{TenantID: tenant, ArtifactRef: ref, HoldID: "legal-2", Reason: "audit", Actor: "bob"}
	for _, hold := range []retention.ArtifactHold{first, second} {
		if err := s.PutArtifactHold(ctx, hold); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ReleaseArtifactHold(ctx, tenant, ref, first.HoldID, "alice", time.Now()); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ArtifactRetentionInput(ctx, tenant, ref, time.Now()); err != nil || retention.CanDeleteArtifact(got) {
		t.Fatalf("an independent hold must still block deletion: %+v %v", got, err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE artifact_objects SET status='DELETING' WHERE tenant_id=$1 AND artifact_ref=$2`, tenant, ref); err != nil {
		t.Fatal(err)
	}
	err := s.ReferenceArtifact(ctx, retention.ArtifactReference{TenantID: tenant, ArtifactRef: ref,
		ReferrerType: "memory", ReferrerID: uuid.NewString(), ReferrerVersion: 1})
	if err == nil {
		t.Fatal("DELETING artifacts must reject new references")
	}
}

func TestArtifactLeaseTakeoverPG(t *testing.T) {
	s, _, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "lease-" + uuid.NewString()
	ref := "mem://" + tenant + "/evidence/" + uuid.NewString()
	now := time.Now().UTC()
	if err := s.PutArtifactRetention(ctx, tenant, ref, "raw", now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.PutArtifactHold(ctx, retention.ArtifactHold{TenantID: tenant, ArtifactRef: ref, HoldID: "legal",
		Reason: "investigation", Actor: "alice", ExpiresAt: timePtr(now.Add(time.Hour))}); err != nil {
		t.Fatal(err)
	}
	claims, err := s.ClaimArtifacts(ctx, now, 10, time.Minute)
	if err != nil || len(claims) != 0 {
		t.Fatalf("an active hold must block claiming: %+v %v", claims, err)
	}
	if err := s.ReleaseArtifactHold(ctx, tenant, ref, "legal", "alice", now); err != nil {
		t.Fatal(err)
	}
	claims, err = s.ClaimArtifacts(ctx, now, 10, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].LeaseToken != 1 {
		t.Fatalf("first claim: %+v %v", claims, err)
	}
	again, err := s.ClaimArtifacts(ctx, now, 10, time.Minute)
	if err != nil || len(again) != 0 {
		t.Fatalf("an active lease must exclude competing cleaners: %+v %v", again, err)
	}
	taken, err := s.ClaimArtifacts(ctx, now.Add(2*time.Minute), 10, time.Minute)
	if err != nil || len(taken) != 1 || taken[0].LeaseToken != 2 {
		t.Fatalf("expired lease must be recoverable: %+v %v", taken, err)
	}
	if err := s.ConfirmArtifactDeleted(ctx, claims[0]); err == nil {
		t.Fatal("a stale lease token must not confirm deletion")
	}
	if err := s.ConfirmArtifactDeleted(ctx, taken[0]); err != nil {
		t.Fatal(err)
	}
}

func timePtr(at time.Time) *time.Time { return &at }
