package pg

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/security"
)

type commitEvaluator struct {
	effect security.Effect
}

func (e commitEvaluator) Evaluate(context.Context, security.Request) (security.PolicyDecision, error) {
	return security.PolicyDecision{Effect: e.effect, PolicyVersion: "test"}, nil
}

type emptyCommitRevocations struct{}

func (emptyCommitRevocations) Stamps(context.Context, security.Request) ([]security.RevocationStamp, error) {
	return nil, nil
}

type commitAuditRecorder struct {
	pool *pgxpool.Pool
}

func (r commitAuditRecorder) RecordDecision(ctx context.Context, request security.Request, decision security.PolicyDecision) error {
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return err
	}
	decisionJSON, err := json.Marshal(decision)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx, `INSERT INTO security_decision_audit
		(tenant_id,decision_id,request_id,operation,effect,mode,request_json,decision_json,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, request.TenantID, decision.DecisionID, request.RequestID,
		request.Operation, decision.Effect, decision.Mode, requestJSON, decisionJSON, decision.DecidedAt)
	return err
}

func securityLease(t *testing.T, store *Store, tenant string) domain.Lease {
	t.Helper()
	lease := seedLeasedToolRun(t, store, tenant, "DISPATCHED")
	lease.Intent = &domain.ActionIntent{ID: uuid.NewString(),
		Environment: "production", Target: testTarget, ToolName: "deploy.restart", ToolVersion: "1",
		InputHash: hashStrings("input"), ActionHash: hashStrings("input"), RiskClass: domain.RiskR1}
	lease.Subject = "run:" + lease.RunID
	lease.Environment = "production"
	lease.InputHash = lease.Intent.InputHash
	_, err := store.pool.Exec(context.Background(), `INSERT INTO action_intents
		(id,tenant_id,run_id,step_id,environment,target,tool_name,tool_version,input_hash,action_hash,risk_class,policy_version,effects)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'{}')`, lease.Intent.ID, tenant, lease.RunID, lease.StepID,
		lease.Intent.Environment, lease.Intent.Target, lease.Intent.ToolName, lease.Intent.ToolVersion,
		lease.Intent.InputHash, lease.Intent.ActionHash, lease.Intent.RiskClass, "test")
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func authorizeSecurityLease(t *testing.T, store *Store, service *security.Service, lease domain.Lease) security.PolicyDecision {
	t.Helper()
	request := security.Request{RequestID: lease.AttemptID + ":" + lease.Intent.ID, Operation: security.ToolExecute,
		TenantID: lease.TenantID, Subject: lease.Subject, ResourceRef: lease.Intent.Target, Environment: lease.Environment,
		Risk: string(lease.Intent.RiskClass), InputHash: lease.Intent.InputHash, RunID: lease.RunID, ActionID: lease.Intent.ID}
	decision, err := service.Authorize(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func commitSecurityService(store *Store, effect security.Effect) *security.Service {
	now := time.Now().UTC()
	return &security.Service{Mode: security.ModeEnforce, Evaluator: commitEvaluator{effect: effect},
		Revocations: emptyCommitRevocations{}, FreshRevocations: emptyCommitRevocations{},
		Recorder: commitAuditRecorder{pool: store.pool}, Now: func() time.Time { return now }}
}

func consumeSecurityApproval(t *testing.T, store *Store, lease domain.Lease) {
	t.Helper()
	requestID := uuid.NewString()
	if _, err := store.pool.Exec(context.Background(), `INSERT INTO approval_requests
		(id,tenant_id,action_intent_id,required_approvals,status,expires_at,requested_event_seq)
		VALUES ($1,$2,$3,1,'CONSUMED',now()+interval '10 minutes',0)`, requestID, lease.TenantID, lease.Intent.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(context.Background(), `INSERT INTO approval_grants
		(id,tenant_id,approval_request_id,action_hash,max_uses,used_count,expires_at,consumed_by_attempt_id)
		VALUES ($1,$2,$3,$4,1,1,now()+interval '10 minutes',$5)`, uuid.NewString(), lease.TenantID, requestID,
		lease.Intent.ActionHash, lease.AttemptID); err != nil {
		t.Fatal(err)
	}
}

func assertSecurityCommitRolledBack(t *testing.T, store *Store, lease domain.Lease) {
	t.Helper()
	var attempt, step, effect string
	if err := store.pool.QueryRow(context.Background(), `SELECT a.status,s.status,e.state FROM step_attempts a
		JOIN agent_steps s ON s.id=a.step_id JOIN external_tool_effects e ON e.step_id=s.id
		WHERE a.id=$1`, lease.AttemptID).Scan(&attempt, &step, &effect); err != nil {
		t.Fatal(err)
	}
	if attempt != "RUNNING" || step != "RUNNING" || effect != "DISPATCHED" {
		t.Fatalf("attempt=%s step=%s effect=%s", attempt, step, effect)
	}
}

func TestSecurityCommitGuardPostgres(t *testing.T) {
	admin, _, _ := pgTestStores(t)
	ctx := context.Background()
	t.Run("valid", func(t *testing.T) {
		lease := securityLease(t, admin, "t-sec-valid-"+uuid.NewString()[:8])
		service := commitSecurityService(admin, security.Allow)
		decision := authorizeSecurityLease(t, admin, service, lease)
		store := New(admin.pool, Config{CommitGuard: NewSecurityCommitGuard(service)})
		if err := store.CommitSuccess(ctx, lease, StepSuccess{SideEffect: true, SecurityDecisionID: decision.DecisionID}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("missing decision", func(t *testing.T) {
		lease := securityLease(t, admin, "t-sec-missing-"+uuid.NewString()[:8])
		service := commitSecurityService(admin, security.Allow)
		store := New(admin.pool, Config{CommitGuard: NewSecurityCommitGuard(service)})
		if err := store.CommitSuccess(ctx, lease, StepSuccess{SideEffect: true}); err == nil {
			t.Fatal("missing decision was accepted")
		}
		assertSecurityCommitRolledBack(t, admin, lease)
	})
	t.Run("revoked before commit", func(t *testing.T) {
		lease := securityLease(t, admin, "t-sec-revoked-"+uuid.NewString()[:8])
		service := commitSecurityService(admin, security.Allow)
		decision := authorizeSecurityLease(t, admin, service, lease)
		if _, err := admin.pool.Exec(ctx, `INSERT INTO security_revocation_scope
			(tenant_id,scope_type,scope_id,version,updated_at) VALUES ($1,'run',$2,1,now())`, lease.TenantID, lease.RunID); err != nil {
			t.Fatal(err)
		}
		store := New(admin.pool, Config{CommitGuard: NewSecurityCommitGuard(service)})
		var securityErr *security.Error
		err := store.CommitSuccess(ctx, lease, StepSuccess{SideEffect: true, SecurityDecisionID: decision.DecisionID})
		if !errors.As(err, &securityErr) || securityErr.Code != security.Revoked {
			t.Fatalf("err=%v", err)
		}
		assertSecurityCommitRolledBack(t, admin, lease)
	})
	t.Run("expired", func(t *testing.T) {
		lease := securityLease(t, admin, "t-sec-expired-"+uuid.NewString()[:8])
		service := commitSecurityService(admin, security.Allow)
		decision := authorizeSecurityLease(t, admin, service, lease)
		service.Now = func() time.Time { return time.Now().Add(2 * time.Minute) }
		store := New(admin.pool, Config{CommitGuard: NewSecurityCommitGuard(service)})
		var securityErr *security.Error
		err := store.CommitSuccess(ctx, lease, StepSuccess{SideEffect: true, SecurityDecisionID: decision.DecisionID})
		if !errors.As(err, &securityErr) || securityErr.Code != security.DecisionExpired {
			t.Fatalf("err=%v", err)
		}
		assertSecurityCommitRolledBack(t, admin, lease)
	})
	t.Run("tampered request", func(t *testing.T) {
		lease := securityLease(t, admin, "t-sec-tampered-"+uuid.NewString()[:8])
		service := commitSecurityService(admin, security.Allow)
		decision := authorizeSecurityLease(t, admin, service, lease)
		if _, err := admin.pool.Exec(ctx, `UPDATE security_decision_audit SET request_json=jsonb_set(request_json,'{action_id}','"other"')
			WHERE tenant_id=$1 AND decision_id=$2`, lease.TenantID, decision.DecisionID); err != nil {
			t.Fatal(err)
		}
		store := New(admin.pool, Config{CommitGuard: NewSecurityCommitGuard(service)})
		if err := store.CommitSuccess(ctx, lease, StepSuccess{SideEffect: true, SecurityDecisionID: decision.DecisionID}); err == nil {
			t.Fatal("tampered request was accepted")
		}
		assertSecurityCommitRolledBack(t, admin, lease)
	})
}

func TestSecurityCommitApprovalPostgres(t *testing.T) {
	admin, _, _ := pgTestStores(t)
	ctx := context.Background()
	for _, revoked := range []bool{false, true} {
		t.Run(map[bool]string{false: "granted", true: "revoked after approval"}[revoked], func(t *testing.T) {
			lease := securityLease(t, admin, "t-sec-approval-"+uuid.NewString()[:8])
			service := commitSecurityService(admin, security.RequireApproval)
			decision := authorizeSecurityLease(t, admin, service, lease)
			consumeSecurityApproval(t, admin, lease)
			if revoked {
				if _, err := admin.pool.Exec(ctx, `INSERT INTO security_revocation_scope
					(tenant_id,scope_type,scope_id,version,updated_at) VALUES ($1,'run',$2,1,now())`, lease.TenantID, lease.RunID); err != nil {
					t.Fatal(err)
				}
			}
			store := New(admin.pool, Config{CommitGuard: NewSecurityCommitGuard(service)})
			err := store.CommitSuccess(ctx, lease, StepSuccess{SideEffect: true, SecurityDecisionID: decision.DecisionID})
			if revoked && err == nil {
				t.Fatal("revoked approved action was committed")
			}
			if !revoked && err != nil {
				t.Fatal(err)
			}
			if revoked {
				assertSecurityCommitRolledBack(t, admin, lease)
			}
		})
	}
	t.Run("missing grant", func(t *testing.T) {
		lease := securityLease(t, admin, "t-sec-no-grant-"+uuid.NewString()[:8])
		service := commitSecurityService(admin, security.RequireApproval)
		decision := authorizeSecurityLease(t, admin, service, lease)
		store := New(admin.pool, Config{CommitGuard: NewSecurityCommitGuard(service)})
		var securityErr *security.Error
		err := store.CommitSuccess(ctx, lease, StepSuccess{SideEffect: true, SecurityDecisionID: decision.DecisionID})
		if !errors.As(err, &securityErr) || securityErr.Code != security.ApprovalRequired {
			t.Fatalf("err=%v", err)
		}
		assertSecurityCommitRolledBack(t, admin, lease)
	})
}
