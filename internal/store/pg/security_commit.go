package pg

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/security"
)

type SecurityCommitGuard struct {
	Service *security.Service
}

func NewSecurityCommitGuard(service *security.Service) *SecurityCommitGuard {
	return &SecurityCommitGuard{Service: service}
}

func (g *SecurityCommitGuard) CheckCommit(ctx context.Context, tx pgx.Tx, lease domain.Lease, decisionID string) error {
	if g == nil || g.Service == nil {
		return &security.Error{Code: security.PolicyUnavailable, Message: "security commit guard is not configured"}
	}
	if decisionID == "" || lease.Intent == nil {
		return &security.Error{Code: security.InvalidRequest, Message: "side-effect commit requires an action decision"}
	}
	var requestJSON, decisionJSON []byte
	err := tx.QueryRow(ctx, `SELECT request_json,decision_json FROM security_decision_audit
		WHERE tenant_id=$1 AND decision_id=$2`, lease.TenantID, decisionID).Scan(&requestJSON, &decisionJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return &security.Error{Code: security.InvalidRequest, Message: "security decision audit was not found"}
	}
	if err != nil {
		return &security.Error{Code: security.PolicyUnavailable, Message: "security decision audit is unavailable", Err: err}
	}
	var request security.Request
	var decision security.PolicyDecision
	if err := json.Unmarshal(requestJSON, &request); err != nil {
		return &security.Error{Code: security.PolicyUnavailable, Message: "security decision request is invalid", Err: err}
	}
	if err := json.Unmarshal(decisionJSON, &decision); err != nil {
		return &security.Error{Code: security.PolicyUnavailable, Message: "security decision is invalid", Err: err}
	}
	if err := validateSecurityCommitBinding(lease, request); err != nil {
		return err
	}
	if decision.Mode != string(security.ModeEnforce) {
		return &security.Error{Code: security.InvalidRequest, Message: "security decision was not issued in enforce mode"}
	}
	service := *g.Service
	service.Approvals = transactionApprovalVerifier{tx: tx, lease: lease}
	if err := service.RecheckUsing(ctx, request, decision, transactionRevocationReader{tx: tx}); err != nil {
		return err
	}
	return nil
}

func validateSecurityCommitBinding(lease domain.Lease, request security.Request) error {
	subject := lease.Subject
	if subject == "" {
		subject = "run:" + lease.RunID
	}
	validOperation := request.Operation == security.ToolExecute || request.Operation == security.MCPInvoke
	if lease.Intent == nil || !validOperation || request.TenantID != lease.TenantID || request.RunID != lease.RunID ||
		request.WorkID != lease.WorkID || request.Subject != subject || request.ActionID != lease.Intent.ID ||
		request.InputHash != lease.Intent.InputHash {
		return &security.Error{Code: security.InvalidRequest, Message: "security decision is not bound to the leased action"}
	}
	return nil
}

type transactionRevocationReader struct {
	tx pgx.Tx
}

func (r transactionRevocationReader) Stamps(ctx context.Context, request security.Request) ([]security.RevocationStamp, error) {
	rows, err := r.tx.Query(ctx, `SELECT scope_type,scope_id,version FROM security_revocation_scope
		WHERE tenant_id=$1 AND ((scope_type='tenant' AND scope_id=$1) OR (scope_type='subject' AND scope_id=$2)
		OR (scope_type='work' AND scope_id=$3) OR (scope_type='run' AND scope_id=$4))
		ORDER BY CASE scope_type WHEN 'tenant' THEN 0 WHEN 'subject' THEN 1 WHEN 'work' THEN 2 ELSE 3 END,scope_id`,
		request.TenantID, request.Subject, request.WorkID, request.RunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stamps []security.RevocationStamp
	for rows.Next() {
		var stamp security.RevocationStamp
		if err := rows.Scan(&stamp.Scope, &stamp.ScopeID, &stamp.Version); err != nil {
			return nil, err
		}
		stamps = append(stamps, stamp)
	}
	return stamps, rows.Err()
}

type transactionApprovalVerifier struct {
	tx    pgx.Tx
	lease domain.Lease
}

func (v transactionApprovalVerifier) VerifyApproval(ctx context.Context, request security.Request, _ int) error {
	var approved bool
	err := v.tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM approval_requests a
		JOIN approval_grants g ON g.approval_request_id=a.id
		LEFT JOIN step_attempts consumed ON consumed.id=g.consumed_by_attempt_id
		WHERE a.action_intent_id=$1 AND a.status='CONSUMED' AND g.action_hash=$2 AND g.used_count>=1
		AND g.expires_at>now() AND (g.consumed_by_attempt_id=$3 OR consumed.step_id=$4)
	)`, request.ActionID, v.lease.Intent.ActionHash, v.lease.AttemptID, v.lease.StepID).Scan(&approved)
	if err != nil {
		return &security.Error{Code: security.PolicyUnavailable, Message: "approval grant is unavailable", Err: err}
	}
	if !approved {
		return &security.Error{Code: security.ApprovalRequired, Message: "a valid consumed approval grant is required"}
	}
	return nil
}

type SecurityApprovalVerifier struct {
	Pool *pgxpool.Pool
}

func (v SecurityApprovalVerifier) VerifyApproval(ctx context.Context, request security.Request, _ int) error {
	if v.Pool == nil {
		return &security.Error{Code: security.PolicyUnavailable, Message: "approval grant reader is not configured"}
	}
	var approved bool
	err := v.Pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM approval_requests a JOIN approval_grants g ON g.approval_request_id=a.id
		WHERE a.tenant_id=$1 AND a.action_intent_id=$2 AND a.status='CONSUMED'
		AND g.action_hash=$3 AND g.used_count>=1 AND g.expires_at>now()
	)`, request.TenantID, request.ActionID, request.InputHash).Scan(&approved)
	if err != nil {
		return &security.Error{Code: security.PolicyUnavailable, Message: "approval grant is unavailable", Err: err}
	}
	if !approved {
		return &security.Error{Code: security.ApprovalRequired, Message: "a valid consumed approval grant is required"}
	}
	return nil
}
