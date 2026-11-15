package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/security"
	"go-sre-harness/internal/security/revocation"
)

var _ revocation.Store = (*Store)(nil)

func (s *Store) Revoke(ctx context.Context, event revocation.Event) (revocation.Event, error) {
	return s.RevokeSecurity(ctx, event)
}

func (s *Store) Stamps(ctx context.Context, query revocation.Query) ([]security.RevocationStamp, error) {
	return s.SecurityStamps(ctx, query)
}

func (s *Store) RevokeSecurity(ctx context.Context, event revocation.Event) (revocation.Event, error) {
	event, err := event.Normalize()
	if err != nil {
		return revocation.Event{}, err
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = s.now()
	}
	event.ID = cpkit.NewID()
	var result revocation.Event
	err = s.tx(ctx, func(tx *sql.Tx) error {
		inserted, err := tx.ExecContext(ctx, s.q(`INSERT INTO security_revocation_request
			(tenant_id,idempotency_key,event_id,created_at) VALUES (?,?,?,?)
			ON CONFLICT (tenant_id,idempotency_key) DO NOTHING`),
			event.TenantID, event.IdempotencyKey, event.ID, s.t(event.CreatedAt))
		if err != nil {
			return err
		}
		count, err := inserted.RowsAffected()
		if err != nil {
			return err
		}
		if count == 0 {
			existing, err := scanRevocationEvent(tx.QueryRowContext(ctx, s.q(`SELECT e.id,e.tenant_id,e.scope_type,e.scope_id,
				e.scope_version,e.actor_id,e.reason,e.created_at FROM security_revocation_request r
				JOIN security_revocation_event e ON e.tenant_id=r.tenant_id AND e.id=r.event_id
				WHERE r.tenant_id=? AND r.idempotency_key=?`), event.TenantID, event.IdempotencyKey))
			if err != nil {
				return err
			}
			if existing.TenantID != event.TenantID || existing.Scope != event.Scope || existing.ScopeID != event.ScopeID ||
				existing.ActorID != event.ActorID || existing.Reason != event.Reason {
				return revocation.ErrRevocationConflict
			}
			result = existing
			return nil
		}
		if err := tx.QueryRowContext(ctx, s.q(`INSERT INTO security_revocation_scope
			(tenant_id,scope_type,scope_id,version,updated_at) VALUES (?,?,?,1,?)
			ON CONFLICT (tenant_id,scope_type,scope_id) DO UPDATE SET
			version=security_revocation_scope.version+1,updated_at=excluded.updated_at RETURNING version`),
			event.TenantID, event.Scope, event.ScopeID, s.t(event.CreatedAt)).Scan(&event.Version); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO security_revocation_event
			(tenant_id,id,scope_type,scope_id,scope_version,actor_id,reason,created_at) VALUES (?,?,?,?,?,?,?,?)`),
			event.TenantID, event.ID, event.Scope, event.ScopeID, event.Version, event.ActorID, event.Reason, s.t(event.CreatedAt)); err != nil {
			return err
		}
		if s.d == Postgres {
			payload, err := securityNotificationPayload(SecurityNotification{TenantID: event.TenantID, Scope: event.Scope, ScopeID: event.ScopeID, Version: event.Version})
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, s.q(`SELECT pg_notify('harness_security_revocation', ?)`), payload); err != nil {
				return err
			}
		}
		result = event
		return nil
	})
	if err != nil {
		if errors.Is(err, revocation.ErrRevocationConflict) {
			return revocation.Event{}, err
		}
		return revocation.Event{}, fmt.Errorf("%w: %v", revocation.ErrRevocationUnavailable, err)
	}
	return result, nil
}

func (s *Store) SecurityStamps(ctx context.Context, query revocation.Query) ([]security.RevocationStamp, error) {
	if err := query.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT scope_type,scope_id,version FROM security_revocation_scope
		WHERE tenant_id=? AND ((scope_type='tenant' AND scope_id=?) OR (scope_type='subject' AND scope_id=?)
		OR (scope_type='work' AND scope_id=?) OR (scope_type='run' AND scope_id=?))
		ORDER BY CASE scope_type WHEN 'tenant' THEN 0 WHEN 'subject' THEN 1 WHEN 'work' THEN 2 ELSE 3 END,scope_id`),
		query.TenantID, query.TenantID, query.Subject, query.WorkID, query.RunID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", revocation.ErrRevocationUnavailable, err)
	}
	defer rows.Close()
	var stamps []security.RevocationStamp
	for rows.Next() {
		var stamp security.RevocationStamp
		if err := rows.Scan(&stamp.Scope, &stamp.ScopeID, &stamp.Version); err != nil {
			return nil, fmt.Errorf("%w: %v", revocation.ErrRevocationUnavailable, err)
		}
		stamps = append(stamps, stamp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", revocation.ErrRevocationUnavailable, err)
	}
	return stamps, nil
}

func scanRevocationEvent(scanner rowScanner) (revocation.Event, error) {
	var event revocation.Event
	var created ts
	err := scanner.Scan(&event.ID, &event.TenantID, &event.Scope, &event.ScopeID, &event.Version, &event.ActorID, &event.Reason, &created)
	if err != nil {
		return event, err
	}
	event.CreatedAt = created.T
	return event, nil
}

type SecurityRecorder struct {
	store *Store
}

func NewSecurityRecorder(store *Store) *SecurityRecorder {
	return &SecurityRecorder{store: store}
}

func (r *SecurityRecorder) RecordDecision(ctx context.Context, request security.Request, decision security.PolicyDecision) error {
	return r.store.RecordSecurityDecision(ctx, request, decision)
}

func (s *Store) RecordSecurityDecision(ctx context.Context, request security.Request, decision security.PolicyDecision) error {
	if request.TenantID == "" || request.RequestID == "" || decision.DecisionID == "" {
		return &security.Error{Code: security.InvalidRequest, Message: "decision audit identity is incomplete"}
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return &security.Error{Code: security.InvalidRequest, Message: "decision request cannot be encoded", Err: err}
	}
	decisionJSON, err := json.Marshal(decision)
	if err != nil {
		return &security.Error{Code: security.InvalidRequest, Message: "decision cannot be encoded", Err: err}
	}
	createdAt := decision.DecidedAt
	if createdAt.IsZero() {
		createdAt = s.now()
	}
	result, err := s.db.ExecContext(ctx, s.q(`INSERT INTO security_decision_audit
		(tenant_id,decision_id,request_id,operation,effect,mode,request_json,decision_json,created_at)
		VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT (tenant_id,decision_id) DO NOTHING`),
		request.TenantID, decision.DecisionID, request.RequestID, request.Operation, decision.Effect, decision.Mode,
		string(requestJSON), string(decisionJSON), s.t(createdAt))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 1 {
		return nil
	}
	var existingRequest, existingDecision jb
	if err := s.db.QueryRowContext(ctx, s.q(`SELECT request_json,decision_json FROM security_decision_audit
		WHERE tenant_id=? AND decision_id=?`), request.TenantID, decision.DecisionID).Scan(&existingRequest, &existingDecision); err != nil {
		return err
	}
	if !equalJSON(existingRequest, requestJSON) || !equalJSON(existingDecision, decisionJSON) {
		return security.ErrDecisionConflict
	}
	return nil
}

func equalJSON(left, right []byte) bool {
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}
