package revocation

import (
	"context"
	"errors"
	"strings"
	"time"

	"go-sre-harness/internal/security"
)

var (
	ErrInvalidRevocation     = errors.New("invalid security revocation")
	ErrRevocationConflict    = errors.New("security revocation idempotency conflict")
	ErrRevocationUnavailable = errors.New("security revocation store unavailable")
)

type Event struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	Scope          string    `json:"scope"`
	ScopeID        string    `json:"scope_id"`
	Version        int64     `json:"version"`
	IdempotencyKey string    `json:"-"`
	ActorID        string    `json:"actor_id"`
	Reason         string    `json:"reason"`
	CreatedAt      time.Time `json:"created_at"`
}

type Query struct {
	TenantID string
	Subject  string
	WorkID   string
	RunID    string
}

type Store interface {
	Revoke(ctx context.Context, event Event) (Event, error)
	Stamps(ctx context.Context, query Query) ([]security.RevocationStamp, error)
}

func (e Event) Normalize() (Event, error) {
	e.TenantID = strings.TrimSpace(e.TenantID)
	e.Scope = strings.TrimSpace(e.Scope)
	e.ScopeID = strings.TrimSpace(e.ScopeID)
	e.IdempotencyKey = strings.TrimSpace(e.IdempotencyKey)
	e.ActorID = strings.TrimSpace(e.ActorID)
	e.Reason = strings.TrimSpace(e.Reason)
	if e.TenantID == "" || len(e.TenantID) > 200 || e.IdempotencyKey == "" || len(e.IdempotencyKey) > 200 ||
		e.ActorID == "" || len(e.ActorID) > 200 || e.Reason == "" || len(e.Reason) > 1000 {
		return e, ErrInvalidRevocation
	}
	switch e.Scope {
	case "tenant":
		if e.ScopeID == "" {
			e.ScopeID = e.TenantID
		}
		if e.ScopeID != e.TenantID {
			return e, ErrInvalidRevocation
		}
	case "subject", "work", "run":
		if e.ScopeID == "" {
			return e, ErrInvalidRevocation
		}
	default:
		return e, ErrInvalidRevocation
	}
	if len(e.ScopeID) > 200 {
		return e, ErrInvalidRevocation
	}
	return e, nil
}

func (q Query) Validate() error {
	q.TenantID = strings.TrimSpace(q.TenantID)
	if q.TenantID == "" || len(q.TenantID) > 200 || len(q.Subject) > 200 || len(q.WorkID) > 200 || len(q.RunID) > 200 {
		return ErrInvalidRevocation
	}
	return nil
}
