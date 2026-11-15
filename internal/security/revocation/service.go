package revocation

import "context"

type Invalidator interface {
	Invalidate(tenantID, scopeID string)
}

type Notifier interface {
	Notify(ctx context.Context, event Event) error
}

type NotifyFunc func(ctx context.Context, event Event) error

func (f NotifyFunc) Notify(ctx context.Context, event Event) error { return f(ctx, event) }

type NotificationObserver interface {
	NotificationFailed(scope string)
}

type Service struct {
	Store    Store
	Cache    Invalidator
	Notifier Notifier
	Observer NotificationObserver
}

func (s *Service) Revoke(ctx context.Context, event Event) (Event, error) {
	if s.Store == nil {
		return Event{}, ErrRevocationUnavailable
	}
	committed, err := s.Store.Revoke(ctx, event)
	if err != nil {
		return Event{}, err
	}
	if s.Cache != nil {
		s.Cache.Invalidate(committed.TenantID, committed.ScopeID)
	}
	if s.Notifier != nil {
		if err := s.Notifier.Notify(ctx, committed); err != nil && s.Observer != nil {
			s.Observer.NotificationFailed(committed.Scope)
		}
	}
	return committed, nil
}
