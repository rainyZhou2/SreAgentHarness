package revocation_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/security"
	"go-sre-harness/internal/security/revocation"
)

type serviceStore struct {
	event revocation.Event
	err   error
}

func (s *serviceStore) Revoke(_ context.Context, event revocation.Event) (revocation.Event, error) {
	if s.err != nil {
		return revocation.Event{}, s.err
	}
	event.ID = "018f0000-0000-7000-8000-000000000002"
	event.Version = 3
	s.event = event
	return event, nil
}

func (s *serviceStore) Stamps(context.Context, revocation.Query) ([]security.RevocationStamp, error) {
	return nil, nil
}

type invalidator struct {
	tenant string
	scope  string
	calls  int
}

func (i *invalidator) Invalidate(tenant, scope string) {
	i.tenant, i.scope, i.calls = tenant, scope, i.calls+1
}

type notifier struct {
	event revocation.Event
	err   error
}

func (n *notifier) Notify(_ context.Context, event revocation.Event) error {
	n.event = event
	return n.err
}

type notificationObserver struct {
	scope string
	calls int
}

func (o *notificationObserver) NotificationFailed(scope string) {
	o.scope, o.calls = scope, o.calls+1
}

func TestRevocationServiceCommittedBeforeNotification(t *testing.T) {
	store := &serviceStore{}
	cache := &invalidator{}
	sink := &notifier{err: errors.New("listener unavailable")}
	observer := &notificationObserver{}
	service := revocation.Service{Store: store, Cache: cache, Notifier: sink, Observer: observer}
	event, err := service.Revoke(context.Background(), revocation.Event{TenantID: "t", Scope: "work", ScopeID: "w", ActorID: "admin", Reason: "incident", CreatedAt: time.Now()})
	if err != nil || event.Version != 3 {
		t.Fatalf("notification failure rolled back revocation: %+v %v", event, err)
	}
	if cache.tenant != "t" || cache.scope != "w" || cache.calls != 1 || sink.event.ID != event.ID {
		t.Fatalf("cache=%+v notification=%+v", cache, sink.event)
	}
	if observer.scope != "work" || observer.calls != 1 {
		t.Fatalf("observer=%+v", observer)
	}
}

func TestRevocationServiceStoreFailureDoesNotNotify(t *testing.T) {
	store := &serviceStore{err: revocation.ErrRevocationUnavailable}
	cache := &invalidator{}
	sink := &notifier{}
	service := revocation.Service{Store: store, Cache: cache, Notifier: sink}
	_, err := service.Revoke(context.Background(), revocation.Event{TenantID: "t", Scope: "work", ScopeID: "w"})
	if !errors.Is(err, revocation.ErrRevocationUnavailable) || cache.calls != 0 || sink.event.ID != "" {
		t.Fatalf("err=%v cache=%+v notification=%+v", err, cache, sink)
	}
}
