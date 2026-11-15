package app

import (
	"context"
	"strings"
	"testing"

	"go-sre-harness/internal/notify"
)

type runOnlyBroker struct{}

func (runOnlyBroker) Publish(_ context.Context, _, _ string, _ []byte) error { return nil }
func (runOnlyBroker) Run(context.Context, notify.Handler) error              { return nil }

func TestWorkSubscriberRoleContract(t *testing.T) {
	local := notify.NewLocal(1)
	for _, mode := range []string{"all", "scheduler"} {
		sub, err := workSubscriberFor(mode, local, true)
		if err != nil || sub == nil {
			t.Fatalf("mode=%s subscriber=%v err=%v", mode, sub, err)
		}
		if _, err := workSubscriberFor(mode, runOnlyBroker{}, true); err == nil || !strings.Contains(err.Error(), "WorkSubscriber") {
			t.Fatalf("mode=%s expected capability error, got %v", mode, err)
		}
	}
	sub, err := workSubscriberFor("api", runOnlyBroker{}, true)
	if err != nil || sub != nil {
		t.Fatalf("api subscriber=%v err=%v", sub, err)
	}
	sub, err = workSubscriberFor("all", runOnlyBroker{}, false)
	if err != nil || sub != nil {
		t.Fatalf("disabled subscriber=%v err=%v", sub, err)
	}
}
