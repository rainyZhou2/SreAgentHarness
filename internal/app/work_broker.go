package app

import (
	"fmt"

	"go-sre-harness/internal/notify"
	"go-sre-harness/internal/plugin"
)

func workSubscriberFor(mode string, broker plugin.MessageBroker, enabled bool) (notify.WorkSubscriber, error) {
	if !enabled || (mode != "all" && mode != "scheduler") {
		return nil, nil
	}
	subscriber, ok := broker.(notify.WorkSubscriber)
	if !ok {
		return nil, fmt.Errorf("work engine in %s role requires broker capability notify.WorkSubscriber", mode)
	}
	return subscriber, nil
}
