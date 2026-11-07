package localstore

import (
	"context"
	"encoding/json"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/scheduler"
)

var _ api.Store = (*Store)(nil)
var _ kernel.StateStore = (*Store)(nil)
var _ kernel.ConversationReader = (*Store)(nil)
var _ kernel.FlowStore = (*Store)(nil)
var _ profile.Loader = (*Store)(nil)
var _ scheduler.OutboxStore = (*Store)(nil)
var _ scheduler.ReconcileStore = (*Store)(nil)

// profileLoader is the compile-time shape of ProfileSpec; the assertion above
// catches signature drift.
type profileLoader interface {
	ProfileSpec(ctx context.Context, tenantID, profileID string, version int64) (json.RawMessage, error)
}

var _ profileLoader = (*Store)(nil)
