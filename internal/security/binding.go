package security

import (
	"context"
	"sync"
)

type Binding struct {
	RequestID  string
	DecisionID string
	Mode       Mode
}

type DecisionSlot struct {
	mu      sync.RWMutex
	binding Binding
	bound   bool
}

type decisionSlotKey struct{}

func WithDecisionSlot(ctx context.Context) context.Context {
	return context.WithValue(ctx, decisionSlotKey{}, &DecisionSlot{})
}

func BindDecision(ctx context.Context, request Request, decision PolicyDecision) error {
	slot, ok := ctx.Value(decisionSlotKey{}).(*DecisionSlot)
	if !ok || slot == nil {
		if Mode(decision.Mode) == ModeEnforce {
			return &Error{Code: PolicyUnavailable, Message: "enforce decision binding slot is required"}
		}
		return nil
	}
	normalized := request
	if Mode(decision.Mode) == ModeEnforce && !decision.DecidedAt.IsZero() {
		var err error
		normalized, err = request.Normalize(decision.DecidedAt)
		if err != nil {
			return err
		}
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.bound || normalized.RequestID == "" || (Mode(decision.Mode) == ModeEnforce &&
		(decision.DecisionID == "" || decision.RequestHash == "" || decision.RequestHash != hashRequest(normalized))) {
		return &Error{Code: PolicyUnavailable, Message: "security decision binding is invalid or already written"}
	}
	slot.binding = Binding{RequestID: request.RequestID, DecisionID: decision.DecisionID, Mode: Mode(decision.Mode)}
	slot.bound = true
	return nil
}

func DecisionFrom(ctx context.Context) (Binding, bool) {
	slot, ok := ctx.Value(decisionSlotKey{}).(*DecisionSlot)
	if !ok || slot == nil {
		return Binding{}, false
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	return slot.binding, slot.bound
}
