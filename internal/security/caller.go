package security

import "context"

type Caller struct {
	TenantID    string
	Subject     string
	WorkID      string
	RunID       string
	Environment string
}

type callerKey struct{}

func WithCaller(ctx context.Context, caller Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

func CallerFrom(ctx context.Context) (Caller, bool) {
	caller, ok := ctx.Value(callerKey{}).(Caller)
	return caller, ok && caller.TenantID != "" && caller.Subject != ""
}
