package cpkit

import "context"

// RequestContext is the identity of one request as the server established
// it (design §15.2): who asks, for which tenant, on behalf of which Work
// and run. It carries identity, tracing and request metadata only; business
// values (goals, subjects, arguments) are passed as explicit parameters.
//
// Every field is set by the server from the authenticated principal or the
// kernel's call info. Nothing here is ever read from a request body, a URL
// parameter or a tool argument.
type RequestContext struct {
	TenantID      string `json:"tenant_id"`
	PrincipalID   string `json:"principal_id,omitempty"`
	PrincipalType string `json:"principal_type,omitempty"` // user | service | agent | system
	WorkID        string `json:"work_id,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	TraceID       string `json:"trace_id,omitempty"`
	RequestID     string `json:"request_id,omitempty"`
	SourceID      string `json:"source_id,omitempty"`
	// AuthzScopeHash identifies the scope the principal was authorized with
	// (audit: two requests with the same hash were bound by the same scope).
	AuthzScopeHash string `json:"authz_scope_hash,omitempty"`
	// DataClass is the most sensitive data class the caller may receive.
	DataClass string `json:"data_class,omitempty"`
}

type requestCtxKey struct{}

// WithRequest attaches rc to ctx.
func WithRequest(ctx context.Context, rc RequestContext) context.Context {
	return context.WithValue(ctx, requestCtxKey{}, rc)
}

// RequestFrom returns the request context attached to ctx.
func RequestFrom(ctx context.Context) (RequestContext, bool) {
	rc, ok := ctx.Value(requestCtxKey{}).(RequestContext)
	return rc, ok
}

// DataClassRank orders data classes: public < internal < confidential <
// restricted. Unknown classes rank as restricted (fail closed).
func DataClassRank(class string) int {
	switch class {
	case "public":
		return 0
	case "", "internal":
		return 1
	case "confidential":
		return 2
	}
	return 3
}
