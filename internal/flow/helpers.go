package flow

// Truthy reports the expression-language truth value of v (used by callers
// that evaluate expressions outside a flow: triggers, tool lifecycles).
func Truthy(v any) bool { return truthy(v) }

// Stringify renders v the way ${...} interpolation does.
func Stringify(v any) string { return stringify(v) }
