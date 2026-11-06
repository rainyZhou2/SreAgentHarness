package kernel

import (
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"go-sre-harness/internal/domain"
)

func obsTracer() trace.Tracer { return otel.Tracer("go-sre-harness/kernel") }

func claimResult(err error) string {
	switch {
	case errors.Is(err, domain.ErrNoRunnableStep):
		return "no_work"
	case errors.Is(err, domain.ErrLeaseHeld):
		return "lease_held"
	case errors.Is(err, domain.ErrNotFound):
		return "not_found"
	default:
		return "error"
	}
}

func failureOutcome(c domain.FailureClass) string {
	switch c {
	case domain.FailRetryable:
		return "retryable"
	case domain.FailPermanent:
		return "failed"
	case domain.FailUnknownEffect:
		return "unknown_effect"
	case domain.FailCancelled:
		return "cancelled"
	}
	return "error"
}
