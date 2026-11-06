package domain

import (
	"errors"
	"fmt"
	"time"
)

// Claim / commit sentinels (§15.2).
var (
	ErrNoRunnableStep  = errors.New("no runnable step")      // duplicate wake, paused, waiting, terminal...
	ErrLeaseHeld       = errors.New("lease held by another") // another worker owns a live lease
	ErrLeaseLost       = errors.New("lease lost")            // fence no longer matches; never commit
	ErrNotFound        = errors.New("not found")
	ErrVersionConflict = errors.New("version conflict")
	ErrInvalidCommand  = errors.New("invalid command")
	ErrUnknownEffect   = errors.New("external effect outcome unknown")
	// ErrNotDispatched is returned by a tool that rejected the call before
	// anything was sent to the external system (validation, allowlist,
	// breaker open). The step fails without entering UNKNOWN.
	ErrNotDispatched = errors.New("tool call rejected before dispatch")
)

// RetryableError marks a failure that is safe to retry (no side effect, or
// idempotency guaranteed). RetryAfter is a hint; zero means default backoff.
type RetryableError struct {
	Err        error
	RetryAfter time.Duration
}

func (e *RetryableError) Error() string { return fmt.Sprintf("retryable: %v", e.Err) }
func (e *RetryableError) Unwrap() error { return e.Err }

func Retryable(err error, after time.Duration) error {
	return &RetryableError{Err: err, RetryAfter: after}
}

// RejectedError carries a domain reason for a rejected command.
type RejectedError struct {
	Reason string
	Cmd    Command
}

func (e *RejectedError) Error() string { return "command rejected: " + e.Reason }
