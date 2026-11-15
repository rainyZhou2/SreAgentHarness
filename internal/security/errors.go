package security

import "errors"

var ErrDecisionConflict = errors.New("security decision audit conflict")

type ErrorCode string

const (
	InvalidRequest    ErrorCode = "security_invalid_request"
	Denied            ErrorCode = "security_denied"
	ApprovalRequired  ErrorCode = "security_approval_required"
	PolicyUnavailable ErrorCode = "security_policy_unavailable"
	Revoked           ErrorCode = "security_revoked"
	DecisionExpired   ErrorCode = "security_decision_expired"
	ReplayReadOnly    ErrorCode = "replay_read_only"
)

type Error struct {
	Code    ErrorCode
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Message == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.Err }
