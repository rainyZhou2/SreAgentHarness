package memory

const (
	StatusCandidate = "CANDIDATE"
	Verified        = "VERIFIED"
	Approved        = "APPROVED"
	Contested       = "CONTESTED"
	Stale           = "STALE"
	Revoked         = "REVOKED"
	Superseded      = "SUPERSEDED"
)

func RequiresApproval(kind string) bool {
	return kind == "procedural"
}

func RecallStatus(status, kind string) bool {
	if RequiresApproval(kind) {
		return status == Approved
	}
	return status == Verified || status == Approved
}

func CanTransition(from, to string) bool {
	switch from {
	case StatusCandidate:
		return to == Verified || to == Revoked
	case Verified:
		return to == Approved || to == Contested || to == Stale || to == Revoked || to == Superseded
	case Approved:
		return to == Contested || to == Stale || to == Revoked || to == Superseded
	case Contested, Stale:
		return to == Verified || to == Revoked || to == Superseded
	default:
		return false
	}
}
