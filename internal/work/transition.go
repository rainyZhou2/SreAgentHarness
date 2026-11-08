package work

// edges is the allowed state graph (RFC §5.3, plus the orchestration edges
// it implies). SUCCEEDED is reachable only from VERIFYING, and only with an
// outcome record (enforced by CheckMutation).
var edges = map[Status][]Status{
	StatusNew:       {StatusPlanning, StatusActive, StatusCancelled},
	StatusPlanning:  {StatusActive, StatusBlocked, StatusCancelled, StatusFailed},
	StatusActive:    {StatusWaiting, StatusVerifying, StatusBlocked, StatusSuspended, StatusCancelled, StatusFailed, StatusPlanning},
	StatusWaiting:   {StatusActive, StatusVerifying, StatusBlocked, StatusSuspended, StatusCancelled, StatusFailed},
	StatusVerifying: {StatusSucceeded, StatusActive, StatusWaiting, StatusBlocked, StatusFailed, StatusSuspended, StatusCancelled},
	StatusBlocked:   {StatusActive, StatusFailed, StatusCancelled, StatusSuspended},
	StatusSuspended: {StatusActive, StatusCancelled},
	// Terminal states only leave through an explicit reopen, which starts a
	// new generation (CmdReopen); it is never implicit.
	StatusSucceeded: {},
	StatusFailed:    {},
	StatusCancelled: {},
}

// CanTransition reports whether from → to is allowed (same status is a
// no-op and always allowed for non-terminal states).
func CanTransition(from, to Status) bool {
	if from == to {
		return !from.Terminal()
	}
	for _, s := range edges[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Transition returns a *TransitionError unless from → to is allowed.
func Transition(from, to Status) error {
	if !CanTransition(from, to) {
		return &TransitionError{From: from, To: to}
	}
	return nil
}

// Operator command types (POST /v1/works/{id}/commands).
const (
	CmdPause   = "pause"   // → SUSPENDED
	CmdResume  = "resume"  // SUSPENDED → ACTIVE
	CmdCancel  = "cancel"  // → CANCELLED (owner runs are cancelled; dispatched effects are not undone)
	CmdReplan  = "replan"  // restart the current stage in a new generation
	CmdResolve = "resolve" // BLOCKED → ACTIVE after a human fixed the blocker
	CmdConfirm = "confirm" // record a human objective decision
	CmdReopen  = "reopen"  // terminal → ACTIVE, new generation, stage 0
	CmdWake    = "wake"    // ask the orchestrator to re-evaluate now
)

// ValidCommand reports whether t is an operator command.
func ValidCommand(t string) bool {
	switch t {
	case CmdPause, CmdResume, CmdCancel, CmdReplan, CmdResolve, CmdConfirm, CmdReopen, CmdWake:
		return true
	}
	return false
}

// TransitionFor is Transition for a mutation that may carry an operator
// command: a terminal Work leaves its state only through CmdReopen, which
// must move it to ACTIVE in a new generation.
func TransitionFor(from, to Status, cmd string, newGeneration bool) error {
	if cmd == CmdReopen && from.Terminal() {
		if to != StatusActive || !newGeneration {
			return invalidf("reopen must move the Work to ACTIVE in a new generation")
		}
		return nil
	}
	return Transition(from, to)
}
