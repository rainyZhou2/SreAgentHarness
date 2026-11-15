package replay

import "go-sre-harness/internal/security"

var ErrReadOnly = &security.Error{Code: security.ReplayReadOnly, Message: "replay is read-only"}

type Guard struct {
	reads    security.Authorizer
	recorder Recorder
}

func NewGuard(reads security.Authorizer, recorder Recorder) *Guard {
	if recorder == nil {
		recorder = NewMemoryRecorder()
	}
	return &Guard{reads: reads, recorder: recorder}
}

func (g *Guard) Authorizer() security.Authorizer {
	return authorizer{reads: g.reads, recorder: g.recorder}
}
