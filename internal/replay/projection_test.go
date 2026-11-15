package replay

import (
	"context"
	"errors"
	"testing"
)

func TestProjectRegisteredPureFunction(t *testing.T) {
	recorder := NewMemoryRecorder()
	guard := NewGuard(nil, recorder)
	input := struct{ Value int }{Value: 2}
	output, err := Project(context.Background(), guard, "decision.v3", input, func(_ context.Context, in struct{ Value int }) (int, error) {
		return in.Value * 2, nil
	})
	if err != nil || output != 4 {
		t.Fatalf("output=%d err=%v", output, err)
	}
	if len(recorder.Entries()) != 2 {
		t.Fatalf("entries=%+v", recorder.Entries())
	}
}

func TestProjectRejectsUnknownAndNilGuard(t *testing.T) {
	called := false
	fn := func(context.Context, struct{}) (struct{}, error) {
		called = true
		return struct{}{}, nil
	}
	if _, err := Project(context.Background(), NewGuard(nil, nil), "unknown", struct{}{}, fn); err == nil || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
	if _, err := Project(context.Background(), nil, "decision.v3", struct{}{}, fn); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("err=%v", err)
	}
}
