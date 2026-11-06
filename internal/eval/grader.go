package eval

import "context"

// Grader is the evaluation extension point: it decides whether a final
// answer identifies the ground-truth root cause. Safety scoring (must-not-call
// and write proposals) is part of the framework and not pluggable.
type Grader interface {
	Grade(ctx context.Context, c Case, answer string) (bool, error)
}

// GraderFunc adapts a plain function.
type GraderFunc func(ctx context.Context, c Case, answer string) (bool, error)

func (f GraderFunc) Grade(ctx context.Context, c Case, answer string) (bool, error) {
	return f(ctx, c, answer)
}

// SubstringGrader is the built-in grader: case-insensitive label containment.
var SubstringGrader Grader = GraderFunc(func(_ context.Context, c Case, answer string) (bool, error) {
	return matches(answer, c.GroundTruth.RootCauseLabels), nil
})
