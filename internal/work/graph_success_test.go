package work

import (
	"errors"
	"go-sre-harness/internal/outcome"
	"testing"
)

func TestInheritedSuccessIsExplicitAndIndependent(t *testing.T) {
	goal := GoalSpec{Success: &outcome.Spec{AllOf: []outcome.Objective{{ID: "healthy", Kind: outcome.KindState, State: &outcome.StateExpr{Predicate: "ready", Equals: "yes"}}}}}
	if got, err := (ChildSpec{Key: "rca"}).InheritedSuccess(goal); err != nil || got != nil {
		t.Fatalf("implicit inheritance: %+v %v", got, err)
	}
	got, err := (ChildSpec{Key: "mitigation", InheritSuccess: true}).InheritedSuccess(goal)
	if err != nil {
		t.Fatal(err)
	}
	got.AllOf[0].State.Equals = "no"
	if goal.Success.AllOf[0].State.Equals != "yes" || goal.Success.Version != 0 {
		t.Fatal("copy mutated parent goal")
	}
}

func TestInheritedSuccessRequiresNonOptionalMachineObjective(t *testing.T) {
	goal := GoalSpec{Success: &outcome.Spec{AllOf: []outcome.Objective{{ID: "healthy", Kind: outcome.KindState, Optional: true, State: &outcome.StateExpr{Predicate: "ready", Equals: "yes"}}}}}
	if _, err := (ChildSpec{Key: "mitigation", InheritSuccess: true}).InheritedSuccess(goal); !errors.Is(err, ErrInvalid) {
		t.Fatalf("optional-only recovery goal accepted: %v", err)
	}
}
