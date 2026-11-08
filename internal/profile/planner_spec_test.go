package profile

import "testing"

func TestPlannerSpecValidate(t *testing.T) {
	six := 6
	cases := []struct {
		spec PlannerSpec
		ok   bool
	}{
		{PlannerSpec{ApprovalMode: "always", ModelLadder: []string{"small", "large"}}, true},
		{PlannerSpec{ApprovalMode: "sometimes"}, false},
		{PlannerSpec{ModelAlias: "secret"}, false},
		{PlannerSpec{MaxRepairs: &six}, false},
		{PlannerSpec{Fallbacks: []PlannerFallback{{GoalPattern: "(", ProfileID: "x", Version: 1}}}, false},
	}
	for i, c := range cases {
		if err := c.spec.Validate([]string{"small", "large"}); (err == nil) != c.ok {
			t.Errorf("case %d: err=%v want ok=%v", i, err, c.ok)
		}
	}
}
