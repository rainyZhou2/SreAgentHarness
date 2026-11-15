package security

import (
	"fmt"
	"testing"
	"time"
)

func TestConstraintsDeterministic(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	rule := ConstraintRule{Name: "prod-change", Version: "v3", Operations: []Operation{ToolExecute},
		Environments: []string{"production"}, Windows: []Window{{Days: []string{"mon"}, Start: "09:00", End: "17:00", Timezone: "Asia/Shanghai"}}, CanaryPercent: percent(25)}
	compiled, err := CompileConstraints([]ConstraintRule{rule})
	if err != nil {
		t.Fatal(err)
	}
	r := Request{Operation: ToolExecute, TenantID: "t", WorkID: "w", ResourceRef: "service://prod/api", Environment: "production"}
	for i := 0; i < 100; i++ {
		r.ActionID = fmt.Sprintf("action-%d", i)
		if canaryBucket(rule.Version, r.TenantID, r.WorkID, r.ActionID, r.ResourceRef) < 25 {
			break
		}
	}
	inside := time.Date(2026, 11, 16, 10, 0, 0, 0, loc)
	d1, err := compiled.Evaluate(r, inside)
	if err != nil || d1.Constraints.CanaryBucket == nil {
		t.Fatalf("constraint: %+v %v", d1, err)
	}
	d2, err := compiled.Evaluate(r, inside)
	if err != nil || *d1.Constraints.CanaryBucket != *d2.Constraints.CanaryBucket {
		t.Fatal("canary changed between evaluations")
	}
	outside := time.Date(2026, 11, 16, 18, 0, 0, 0, loc)
	denied, err := compiled.Evaluate(r, outside)
	if err != nil || denied.Effect != Deny {
		t.Fatalf("outside change window: %+v %v", denied, err)
	}
}

func TestCanaryBoundaries(t *testing.T) {
	for _, p := range []int{-1, 101} {
		if _, err := CompileConstraints([]ConstraintRule{{Name: "x", Version: "v", CanaryPercent: percent(p)}}); err == nil {
			t.Fatalf("accepted %d", p)
		}
	}
	if _, err := CompileConstraints([]ConstraintRule{{Name: "default", Version: "v"}}); err != nil {
		t.Fatalf("nil must mean 100: %v", err)
	}
	zero := ConstraintRule{Name: "zero", Version: "v", Operations: []Operation{ToolExecute}, CanaryPercent: percent(0)}
	compiled, err := CompileConstraints([]ConstraintRule{zero})
	if err != nil {
		t.Fatal(err)
	}
	d, err := compiled.Evaluate(Request{Operation: ToolExecute, TenantID: "t"}, time.Now())
	if err != nil || d.Effect != Deny {
		t.Fatalf("explicit zero must deny: %+v %v", d, err)
	}
	if got := canaryBucket("v1", "t", "w", "a", "service://prod/api"); got > 99 {
		t.Fatalf("bucket %d", got)
	}
}

func TestWindowCrossMidnight(t *testing.T) {
	w := Window{Days: []string{"mon"}, Start: "23:00", End: "02:00", Timezone: "Asia/Shanghai"}
	if err := w.Compile(); err != nil {
		t.Fatal(err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	if !w.Contains(time.Date(2026, 11, 17, 1, 0, 0, 0, loc)) {
		t.Fatal("Tuesday after midnight belongs to Monday window")
	}
	if w.Contains(time.Date(2026, 11, 17, 2, 0, 0, 0, loc)) {
		t.Fatal("window end must be exclusive")
	}
}

func percent(v int) *int { return &v }
