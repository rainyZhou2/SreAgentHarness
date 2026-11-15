package memory

import "testing"

func TestConflictPairIsSymmetricAndBounded(t *testing.T) {
	a := Memory{ID: "a", TenantID: "t", Scope: "service", ScopeID: "api"}
	b := Memory{ID: "b", TenantID: "t", Scope: "service", ScopeID: "api"}
	pair, err := NewConflictPair(a, b)
	if err != nil || pair.FirstID != "a" || pair.SecondID != "b" {
		t.Fatalf("pair=%+v err=%v", pair, err)
	}
	reversed, err := NewConflictPair(b, a)
	if err != nil || reversed != pair {
		t.Fatalf("reversed=%+v err=%v", reversed, err)
	}
	b.TenantID = "other"
	if _, err := NewConflictPair(a, b); err == nil {
		t.Fatal("cross-tenant conflict accepted")
	}
}

func TestResolveConflictsFailsClosedForWholeSubject(t *testing.T) {
	a := Memory{ID: "a", Scope: "service", ScopeID: "api", Subject: "cause", ContentHash: "one", Trust: "T1"}
	b := Memory{ID: "b", Scope: "service", ScopeID: "api", Subject: "cause", ContentHash: "two", Trust: "T4"}
	kept, conflicts := ResolveConflicts([]Memory{a, b})
	if len(kept) != 0 || len(conflicts["cause"]) != 2 {
		t.Fatalf("kept=%+v conflicts=%+v", kept, conflicts)
	}
	kept, conflicts = ResolveConflicts([]Memory{a, a})
	if len(kept) != 1 || len(conflicts) != 0 {
		t.Fatalf("identical content must not conflict: kept=%+v conflicts=%+v", kept, conflicts)
	}
}

func TestSupersessionRequiresPublishedSameSubject(t *testing.T) {
	old := Memory{ID: "old", TenantID: "t", Scope: "service", ScopeID: "api", Subject: "cause"}
	current := Memory{ID: "new", TenantID: "t", Scope: "service", ScopeID: "api", Subject: "cause", Type: "procedural", Status: Approved}
	if err := ValidateSupersession(old, current); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Memory){
		func(m *Memory) { m.TenantID = "other" },
		func(m *Memory) { m.ScopeID = "other" },
		func(m *Memory) { m.Subject = "other" },
		func(m *Memory) { m.Status = Verified },
	} {
		next := current
		mutate(&next)
		if err := ValidateSupersession(old, next); err == nil {
			t.Fatal("invalid supersession accepted")
		}
	}
}
