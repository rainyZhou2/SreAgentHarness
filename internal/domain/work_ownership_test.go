package domain

import (
	"errors"
	"strconv"
	"testing"
)

func TestResolveRunWorkOwnership(t *testing.T) {
	parentID, actor := "parent", "work:legacy"
	for _, tc := range []struct {
		name  string
		root  Run
		owned bool
	}{
		{"owner", Run{WorkOwner: &WorkRunOwner{WorkID: "work", Generation: 1, StageID: "s"}}, true},
		{"legacy type", Run{InitiatorType: "work"}, true},
		{"legacy actor", Run{InitiatorID: &actor}, true},
		{"standalone", Run{InitiatorType: "system"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := tc.root
			root.ID, root.TenantID = parentID, "tenant"
			for _, run := range []Run{root, {ID: "child", TenantID: "tenant", ParentRunID: &parentID}} {
				got, err := ResolveRunWorkOwnership(run, func(string) (Run, error) { return root, nil })
				if err != nil || got != tc.owned {
					t.Fatalf("owned=%v err=%v", got, err)
				}
			}
		})
	}
}

func TestResolveRunWorkOwnershipRejectsBrokenAncestry(t *testing.T) {
	parentID := "parent"
	child := Run{ID: "child", TenantID: "tenant", ParentRunID: &parentID}
	for name, reader := range map[string]func(string) (Run, error){
		"missing":        func(string) (Run, error) { return Run{}, ErrNotFound },
		"foreign tenant": func(id string) (Run, error) { return Run{ID: id, TenantID: "other", InitiatorType: "work"}, nil },
		"cycle":          func(id string) (Run, error) { return Run{ID: id, TenantID: "tenant", ParentRunID: &parentID}, nil },
		"wrong identity": func(string) (Run, error) { return Run{ID: "unrelated", TenantID: "tenant"}, nil },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ResolveRunWorkOwnership(child, reader); err == nil {
				t.Fatal("accepted broken ancestry")
			}
		})
	}
	calls := 0
	_, err := ResolveRunWorkOwnership(child, func(id string) (Run, error) {
		calls++
		next := strconv.Itoa(calls)
		return Run{ID: id, TenantID: "tenant", ParentRunID: &next}, nil
	})
	var rejected *RejectedError
	if !errors.As(err, &rejected) || rejected.Reason != "run_ancestry_limit" || calls > 64 {
		t.Fatalf("unbounded ancestry: calls=%d err=%v", calls, err)
	}
}
