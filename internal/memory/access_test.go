package memory

import "testing"

func TestAuthorizeRead(t *testing.T) {
	owner := AccessContext{TenantID: "t", Subjects: []string{"user:alice"}}
	role := AccessContext{TenantID: "t", Subjects: []string{"user:bob", "role:sre"}}
	admin := AccessContext{TenantID: "t", Subjects: []string{"role:admin"}}
	grants := []ACLGrant{{TenantID: "t", MemoryID: "m", Subject: "user:alice"},
		{TenantID: "t", MemoryID: "m", Subject: "role:sre"}}
	for _, tc := range []struct {
		name string
		ctx  AccessContext
		want bool
	}{
		{"owner", owner, true},
		{"role", role, true},
		{"admin", admin, true},
		{"empty", AccessContext{TenantID: "t"}, false},
		{"foreign", AccessContext{TenantID: "other", Subjects: []string{"user:alice"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := AuthorizeRead(tc.ctx, Memory{ID: "m", TenantID: "t"}, grants); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
	if AuthorizeRead(owner, Memory{ID: "m", TenantID: "t"}, nil) {
		t.Fatal("empty ACL must deny")
	}
	if AuthorizeRead(owner, Memory{ID: "other", TenantID: "t"}, grants) {
		t.Fatal("grant for another memory must deny")
	}
}

func TestAccessContextRoundTrip(t *testing.T) {
	want := AccessContext{TenantID: "t", Subjects: []string{"user:alice", "role:viewer"}}
	ctx := WithAccessContext(t.Context(), want)
	got, ok := AccessFrom(ctx)
	if !ok || got.TenantID != want.TenantID || len(got.Subjects) != 2 {
		t.Fatalf("got %+v, %v", got, ok)
	}
}
