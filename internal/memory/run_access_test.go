package memory

import (
	"context"
	"testing"
)

func TestRetrieverDerivesRunAccess(t *testing.T) {
	r := Retriever{Store: candidateStore{}}
	ctx := r.withRunAccess(context.Background(), "t", "run-1")
	access, ok := AccessFrom(ctx)
	if !ok || access.TenantID != "t" {
		t.Fatalf("access=%+v ok=%v", access, ok)
	}
	for _, subject := range []string{"role:viewer", "service:run:run-1", "user:user"} {
		if !access.Allows("m", "t", []ACLGrant{{TenantID: "t", MemoryID: "m", Subject: subject}}) {
			t.Fatalf("missing subject %s", subject)
		}
	}
}

func TestRetrieverDoesNotOverrideAuthenticatedAccess(t *testing.T) {
	want := AccessContext{TenantID: "t", Subjects: []string{"user:alice"}}
	ctx := WithAccessContext(context.Background(), want)
	r := Retriever{Store: candidateStore{}}
	got, _ := AccessFrom(r.withRunAccess(ctx, "t", "run-1"))
	if len(got.Subjects) != 1 || got.Subjects[0] != "user:alice" {
		t.Fatalf("access was replaced: %+v", got)
	}
}
