package localstore

import (
	"context"
	"errors"
	"go-sre-harness/internal/domain"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSubmitTurnRequiresSessionOwnedBranch(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	own, _, err := s.CreateSession(ctx, domain.CreateSessionRequest{TenantID: "t"})
	if err != nil {
		t.Fatal(err)
	}
	foreign, _, err := s.CreateSession(ctx, domain.CreateSessionRequest{TenantID: "other"})
	if err != nil {
		t.Fatal(err)
	}
	before, objects := s.snapshotStateLocked(), objectRows(t, s)
	_, err = s.SubmitTurn(ctx, domain.SubmitTurnRequest{TenantID: "t", SessionID: own.ID, BranchID: foreign.DefaultBranchID, ClientRequestID: "request", InputRef: "input"}, domain.BranchQueue, 10)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("foreign branch accepted: %v", err)
	}
	if !reflect.DeepEqual(before, s.snapshotStateLocked()) || !reflect.DeepEqual(objects, objectRows(t, s)) {
		t.Fatal("foreign branch mutated")
	}
}
