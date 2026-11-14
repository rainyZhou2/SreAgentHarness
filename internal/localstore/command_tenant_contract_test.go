package localstore

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"go-sre-harness/internal/domain"
)

func TestApplyCommandRejectsForeignTenantWithoutWrites(t *testing.T) {
	for _, existingCommand := range []bool{false, true} {
		for _, tenant := range []string{"foreign", ""} {
			name := tenant
			if existingCommand {
				name += "/existing-key"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				s, err := Open(filepath.Join(t.TempDir(), "h.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				run, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "owner", GoalRef: "goal", GoalHash: "hash"})
				if err != nil {
					t.Fatal(err)
				}
				req := domain.CommandRequest{Type: domain.CmdPause, ActorID: "operator", IdempotencyKey: "same-key"}
				var ownerCommand domain.Command
				if existingCommand {
					ownerCommand, err = s.ApplyCommand(ctx, "owner", run.ID, req)
					if err != nil {
						t.Fatal(err)
					}
				}
				before, objects, files := s.snapshotStateLocked(), objectRows(t, s), eventFiles(t, s)
				got, err := s.ApplyCommand(ctx, tenant, run.ID, req)
				if !errors.Is(err, domain.ErrNotFound) || got.ID != "" {
					t.Errorf("foreign command tenant=%q: %s %v", tenant, got.ID, err)
				}
				if !reflect.DeepEqual(before, s.snapshotStateLocked()) {
					t.Error("foreign command changed memory or persisted a rejected command")
				}
				if !reflect.DeepEqual(objects, objectRows(t, s)) {
					t.Error("foreign command persisted objects")
				}
				if !reflect.DeepEqual(files, eventFiles(t, s)) {
					t.Error("foreign command emitted events")
				}
				allowed, err := s.ApplyCommand(ctx, "owner", run.ID, req)
				if err != nil || allowed.Status != domain.CommandApplied {
					t.Fatalf("owner command: %v %v", allowed, err)
				}
				if existingCommand && allowed.ID != ownerCommand.ID {
					t.Fatal("owner idempotency was changed by foreign command")
				}
				current, err := s.GetRun(ctx, "owner", run.ID)
				if err != nil || current.Status != domain.RunPaused {
					t.Fatalf("owner pause did not apply: %s %v", current.Status, err)
				}
			})
		}
	}
}
