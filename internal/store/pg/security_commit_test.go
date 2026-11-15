package pg

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/security"
)

func TestValidateSecurityCommitBinding(t *testing.T) {
	lease := domain.Lease{TenantID: "t", RunID: "r", WorkID: "w", Subject: "work:w", Intent: &domain.ActionIntent{ID: "a", InputHash: "sha256:input"}}
	request := security.Request{Operation: security.ToolExecute, TenantID: "t", RunID: "r", WorkID: "w", Subject: "work:w", ActionID: "a", InputHash: "sha256:input"}
	if err := validateSecurityCommitBinding(lease, request); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*security.Request){
		"run":     func(r *security.Request) { r.RunID = "other" },
		"work":    func(r *security.Request) { r.WorkID = "other" },
		"subject": func(r *security.Request) { r.Subject = "run:r" },
		"action":  func(r *security.Request) { r.ActionID = "other" },
		"input":   func(r *security.Request) { r.InputHash = "sha256:other" },
		"operation": func(r *security.Request) {
			r.Operation = security.ModelEgress
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := request
			mutate(&candidate)
			if err := validateSecurityCommitBinding(lease, candidate); err == nil {
				t.Fatal("mismatched decision was accepted")
			}
		})
	}
}

type commitGuardStub struct{}

func (commitGuardStub) CheckCommit(context.Context, pgx.Tx, domain.Lease, string) error { return nil }

func TestNewPreservesCommitGuardWithDefaultLimits(t *testing.T) {
	store := New(nil, Config{CommitGuard: commitGuardStub{}})
	if store.cfg.CommitGuard == nil || store.cfg.MaxStepsPerRun != DefaultConfig().MaxStepsPerRun {
		t.Fatalf("config=%+v", store.cfg)
	}
}
