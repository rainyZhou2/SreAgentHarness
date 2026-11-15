package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"go-sre-harness/internal/security"
	"go-sre-harness/internal/tools"
)

type securedBackend struct {
	next        Backend
	authorizer  security.Authorizer
	environment string
}

func WithSecurity(next Backend, authorizer security.Authorizer, environment string) Backend {
	return securedBackend{next: next, authorizer: authorizer, environment: environment}
}

func (b securedBackend) Info() Info { return b.next.Info() }

func (b securedBackend) Exec(ctx context.Context, request Request) (Result, error) {
	if err := request.Validate(); err != nil {
		return Result{}, err
	}
	call, err := b.callContext(ctx, request.TenantID, request.RunID)
	if err != nil {
		return Result{}, err
	}
	input := struct {
		Argv    []string          `json:"argv"`
		Stdin   string            `json:"stdin"`
		Files   map[string]string `json:"files"`
		Outputs []string          `json:"outputs"`
		Env     map[string]string `json:"env"`
		Timeout int64             `json:"timeout"`
	}{request.Argv, request.Stdin, request.Files, request.Outputs, request.Env, int64(request.Timeout)}
	hash, err := sandboxHash(input)
	if err != nil {
		return Result{}, err
	}
	workspace := WorkspaceKey(request.TenantID, request.RunID)
	securityRequest := b.request(call, "sandbox.exec:"+workspace, "sandbox://"+workspace, hash)
	if err := b.authorize(ctx, securityRequest); err != nil {
		return Result{}, err
	}
	return b.next.Exec(ctx, request)
}

func (b securedBackend) Destroy(ctx context.Context, tenantID, runID string) error {
	if tenantID == "" || runID == "" {
		return ErrInvalid
	}
	call, err := b.callContext(ctx, tenantID, runID)
	if err != nil {
		return err
	}
	workspace := WorkspaceKey(tenantID, runID)
	hash, err := sandboxHash(struct {
		Workspace string `json:"workspace"`
	}{workspace})
	if err != nil {
		return err
	}
	securityRequest := b.request(call, "sandbox.destroy:"+workspace, "sandbox://"+workspace, hash)
	if err := b.authorize(ctx, securityRequest); err != nil {
		return err
	}
	return b.next.Destroy(ctx, tenantID, runID)
}

func (b securedBackend) callContext(ctx context.Context, tenantID, runID string) (tools.CallInfo, error) {
	call, ok := tools.CallInfoFrom(ctx)
	if !ok || call.TenantID == "" || call.RunID == "" || call.TenantID != tenantID || call.RunID != runID {
		return tools.CallInfo{}, &security.Error{Code: security.InvalidRequest, Message: "sandbox context does not match the authenticated run"}
	}
	return call, nil
}

func (b securedBackend) request(call tools.CallInfo, operationID, resourceRef, inputHash string) security.Request {
	subject := call.Subject
	if subject == "" {
		subject = "run:" + call.RunID
	}
	environment := b.environment
	if call.Environment != "" {
		environment = call.Environment
	}
	return security.Request{RequestID: call.AttemptID + ":" + operationID, Operation: security.SandboxExecute,
		TenantID: call.TenantID, Subject: subject, ResourceRef: resourceRef, Environment: environment,
		DataClass: "internal", Risk: "R3", InputHash: inputHash, WorkID: call.WorkID, RunID: call.RunID, ActionID: call.ActionID}
}

func (b securedBackend) authorize(ctx context.Context, request security.Request) error {
	if b.authorizer == nil {
		return &security.Error{Code: security.PolicyUnavailable, Message: "sandbox security authorizer is not configured"}
	}
	decision, err := b.authorizer.Authorize(ctx, request)
	if err != nil {
		return err
	}
	switch decision.Effect {
	case security.Allow:
	case security.RequireApproval:
		return &security.Error{Code: security.ApprovalRequired, Message: "sandbox execution requires a tool action approval"}
	default:
		return &security.Error{Code: security.Denied, Message: "sandbox execution denied"}
	}
	return b.authorizer.Recheck(ctx, request, decision)
}

func sandboxHash(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", &security.Error{Code: security.InvalidRequest, Message: "sandbox input cannot be encoded", Err: err}
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
