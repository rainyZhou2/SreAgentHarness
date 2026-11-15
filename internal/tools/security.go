package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/security"
)

type guardedSecurity struct {
	next        Tool
	auth        security.Authorizer
	environment string
}

func WithSecurity(next Tool, authorizer security.Authorizer, environment string) Tool {
	return guardedSecurity{next: next, auth: authorizer, environment: environment}
}

func (g guardedSecurity) Descriptor() Descriptor { return g.next.Descriptor() }

func (g guardedSecurity) Invoke(ctx context.Context, key string, args json.RawMessage) (json.RawMessage, error) {
	if _, err := g.authorize(ctx, args); err != nil {
		return nil, err
	}
	return g.next.Invoke(ctx, key, args)
}

func (g guardedSecurity) authorize(ctx context.Context, args json.RawMessage) (security.Binding, error) {
	if g.auth == nil {
		return security.Binding{}, &security.Error{Code: security.PolicyUnavailable, Message: "tool security authorizer is not configured"}
	}
	call, ok := CallInfoFrom(ctx)
	if !ok || call.TenantID == "" || call.RunID == "" {
		return security.Binding{}, &security.Error{Code: security.InvalidRequest, Message: "authenticated run context is required"}
	}
	descriptor := g.next.Descriptor()
	operation := security.ToolExecute
	if strings.HasPrefix(descriptor.Source, "mcp:") {
		operation = security.MCPInvoke
	}
	request, err := newSecurityRequest(operation, call, descriptor, g.environment, args)
	if err != nil {
		return security.Binding{}, err
	}
	decision, err := g.auth.Authorize(ctx, request)
	if err != nil {
		return security.Binding{}, err
	}
	if decision.Effect != security.Allow && decision.Effect != security.RequireApproval {
		return security.Binding{}, &security.Error{Code: security.Denied, Message: "tool invocation denied"}
	}
	if err := g.auth.Recheck(ctx, request, decision); err != nil {
		return security.Binding{}, err
	}
	if err := security.BindDecision(ctx, request, decision); err != nil {
		return security.Binding{}, err
	}
	binding, _ := security.DecisionFrom(ctx)
	return binding, nil
}

func AuthorizeCall(ctx context.Context, tool Tool, args json.RawMessage) (security.Binding, bool, error) {
	guarded, ok := tool.(interface {
		authorize(context.Context, json.RawMessage) (security.Binding, error)
	})
	if !ok {
		return security.Binding{}, false, nil
	}
	binding, err := guarded.authorize(ctx, args)
	return binding, true, err
}

func newSecurityRequest(operation security.Operation, call CallInfo, descriptor Descriptor, environment string, args json.RawMessage) (security.Request, error) {
	if !json.Valid(args) {
		return security.Request{}, &security.Error{Code: security.InvalidRequest, Message: "tool arguments must be valid JSON"}
	}
	inputHash := call.ActionInputHash
	if inputHash == "" {
		encoded, err := json.Marshal(domain.ToolCall{Name: descriptor.Name, Args: args})
		if err != nil {
			return security.Request{}, &security.Error{Code: security.InvalidRequest, Message: "tool arguments cannot be encoded", Err: err}
		}
		sum := sha256.Sum256(encoded)
		inputHash = "sha256:" + hex.EncodeToString(sum[:])
	}
	subject := call.Subject
	if subject == "" {
		subject = "run:" + call.RunID
	}
	if call.Environment != "" {
		environment = call.Environment
	}
	actionID := call.ActionID
	requestID := call.AttemptID + ":" + actionID
	if actionID == "" {
		requestID = call.AttemptID + ":" + descriptor.Name
	}
	attributes := map[string]string{
		"tool_name": descriptor.Name, "tool_version": descriptor.Version,
		"tool_target_arg": descriptor.TargetArg, "tool_source": descriptor.Source,
	}
	for key, value := range descriptor.Labels {
		attributes["tool_label_"+key] = value
	}
	return security.Request{RequestID: requestID, Operation: operation, TenantID: call.TenantID, Subject: subject,
		ResourceRef: Target(descriptor, args), Environment: environment, Risk: string(descriptor.Risk), InputHash: inputHash,
		WorkID: call.WorkID, RunID: call.RunID, ActionID: actionID, Attributes: attributes, Input: append(json.RawMessage(nil), args...)}, nil
}
