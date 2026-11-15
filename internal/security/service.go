package security

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"sort"
	"time"
)

type Mode string

const (
	ModeLegacy  Mode = "legacy"
	ModeShadow  Mode = "shadow"
	ModeEnforce Mode = "enforce"
)

type Service struct {
	Mode             Mode
	Evaluator        Evaluator
	Revocations      RevocationReader
	FreshRevocations RevocationReader
	Recorder         DecisionRecorder
	Approvals        ApprovalVerifier
	Observer         Observer
	Now              func() time.Time
}

func (s *Service) Authorize(ctx context.Context, request Request) (PolicyDecision, error) {
	started := time.Now()
	now := s.clock().UTC()
	request, err := request.Normalize(now)
	if err != nil {
		return PolicyDecision{}, err
	}
	if s.Mode == ModeLegacy {
		decision := PolicyDecision{Effect: Allow, PolicyVersion: "legacy", Mode: string(s.Mode), RequestHash: hashRequest(request), DecidedAt: now, ExpiresAt: now.Add(DecisionTTL)}
		_, err := decision.Finalize(now)
		return decision, err
	}
	if (s.Mode != ModeShadow && s.Mode != ModeEnforce) || s.Evaluator == nil || s.Revocations == nil {
		return PolicyDecision{}, &Error{Code: PolicyUnavailable, Message: "security service is not configured"}
	}
	if request.SideEffecting() && (s.Recorder == nil || s.FreshRevocations == nil) {
		return PolicyDecision{}, &Error{Code: PolicyUnavailable, Message: "side-effect audit and fresh revocation reader are required"}
	}
	stamps, err := s.Revocations.Stamps(ctx, request)
	if err != nil {
		return PolicyDecision{}, &Error{Code: PolicyUnavailable, Message: "revocation state is unavailable", Err: err}
	}
	for _, stamp := range stamps {
		if stamp.Version > 0 {
			decision := PolicyDecision{Effect: Deny, PolicyVersion: "revocation", Mode: string(s.Mode), RevocationStamps: stamps, Reasons: []Reason{{Code: "revoked", Message: "an applicable security scope is revoked"}}}
			return s.finish(ctx, request, decision, now, started)
		}
	}
	decision, err := s.Evaluator.Evaluate(ctx, request)
	if err != nil {
		return PolicyDecision{}, &Error{Code: PolicyUnavailable, Message: "policy evaluation failed", Err: err}
	}
	decision.Mode = string(s.Mode)
	decision.RevocationStamps = stamps
	return s.finish(ctx, request, decision, now, started)
}

func (s *Service) finish(ctx context.Context, request Request, decision PolicyDecision, now, started time.Time) (PolicyDecision, error) {
	decision.RequestHash = hashRequest(request)
	if _, err := decision.Finalize(now); err != nil {
		return PolicyDecision{}, err
	}
	s.observe(request, decision, time.Since(started))
	if request.SideEffecting() {
		if err := s.Recorder.RecordDecision(ctx, request, decision); err != nil {
			return PolicyDecision{}, &Error{Code: PolicyUnavailable, Message: "security decision audit failed", Err: err}
		}
	}
	if s.Mode == ModeShadow {
		out := PolicyDecision{Effect: Allow, PolicyVersion: "shadow", Mode: string(ModeShadow), RequestHash: hashRequest(request), DecidedAt: now, ExpiresAt: now.Add(DecisionTTL), Reasons: []Reason{{Code: "shadow_observe", Message: "unified decision recorded without enforcement"}}}
		_, err := out.Finalize(now)
		return out, err
	}
	return decision, nil
}

func (s *Service) Recheck(ctx context.Context, request Request, decision PolicyDecision) error {
	if s.Mode == ModeLegacy || s.Mode == ModeShadow {
		return nil
	}
	if s.Mode != ModeEnforce {
		return &Error{Code: PolicyUnavailable, Message: "unknown security mode"}
	}
	reader := s.Revocations
	if request.SideEffecting() {
		reader = s.FreshRevocations
	}
	if reader == nil {
		return &Error{Code: PolicyUnavailable, Message: "revocation reader is not configured"}
	}
	return s.RecheckUsing(ctx, request, decision, reader)
}

func (s *Service) RecheckUsing(ctx context.Context, request Request, decision PolicyDecision, reader RevocationReader) error {
	if s.Mode == ModeLegacy || s.Mode == ModeShadow {
		return nil
	}
	if s.Mode != ModeEnforce || reader == nil {
		return &Error{Code: PolicyUnavailable, Message: "security recheck is not configured"}
	}
	now := s.clock().UTC()
	if request.OccurredAt.IsZero() {
		request.OccurredAt = decision.DecidedAt
	}
	normalized, err := request.Normalize(now)
	if err != nil {
		return err
	}
	if decision.Effect != Allow && decision.Effect != RequireApproval {
		return &Error{Code: Denied, Message: "operation was not allowed"}
	}
	if decision.DecisionID == "" {
		return &Error{Code: PolicyUnavailable, Message: "security decision digest is missing"}
	}
	if decision.DecidedAt.IsZero() || decision.DecidedAt.After(now) || !decision.ExpiresAt.After(now) || decision.ExpiresAt.After(decision.DecidedAt.Add(DecisionTTL)) {
		return &Error{Code: DecisionExpired, Message: "security decision validity is invalid or expired"}
	}
	if decision.Constraints.NotBefore != nil && now.Before(*decision.Constraints.NotBefore) {
		return &Error{Code: Denied, Message: "security decision is not active"}
	}
	if decision.RequestHash == "" || decision.RequestHash != hashRequest(normalized) {
		return &Error{Code: InvalidRequest, Message: "security decision is bound to a different request"}
	}
	clone, err := cloneDecision(decision)
	if err != nil {
		return &Error{Code: PolicyUnavailable, Err: err}
	}
	want := decision.DecisionID
	if _, err := clone.Finalize(clone.DecidedAt); err != nil {
		return &Error{Code: PolicyUnavailable, Err: err}
	}
	if subtle.ConstantTimeCompare([]byte(want), []byte(clone.DecisionID)) != 1 {
		return &Error{Code: PolicyUnavailable, Message: "security decision digest mismatch"}
	}
	if decision.Effect == RequireApproval {
		if s.Approvals == nil {
			return &Error{Code: ApprovalRequired, Message: "verified approval grant is required"}
		}
		if err := s.Approvals.VerifyApproval(ctx, normalized, decision.RequiredApprovals); err != nil {
			return err
		}
	}
	current, err := reader.Stamps(ctx, normalized)
	if err != nil {
		return &Error{Code: PolicyUnavailable, Message: "revocation state is unavailable", Err: err}
	}
	previous := map[string]int64{}
	for _, stamp := range decision.RevocationStamps {
		previous[stamp.Scope+"\x00"+stamp.ScopeID] = stamp.Version
	}
	for _, stamp := range current {
		if stamp.Version > previous[stamp.Scope+"\x00"+stamp.ScopeID] {
			return &Error{Code: Revoked, Message: "security scope was revoked after authorization"}
		}
	}
	return nil
}

func (s *Service) clock() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Service) observe(request Request, decision PolicyDecision, duration time.Duration) {
	if s.Observer == nil {
		return
	}
	reason := ""
	if len(decision.Reasons) > 0 {
		reason = decision.Reasons[0].Code
	}
	s.Observer.Decision(Observation{Operation: request.Operation, Effect: decision.Effect, Mode: s.Mode, ReasonCode: reason, Duration: duration})
}

func hashRequest(request Request) string {
	parts := []string{request.RequestID, string(request.Operation), request.TenantID, request.Subject, request.ResourceRef,
		request.Environment, request.DataClass, request.Risk, request.InputHash, request.WorkID, request.RunID, request.ActionID,
		request.OccurredAt.UTC().Format(time.RFC3339Nano)}
	keys := make([]string, 0, len(request.Attributes))
	for key := range request.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, key, request.Attributes[key])
	}
	return hashParts(parts...)
}

func cloneDecision(decision PolicyDecision) (PolicyDecision, error) {
	encoded, err := json.Marshal(decision)
	if err != nil {
		return PolicyDecision{}, err
	}
	var clone PolicyDecision
	if err := json.Unmarshal(encoded, &clone); err != nil {
		return PolicyDecision{}, err
	}
	return clone, nil
}
