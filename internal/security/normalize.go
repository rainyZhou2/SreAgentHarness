package security

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"time"
)

const (
	MaxID             = 200
	MaxAttributes     = 32
	MaxAttributeKey   = 64
	MaxAttributeValue = 256
	DecisionTTL       = time.Minute
)

var hashRE = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

var operationSet = map[Operation]bool{
	ToolExecute: true, SandboxExecute: true, MCPInvoke: true,
	ArtifactRead: true, ArtifactWrite: true, ArtifactDelete: true,
	MemoryRead: true, MemoryWrite: true, MemoryPublish: true, ModelEgress: true,
}

func (r Request) Normalize(now time.Time) (Request, error) {
	if now.IsZero() {
		return r, &Error{Code: InvalidRequest, Message: "decision clock is required"}
	}
	r.RequestID = strings.TrimSpace(r.RequestID)
	r.TenantID = strings.TrimSpace(r.TenantID)
	r.Subject = strings.TrimSpace(r.Subject)
	r.ResourceRef = strings.TrimSpace(r.ResourceRef)
	r.Environment = strings.TrimSpace(r.Environment)
	r.DataClass = strings.TrimSpace(r.DataClass)
	r.Risk = strings.ToUpper(strings.TrimSpace(r.Risk))
	r.WorkID = strings.TrimSpace(r.WorkID)
	r.RunID = strings.TrimSpace(r.RunID)
	r.ActionID = strings.TrimSpace(r.ActionID)
	for name, value := range map[string]string{
		"request_id": r.RequestID, "tenant": r.TenantID, "subject": r.Subject,
		"resource": r.ResourceRef, "work": r.WorkID, "run": r.RunID, "action": r.ActionID,
	} {
		if len(value) > MaxID {
			return r, &Error{Code: InvalidRequest, Message: name + " exceeds 200 characters"}
		}
	}
	if !operationSet[r.Operation] || r.TenantID == "" || r.Subject == "" || !hashRE.MatchString(r.InputHash) {
		return r, &Error{Code: InvalidRequest, Message: "operation, tenant, subject and a SHA-256 input hash are required"}
	}
	if r.RequestID == "" {
		r.RequestID = r.InputHash
	}
	if r.Environment == "" {
		r.Environment = "production"
	}
	if r.DataClass == "" {
		r.DataClass = "internal"
	}
	if len(r.Attributes) > MaxAttributes {
		return r, &Error{Code: InvalidRequest, Message: "too many attributes"}
	}
	if len(r.Attributes) > 0 {
		attributes := make(map[string]string, len(r.Attributes))
		for key, value := range r.Attributes {
			if len(key) == 0 || len(key) > MaxAttributeKey || len(value) > MaxAttributeValue {
				return r, &Error{Code: InvalidRequest, Message: "invalid decision attribute"}
			}
			attributes[key] = value
		}
		r.Attributes = attributes
	}
	if len(r.Input) > 0 {
		if !json.Valid(r.Input) {
			return r, &Error{Code: InvalidRequest, Message: "decision input must be valid JSON"}
		}
		r.Input = bytes.Clone(r.Input)
	}
	if r.OccurredAt.IsZero() {
		r.OccurredAt = now.UTC()
	} else {
		r.OccurredAt = r.OccurredAt.UTC()
	}
	if r.OccurredAt.After(now.Add(time.Minute)) {
		return r, &Error{Code: InvalidRequest, Message: "decision time is in the future"}
	}
	return r, nil
}

func (r Request) SideEffecting() bool {
	switch r.Operation {
	case ArtifactRead, MemoryRead:
		return false
	case ToolExecute, MCPInvoke:
		return r.Risk != "R0"
	default:
		return true
	}
}
