package security

import (
	"context"
	"encoding/json"
	"time"
)

type Operation string

const (
	ToolExecute    Operation = "tool.execute"
	SandboxExecute Operation = "sandbox.execute"
	MCPInvoke      Operation = "mcp.invoke"
	ArtifactRead   Operation = "artifact.read"
	ArtifactWrite  Operation = "artifact.write"
	ArtifactDelete Operation = "artifact.delete"
	MemoryRead     Operation = "memory.read"
	MemoryWrite    Operation = "memory.write"
	MemoryPublish  Operation = "memory.publish"
	ModelEgress    Operation = "model.egress"
)

type Effect string

const (
	Allow           Effect = "allow"
	RequireApproval Effect = "require_approval"
	Deny            Effect = "deny"
)

type Request struct {
	RequestID   string            `json:"request_id"`
	Operation   Operation         `json:"operation"`
	TenantID    string            `json:"tenant_id"`
	Subject     string            `json:"subject"`
	ResourceRef string            `json:"resource_ref,omitempty"`
	Environment string            `json:"environment,omitempty"`
	DataClass   string            `json:"data_class,omitempty"`
	Risk        string            `json:"risk,omitempty"`
	InputHash   string            `json:"input_hash"`
	WorkID      string            `json:"work_id,omitempty"`
	RunID       string            `json:"run_id,omitempty"`
	ActionID    string            `json:"action_id,omitempty"`
	OccurredAt  time.Time         `json:"occurred_at"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	Input       json.RawMessage   `json:"-"`
}

type RevocationStamp struct {
	Scope   string `json:"scope"`
	ScopeID string `json:"scope_id"`
	Version int64  `json:"version"`
}

type Reason struct {
	Code      string `json:"code"`
	PolicyRef string `json:"policy_ref,omitempty"`
	Message   string `json:"message"`
}

type Constraints struct {
	NotBefore    *time.Time `json:"not_before,omitempty"`
	WindowRef    string     `json:"window_ref,omitempty"`
	CanaryRef    string     `json:"canary_ref,omitempty"`
	CanaryBucket *uint32    `json:"canary_bucket,omitempty"`
}

type PolicyDecision struct {
	Effect            Effect            `json:"effect"`
	Reasons           []Reason          `json:"reasons,omitempty"`
	PolicyVersion     string            `json:"policy_version"`
	RequiredApprovals int               `json:"required_approvals,omitempty"`
	Constraints       Constraints       `json:"constraints,omitempty"`
	RevocationStamps  []RevocationStamp `json:"revocation_stamps,omitempty"`
	RequestHash       string            `json:"request_hash"`
	DecisionID        string            `json:"decision_id"`
	DecidedAt         time.Time         `json:"decided_at"`
	ExpiresAt         time.Time         `json:"expires_at"`
	Mode              string            `json:"mode"`
}

type Evaluator interface {
	Evaluate(ctx context.Context, r Request) (PolicyDecision, error)
}

type RevocationReader interface {
	Stamps(ctx context.Context, r Request) ([]RevocationStamp, error)
}

type DecisionRecorder interface {
	RecordDecision(ctx context.Context, r Request, d PolicyDecision) error
}

type ApprovalVerifier interface {
	VerifyApproval(ctx context.Context, r Request, required int) error
}

type Authorizer interface {
	Authorize(ctx context.Context, r Request) (PolicyDecision, error)
	Recheck(ctx context.Context, r Request, d PolicyDecision) error
}
