// Package remote implements RemoteExecutor over gRPC (§16.2): a client that
// exposes a remote worker's allowlisted capabilities as tools, and a server
// helper for writing remote executors in Go.
package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/mcpexec"
	executorv1 "go-sre-harness/proto/executor/v1"
)

const ProtocolVersion = "v1"

type Config struct {
	Name    string                        `json:"name"`
	Address string                        `json:"address"`
	Allow   map[string]mcpexec.ToolPolicy `json:"allow"`
	// TLS: CA bundle and client cert for mTLS. Plaintext only when Insecure.
	CAFile   string           `json:"ca_file,omitempty"`
	CertFile string           `json:"cert_file,omitempty"`
	KeyFile  string           `json:"key_file,omitempty"`
	Insecure bool             `json:"insecure,omitempty"`
	Timeout  mcpexec.Duration `json:"timeout,omitempty"`
}

type Client struct {
	cfg  Config
	conn *grpc.ClientConn
	api  executorv1.ExecutorServiceClient
	log  *slog.Logger
}

// Dial connects to a remote executor. creds may be nil to derive them from cfg.
func Dial(cfg Config, creds credentials.TransportCredentials, log *slog.Logger, extra ...grpc.DialOption) (*Client, error) {
	if cfg.Name == "" || cfg.Address == "" {
		return nil, errors.New("remote executor: name and address are required")
	}
	for n, p := range cfg.Allow {
		switch p.Risk {
		case domain.RiskR0, domain.RiskR1, domain.RiskR2, domain.RiskR3:
		default:
			return nil, fmt.Errorf("remote %s: capability %s needs an explicit risk", cfg.Name, n)
		}
	}
	if creds == nil {
		var err error
		if creds, err = transportCreds(cfg); err != nil {
			return nil, err
		}
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = mcpexec.Duration(2 * time.Minute)
	}
	if log == nil {
		log = slog.Default()
	}
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(creds)}, extra...)
	conn, err := grpc.NewClient(cfg.Address, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{cfg: cfg, conn: conn, api: executorv1.NewExecutorServiceClient(conn), log: log}, nil
}

func transportCreds(cfg Config) (credentials.TransportCredentials, error) {
	if cfg.Insecure {
		return insecure.NewCredentials(), nil
	}
	return loadMTLS(cfg)
}

func (c *Client) Close() error { return c.conn.Close() }

// Discover registers the intersection of the remote capabilities and the
// operator allowlist.
func (c *Client) Discover(ctx context.Context) ([]tools.Tool, []string, error) {
	caps, err := c.api.GetCapabilities(ctx, &executorv1.CapabilitiesRequest{ProtocolVersion: ProtocolVersion})
	if err != nil {
		return nil, nil, err
	}
	if caps.ProtocolVersion != "" && caps.ProtocolVersion != ProtocolVersion {
		return nil, nil, fmt.Errorf("remote %s speaks protocol %s, want %s", c.cfg.Name, caps.ProtocolVersion, ProtocolVersion)
	}
	specs := map[string]*executorv1.ToolSpec{}
	for _, t := range caps.Tools {
		specs[t.Name] = t
	}
	for _, n := range caps.Names {
		if _, ok := specs[n]; !ok {
			specs[n] = &executorv1.ToolSpec{Name: n}
		}
	}
	var out []tools.Tool
	var skipped []string
	for name, spec := range specs {
		pol, ok := c.cfg.Allow[name]
		if !ok {
			skipped = append(skipped, name)
			continue
		}
		schema := json.RawMessage(spec.InputSchemaJson)
		if !json.Valid(schema) {
			schema = json.RawMessage(`{"type":"object"}`)
		}
		trust := pol.Trust
		if trust == "" {
			trust = "T3"
		}
		out = append(out, &Tool{c: c, remote: name, desc: tools.Descriptor{Name: "remote." + c.cfg.Name + "." + name,
			Version: spec.Version, Risk: pol.Risk, TargetArg: pol.TargetArg, Trust: trust, Source: "remote:" + c.cfg.Name,
			InputSchema: schema, Description: "[remote executor " + c.cfg.Name + "] " + spec.Description}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Descriptor().Name < out[j].Descriptor().Name })
	sort.Strings(skipped)
	return out, skipped, nil
}

type Tool struct {
	c      *Client
	remote string
	desc   tools.Descriptor
}

func (t *Tool) Descriptor() tools.Descriptor { return t.desc }

func (t *Tool) Invoke(ctx context.Context, idempotencyKey string, args json.RawMessage) (json.RawMessage, error) {
	ci, _ := tools.CallInfoFrom(ctx)
	deadline := time.Now().Add(time.Duration(t.c.cfg.Timeout))
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	cctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	attempt := ci.AttemptID
	if attempt == "" {
		attempt = idempotencyKey
	}
	resp, err := t.c.api.ExecuteStep(cctx, &executorv1.ExecuteStepRequest{TenantId: ci.TenantID, RunId: ci.RunID,
		StepId: ci.StepID, AttemptId: attempt, Executor: t.remote, InputRef: ci.InputRef,
		ContextSnapshotId: ci.ContextSnapshotID, FenceToken: ci.FenceToken, IdempotencyKey: idempotencyKey,
		DeadlineUnixMs: deadline.UnixMilli(), InputJson: args}, grpc.WaitForReady(false))
	if err != nil {
		if ctx.Err() != nil {
			// Best-effort cancel acknowledgement; the step is UNKNOWN for side effects.
			cc, c2 := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			_, _ = t.c.api.CancelAttempt(cc, &executorv1.CancelAttemptRequest{AttemptId: attempt})
			c2()
		}
		if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable && !t.desc.Risk.HasSideEffects() {
			return nil, domain.Retryable(err, 2*time.Second)
		}
		return nil, fmt.Errorf("remote %s/%s: %w", t.c.cfg.Name, t.remote, err)
	}
	switch resp.Status {
	case "SUCCEEDED":
		out := map[string]any{"executor": t.c.cfg.Name, "capability": t.remote}
		if len(resp.ResultJson) > 0 && json.Valid(resp.ResultJson) {
			out["result"] = json.RawMessage(resp.ResultJson)
		}
		if resp.ResultRef != "" {
			out["result_ref"], out["result_hash"] = resp.ResultRef, resp.ResultHash
		}
		if resp.ExternalOperationId != "" {
			out["external_operation_id"] = resp.ExternalOperationId
		}
		return json.Marshal(out)
	case "REJECTED":
		return nil, fmt.Errorf("%w: remote %s: %s %s", domain.ErrNotDispatched, t.c.cfg.Name, resp.ErrorCode, resp.ErrorMessage)
	case "UNKNOWN":
		return nil, fmt.Errorf("%w: remote %s: %s %s", domain.ErrUnknownEffect, t.c.cfg.Name, resp.ErrorCode, resp.ErrorMessage)
	default:
		return nil, fmt.Errorf("remote %s/%s failed: %s %s", t.c.cfg.Name, t.remote, resp.ErrorCode, resp.ErrorMessage)
	}
}

// ---------- server helper ----------

// Server serves native tools over ExecutorService. Results are cached by
// idempotency key so a retried attempt returns the first outcome instead of
// re-applying the effect.
type Server struct {
	executorv1.UnimplementedExecutorServiceServer
	reg *tools.Registry

	mu       sync.Mutex
	results  map[string]*executorv1.ExecuteStepResponse
	inflight map[string]context.CancelFunc
}

func NewServer(ts ...tools.Tool) *Server {
	return &Server{reg: tools.NewRegistry(ts...), results: map[string]*executorv1.ExecuteStepResponse{},
		inflight: map[string]context.CancelFunc{}}
}

func (s *Server) GetCapabilities(context.Context, *executorv1.CapabilitiesRequest) (*executorv1.CapabilitiesResponse, error) {
	resp := &executorv1.CapabilitiesResponse{ProtocolVersion: ProtocolVersion}
	for _, d := range s.reg.Descriptors() {
		resp.Names = append(resp.Names, d.Name)
		resp.Tools = append(resp.Tools, &executorv1.ToolSpec{Name: d.Name, Version: d.Version, Description: d.Description,
			InputSchemaJson: string(d.InputSchema)})
	}
	return resp, nil
}

func (s *Server) ExecuteStep(ctx context.Context, req *executorv1.ExecuteStepRequest) (*executorv1.ExecuteStepResponse, error) {
	if req.IdempotencyKey == "" || req.AttemptId == "" {
		return &executorv1.ExecuteStepResponse{Status: "REJECTED", ErrorCode: "INVALID_REQUEST",
			ErrorMessage: "idempotency_key and attempt_id are required"}, nil
	}
	t, ok := s.reg.Lookup(req.Executor)
	if !ok {
		return &executorv1.ExecuteStepResponse{Status: "REJECTED", ErrorCode: "UNKNOWN_EXECUTOR"}, nil
	}
	s.mu.Lock()
	if prev, ok := s.results[req.IdempotencyKey]; ok {
		s.mu.Unlock()
		return prev, nil
	}
	if req.DeadlineUnixMs > 0 {
		var cancelDeadline context.CancelFunc
		ctx, cancelDeadline = context.WithDeadline(ctx, time.UnixMilli(req.DeadlineUnixMs))
		defer cancelDeadline()
	}
	ctx, cancel := context.WithCancel(ctx)
	s.inflight[req.AttemptId] = cancel
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.inflight, req.AttemptId)
		s.mu.Unlock()
		cancel()
	}()
	out, err := t.Invoke(tools.WithCallInfo(ctx, tools.CallInfo{TenantID: req.TenantId, RunID: req.RunId, StepID: req.StepId,
		AttemptID: req.AttemptId, FenceToken: req.FenceToken, ContextSnapshotID: req.ContextSnapshotId, InputRef: req.InputRef}),
		req.IdempotencyKey, req.InputJson)
	var resp *executorv1.ExecuteStepResponse
	switch {
	case err == nil:
		resp = &executorv1.ExecuteStepResponse{Status: "SUCCEEDED", ResultJson: out}
	case errors.Is(err, domain.ErrNotDispatched):
		resp = &executorv1.ExecuteStepResponse{Status: "REJECTED", ErrorCode: "REJECTED", ErrorMessage: err.Error()}
	case errors.Is(err, domain.ErrUnknownEffect), ctx.Err() != nil && t.Descriptor().Risk.HasSideEffects():
		resp = &executorv1.ExecuteStepResponse{Status: "UNKNOWN", ErrorCode: "EFFECT_UNKNOWN", ErrorMessage: err.Error()}
	default:
		resp = &executorv1.ExecuteStepResponse{Status: "FAILED", ErrorCode: "TOOL_ERROR", ErrorMessage: err.Error()}
	}
	if resp.Status == "SUCCEEDED" || t.Descriptor().Risk.HasSideEffects() {
		s.mu.Lock()
		s.results[req.IdempotencyKey] = resp
		s.mu.Unlock()
	}
	return resp, nil
}

func (s *Server) CancelAttempt(_ context.Context, req *executorv1.CancelAttemptRequest) (*executorv1.CancelAttemptResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.inflight[req.AttemptId]; ok {
		c()
		return &executorv1.CancelAttemptResponse{Accepted: true}, nil
	}
	return &executorv1.CancelAttemptResponse{Accepted: false}, nil
}
