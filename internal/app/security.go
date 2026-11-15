package app

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/security"
	"go-sre-harness/internal/security/revocation"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/store/pg"
)

type SecurityConfig struct {
	Constraints []security.ConstraintRule `json:"constraints,omitempty"`
}

type securityRuntime struct {
	Service     *security.Service
	Revocations *revocation.Service
	cache       *revocation.Cache
	pool        *pgxpool.Pool
}

func securityMode(getenv func(string) string) (security.Mode, error) {
	mode := security.Mode(strings.ToLower(strings.TrimSpace(getenv("HARNESS_SECURITY_MODE"))))
	if mode == "" {
		mode = security.ModeLegacy
	}
	switch mode {
	case security.ModeLegacy, security.ModeShadow, security.ModeEnforce:
		return mode, nil
	default:
		return "", fmt.Errorf("unknown HARNESS_SECURITY_MODE %q", mode)
	}
}

func validateSecurity(mode security.Mode, deps security.Dependencies) error {
	if mode != security.ModeEnforce {
		return nil
	}
	var missing []string
	if deps.Recorder == nil {
		missing = append(missing, "decision recorder")
	}
	if deps.FreshRevocations == nil {
		missing = append(missing, "fresh revocation reader")
	}
	if deps.ApprovalVerifier == nil {
		missing = append(missing, "approval verifier")
	}
	if !deps.ToolsDecorated {
		missing = append(missing, "tool registry security decorator")
	}
	if !deps.CommitGuard {
		missing = append(missing, "security commit guard")
	}
	if len(missing) > 0 {
		return errors.New("enforce security mode requires " + strings.Join(missing, ", "))
	}
	return nil
}

func buildSecurity(pool *pgxpool.Pool, cfg SecurityConfig, mode security.Mode, metrics *obs.Registry) (*securityRuntime, error) {
	if pool == nil {
		return nil, errors.New("security requires a PostgreSQL pool")
	}
	constraints, err := security.CompileConstraints(cfg.Constraints)
	if err != nil {
		return nil, fmt.Errorf("security constraints: %w", err)
	}
	store := cpsql.OpenPostgres(pool)
	cache := revocation.NewCache(store, time.Now)
	observer := newSecurityObserver(metrics)
	service := &security.Service{
		Mode:             mode,
		Revocations:      revocation.RequestReader{Reader: cache},
		FreshRevocations: revocation.RequestReader{Reader: store},
		Recorder:         cpsql.NewSecurityRecorder(store),
		Approvals:        pg.SecurityApprovalVerifier{Pool: pool},
		Observer:         observer,
	}
	service.Evaluator = policy.UnifiedEvaluator{Constraints: constraints}
	revocations := &revocation.Service{Store: store, Cache: cache, Observer: observer}
	return &securityRuntime{Service: service, Revocations: revocations, cache: cache, pool: pool}, nil
}

func (s *securityRuntime) setPolicyGate(gate policy.Gate) {
	evaluator, ok := s.Service.Evaluator.(policy.UnifiedEvaluator)
	if !ok {
		return
	}
	evaluator.Gate = gate
	s.Service.Evaluator = evaluator
}

func (s *securityRuntime) listener() *cpsql.SecurityListener {
	return &cpsql.SecurityListener{
		Config: s.pool.Config().ConnConfig.Copy(),
		Invalidate: func(event cpsql.SecurityNotification) {
			s.cache.Invalidate(event.TenantID, event.ScopeID)
		},
	}
}

type securityObserver struct {
	decisions     obs.Counter
	latency       obs.Histogram
	notifications obs.Counter
}

func newSecurityObserver(registry *obs.Registry) *securityObserver {
	return &securityObserver{
		decisions:     registry.Counter("harness_security_decisions_total", "Unified security decisions.", "operation", "effect", "mode"),
		latency:       registry.Histogram("harness_security_decision_seconds", "Unified security decision latency.", nil, "operation", "mode"),
		notifications: registry.Counter("harness_security_notification_failures_total", "Security notification failures.", "scope"),
	}
}

func (o *securityObserver) Decision(observation security.Observation) {
	o.decisions.Inc(string(observation.Operation), string(observation.Effect), string(observation.Mode))
	o.latency.Observe(observation.Duration.Seconds(), string(observation.Operation), string(observation.Mode))
}

func (o *securityObserver) NotificationFailed(scope string) {
	o.notifications.Inc(scope)
}
