package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Ledger persists the model request / attempt / usage records (§34). The
// pg implementation creates the request only while the caller still holds
// the step fence (INSERT … SELECT … WHERE fence matches) and appends the
// matching model.* run events in the same transaction.
type Ledger interface {
	CreateModelRequest(ctx context.Context, sc Scope, rec RequestRecord) error
	StartModelAttempt(ctx context.Context, sc Scope, rec AttemptRecord) error
	AppendUsage(ctx context.Context, tenantID string, rec UsageRecord) error
	FinishModelAttempt(ctx context.Context, sc Scope, rec AttemptResult) error
	FinishModelRequest(ctx context.Context, sc Scope, rec RequestResult) error
	// SpentMicros is the tenant's effective model spend since t (budget admission).
	SpentMicros(ctx context.Context, tenantID string, since time.Time) (int64, error)
}

type RequestRecord struct {
	ID                 string
	ModelAlias         string
	RoutePolicyID      string
	RoutePolicyVersion int64
	RequestRef         string
	RequestHash        string
	ManifestRef        string
	MaxOutputTokens    int64
}

type AttemptRecord struct {
	ID             string
	ModelRequestID string
	AttemptNo      int
	ProviderID     string
	EndpointID     string
	ModelID        string
	ModelVersion   string
	RouteReason    json.RawMessage
}

type UsageState string

const (
	UsageEstimated  UsageState = "ESTIMATED"
	UsageReported   UsageState = "REPORTED"
	UsageReconciled UsageState = "RECONCILED"
	UsageUnknown    UsageState = "UNKNOWN"
)

type UsageRecord struct {
	ID             string
	AttemptID      string
	State          UsageState
	Revision       int
	SupersedesID   string
	Usage          *Usage // nil for UNKNOWN
	PricingVersion string
	CostMicros     int64
}

type AttemptResult struct {
	ID                string
	ModelRequestID    string
	Status            string // SUCCEEDED | FAILED | CANCELLED | OUTCOME_UNKNOWN
	ProviderRequestID string
	ResponseRef       string
	FinishReason      string
	ErrorCode         string
	TTFT              time.Duration
	Usage             Usage
	CostMicros        int64
}

type RequestResult struct {
	ID          string
	Status      string // SUCCEEDED | FAILED | CANCELLED | OUTCOME_UNKNOWN
	ResponseRef string
	ErrorCode   string
	Endpoint    string
	Model       string
	Usage       Usage
	CostMicros  int64
}

// Artifacts is the subset of artifact.Store the gateway needs.
type Artifacts interface {
	Put(ctx context.Context, tenantID, kind string, data []byte) (ref, hash string, err error)
}

// Admission configures per-tenant limits (§36.1).
type Admission struct {
	// DailyBudgetMicros: 0 = unlimited. Per-tenant overrides win.
	DailyBudgetMicros  int64
	TenantBudgetMicros map[string]int64
	// MaxInflightPerTenant bounds concurrent model requests (0 = unlimited).
	MaxInflightPerTenant int
}

type Gateway struct {
	mu        sync.RWMutex
	policy    RoutePolicy // base policy merged with the overlay
	base      RoutePolicy
	overlay   Overlay
	overGen   int64
	extra     map[string]Provider // overlay providers
	providers map[string]Provider
	ledger    Ledger
	artifacts Artifacts
	breakers  *Breakers
	scorer    Scorer
	admission Admission
	log       *slog.Logger

	inflightMu sync.Mutex
	inflight   map[string]int
	now        func() time.Time
}

func New(policy RoutePolicy, ledger Ledger, arts Artifacts, adm Admission, log *slog.Logger, providers ...Provider) *Gateway {
	if log == nil {
		log = slog.Default()
	}
	g := &Gateway{policy: policy, providers: map[string]Provider{}, ledger: ledger, artifacts: arts,
		breakers: NewBreakers(3, 30*time.Second), admission: adm, log: log, inflight: map[string]int{}, now: time.Now}
	if g.policy.MaxAttempts == 0 {
		g.policy.MaxAttempts = 2
	}
	g.base = g.policy
	for _, p := range providers {
		g.providers[p.ID()] = p
	}
	return g
}

func (g *Gateway) Policy() RoutePolicy {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.policy
}

// SetPolicy swaps the route policy (new version); in-flight requests keep
// the version they recorded.
func (g *Gateway) SetPolicy(p RoutePolicy) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.base = p
	g.policy = mergeOverlay(p, g.overlay, g.overGen)
}

// Overlay is a set of runtime endpoints (e.g. models a tenant configured
// through the API) layered over the configured route policy. Overlay
// endpoints should be entitled to specific tenants (Endpoint.Tenants);
// their aliases are preferred over the base policy's.
type Overlay struct {
	Endpoints []Endpoint
	Aliases   map[string][]string
	Providers []Provider // keyed by ID(); must not reuse a base provider ID
}

// SetOverlay replaces the runtime overlay. Endpoints whose ID clashes with
// a base endpoint are dropped (configuration wins).
func (g *Gateway) SetOverlay(o Overlay) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.overGen++
	g.extra = map[string]Provider{}
	for _, p := range o.Providers {
		if _, clash := g.providers[p.ID()]; !clash {
			g.extra[p.ID()] = p
		}
	}
	g.overlay = o
	g.policy = mergeOverlay(g.base, o, g.overGen)
}

func mergeOverlay(base RoutePolicy, o Overlay, gen int64) RoutePolicy {
	if len(o.Endpoints) == 0 {
		return base
	}
	p := base
	seen := map[string]bool{}
	p.Endpoints = append([]Endpoint(nil), base.Endpoints...)
	for _, e := range base.Endpoints {
		seen[e.ID] = true
	}
	added := map[string]bool{}
	for _, e := range o.Endpoints {
		if !seen[e.ID] {
			seen[e.ID] = true
			added[e.ID] = true
			p.Endpoints = append(p.Endpoints, e)
		}
	}
	p.Aliases = map[string][]string{}
	for k, v := range base.Aliases {
		p.Aliases[k] = append([]string(nil), v...)
	}
	for alias, ids := range o.Aliases {
		var front []string
		for _, id := range ids {
			if added[id] {
				front = append(front, id)
			}
		}
		p.Aliases[alias] = append(front, p.Aliases[alias]...)
	}
	// A distinct version so model requests record which overlay routed them.
	p.Version = base.Version*1_000_000 + gen
	return p
}

func (g *Gateway) provider(id string) (Provider, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if p, ok := g.providers[id]; ok {
		return p, true
	}
	p, ok := g.extra[id]
	return p, ok
}

func (g *Gateway) Breakers() *Breakers {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.breakers
}

// SetScorer replaces the soft routing score (the route_policy plugin).
func (g *Gateway) SetScorer(s Scorer) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.scorer = s
}

// SetBreakers replaces the per-endpoint circuit breakers.
func (g *Gateway) SetBreakers(b *Breakers) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.breakers = b
}

func (g *Gateway) routing() (RoutePolicy, *Breakers, Scorer) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.policy, g.breakers, g.scorer
}

// EstimateTokens is a conservative byte-based estimate used for admission and
// context-window filtering before the provider reports exact usage.
func EstimateTokens(req Request) int64 {
	b, _ := json.Marshal(req)
	return int64(len(b))/3 + 16
}

// Resolve returns the endpoint the alias would route to now (no side effects).
// The context assembler uses it to pick a chat template and context window.
func (g *Gateway) Resolve(req Request, sc Scope) (Endpoint, error) {
	policy, br, scorer := g.routing()
	cands, err := policy.Route(req, sc, EstimateTokens(req), br, scorer, g.now())
	if err != nil {
		return Endpoint{}, err
	}
	return cands[0].Endpoint, nil
}

// Generate executes one model request: ledger row (fenced) → route →
// admission → attempts with pre-send fallback → aggregated, validated result.
func (g *Gateway) Generate(ctx context.Context, sc Scope, req Request, sink Sink) (Response, error) {
	policy, br, scorer := g.routing()
	canon, err := json.Marshal(req)
	if err != nil {
		return Response{}, err
	}
	reqRef, reqHash, err := g.artifacts.Put(ctx, sc.TenantID, "model-request", canon)
	if err != nil {
		return Response{}, errf(ErrProviderUnavailable, false, "store request artifact: %v", err)
	}
	rec := RequestRecord{ID: uuid.NewString(), ModelAlias: req.ModelAlias, RoutePolicyID: policy.ID,
		RoutePolicyVersion: policy.Version, RequestRef: reqRef, RequestHash: reqHash, ManifestRef: sc.ManifestRef,
		MaxOutputTokens: req.MaxOutputTokens}
	if err := g.ledger.CreateModelRequest(ctx, sc, rec); err != nil {
		return Response{}, err // fence lost: the caller must drop the step result
	}
	finish := func(res RequestResult) {
		res.ID = rec.ID
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := g.ledger.FinishModelRequest(fctx, sc, res); err != nil {
			g.log.Warn("finish model request", "id", rec.ID, "err", err)
		}
	}

	estimate := EstimateTokens(req)
	cands, err := policy.Route(req, sc, estimate, br, scorer, g.now())
	if err != nil {
		ge, _ := AsError(err)
		finish(RequestResult{Status: "FAILED", ErrorCode: string(ge.Code)})
		return Response{}, err
	}
	if err := g.admit(ctx, sc, cands[0].Endpoint, estimate, req.MaxOutputTokens); err != nil {
		ge, _ := AsError(err)
		finish(RequestResult{Status: "FAILED", ErrorCode: string(ge.Code)})
		return Response{}, err
	}
	release, err := g.acquire(sc.TenantID)
	if err != nil {
		finish(RequestResult{Status: "FAILED", ErrorCode: string(ErrRateLimited)})
		return Response{}, err
	}
	defer release()

	var lastErr error
	for i, cand := range cands {
		if i >= policy.MaxAttempts {
			break
		}
		resp, err := g.attempt(ctx, sc, rec, i+1, cand, req, estimate, sink)
		if err == nil {
			resp.ModelRequestID = rec.ID
			finish(RequestResult{Status: "SUCCEEDED", ResponseRef: resp.respRef, Endpoint: cand.Endpoint.ID,
				Model: resp.ResolvedModel, Usage: resp.Usage, CostMicros: resp.CostMicros})
			return resp, nil
		}
		lastErr = err
		ge, ok := AsError(err)
		if !ok || !ge.Fallbackable() {
			break
		}
		g.log.Info("model fallback", "request", rec.ID, "from", cand.Endpoint.ID, "code", ge.Code)
	}
	ge, ok := AsError(lastErr)
	status, code := "FAILED", string(ErrProviderUnavailable)
	if ok {
		code = string(ge.Code)
		switch ge.Code {
		case ErrCancelled:
			status = "CANCELLED"
		case ErrTimeoutOutcomeUnknown:
			status = "OUTCOME_UNKNOWN"
		}
	}
	finish(RequestResult{Status: status, ErrorCode: code})
	return Response{}, lastErr
}

func (g *Gateway) attempt(ctx context.Context, sc Scope, rec RequestRecord, no int, cand Candidate, req Request,
	estimate int64, sink Sink) (Response, error) {
	ep := cand.Endpoint
	prov, ok := g.provider(ep.ProviderID)
	if !ok {
		return Response{}, errf(ErrProviderUnavailable, false, "provider %q not configured", ep.ProviderID)
	}
	reason, _ := json.Marshal(cand.Reason)
	att := AttemptRecord{ID: uuid.NewString(), ModelRequestID: rec.ID, AttemptNo: no, ProviderID: ep.ProviderID,
		EndpointID: ep.ID, ModelID: ep.ModelID, ModelVersion: ep.ModelVersion, RouteReason: reason}
	if err := g.ledger.StartModelAttempt(ctx, sc, att); err != nil {
		return Response{}, err
	}
	est := Usage{InputTokens: estimate, OutputTokens: req.MaxOutputTokens}
	estID := uuid.NewString()
	_ = g.ledger.AppendUsage(ctx, sc.TenantID, UsageRecord{ID: estID, AttemptID: att.ID, State: UsageEstimated,
		Revision: 1, Usage: &est, PricingVersion: ep.Pricing.Version, CostMicros: ep.Pricing.CostMicros(est)})

	agg := NewAggregator(req.Tools)
	names := ToolNameMap(req.Tools)
	emit := func(ev Event) {
		ev.ModelRequestID, ev.AttemptNo = rec.ID, no
		if ev.ToolName != "" {
			if n, ok := names[ev.ToolName]; ok {
				ev.ToolName = n
			}
		}
		agg.Add(ev)
		if sink != nil {
			sink(ev)
		}
	}
	err := prov.Stream(ctx, ep, req, emit)
	var resp Response
	if err == nil {
		resp, err = agg.Result()
	}
	if err != nil && ctx.Err() != nil {
		if _, ok := AsError(err); !ok || errors.Is(ctx.Err(), context.Canceled) {
			sent := agg.TTFT() > 0
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				err = errf(ErrTimeoutOutcomeUnknown, true, "deadline exceeded")
				if !sent {
					err = errf(ErrTimeoutBeforeSend, false, "deadline exceeded before first token")
				}
			} else {
				err = &Error{Code: ErrCancelled, Sent: true, Msg: "cancelled"}
			}
		}
	}
	if err != nil {
		ge, ok := AsError(err)
		if !ok {
			ge = errf(ErrStreamInterrupted, true, "%v", err)
			err = ge
		}
		status := "FAILED"
		switch ge.Code {
		case ErrCancelled:
			status = "CANCELLED"
		case ErrTimeoutOutcomeUnknown:
			status = "OUTCOME_UNKNOWN"
		}
		if ge.Code == ErrRateLimited || ge.Code == ErrProviderUnavailable || ge.Code == ErrStreamInterrupted ||
			ge.Code == ErrTimeoutBeforeSend || ge.Code == ErrTimeoutOutcomeUnknown {
			g.Breakers().Record(ep.ID, false, g.now())
		}
		u := agg.Usage()
		usage := UsageRecord{ID: uuid.NewString(), AttemptID: att.ID, Revision: 2, SupersedesID: estID,
			PricingVersion: ep.Pricing.Version}
		switch {
		case !ge.Sent:
			zero := Usage{}
			usage.State, usage.Usage = UsageReported, &zero
		case u != (Usage{}):
			usage.State, usage.Usage, usage.CostMicros = UsageReported, &u, ep.Pricing.CostMicros(u)
		default:
			usage.State = UsageUnknown // billed but not reported: reconcile from provider invoices
		}
		_ = g.ledger.AppendUsage(context.WithoutCancel(ctx), sc.TenantID, usage)
		_ = g.ledger.FinishModelAttempt(context.WithoutCancel(ctx), sc, AttemptResult{ID: att.ID, ModelRequestID: rec.ID,
			Status: status, ErrorCode: string(ge.Code), TTFT: agg.TTFT(), Usage: u, CostMicros: usage.CostMicros,
			ProviderRequestID: agg.resp.ProviderRequestID})
		return Response{}, err
	}

	g.Breakers().Record(ep.ID, true, g.now())
	resp.ProviderID, resp.EndpointID, resp.AttemptID = ep.ProviderID, ep.ID, att.ID
	if resp.ResolvedModel == "" {
		resp.ResolvedModel = ep.ModelID
	}
	resp.CostMicros = ep.Pricing.CostMicros(resp.Usage)
	body, _ := json.Marshal(resp)
	ref, _, perr := g.artifacts.Put(ctx, sc.TenantID, "model-response", body)
	if perr != nil {
		return Response{}, errf(ErrStreamInterrupted, true, "store response artifact: %v", perr)
	}
	resp.respRef = ref
	_ = g.ledger.AppendUsage(ctx, sc.TenantID, UsageRecord{ID: uuid.NewString(), AttemptID: att.ID, State: UsageReported,
		Revision: 2, SupersedesID: estID, Usage: &resp.Usage, PricingVersion: ep.Pricing.Version, CostMicros: resp.CostMicros})
	if err := g.ledger.FinishModelAttempt(ctx, sc, AttemptResult{ID: att.ID, ModelRequestID: rec.ID, Status: "SUCCEEDED",
		ProviderRequestID: resp.ProviderRequestID, ResponseRef: ref, FinishReason: resp.FinishReason, TTFT: resp.TTFT,
		Usage: resp.Usage, CostMicros: resp.CostMicros}); err != nil {
		return Response{}, err
	}
	return resp, nil
}

func (g *Gateway) admit(ctx context.Context, sc Scope, ep Endpoint, estimate, maxOut int64) error {
	budget := g.admission.DailyBudgetMicros
	if b, ok := g.admission.TenantBudgetMicros[sc.TenantID]; ok {
		budget = b
	}
	if budget <= 0 {
		return nil
	}
	now := g.now().UTC()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	spent, err := g.ledger.SpentMicros(ctx, sc.TenantID, day)
	if err != nil {
		return errf(ErrProviderUnavailable, false, "budget lookup: %v", err)
	}
	need := ep.Pricing.CostMicros(Usage{InputTokens: estimate, OutputTokens: maxOut})
	if spent+need > budget {
		return errf(ErrBudgetExceeded, false, "tenant %s daily model budget exhausted (spent %d + need %d > %d µUSD)",
			sc.TenantID, spent, need, budget)
	}
	return nil
}

func (g *Gateway) acquire(tenant string) (func(), error) {
	if g.admission.MaxInflightPerTenant <= 0 {
		return func() {}, nil
	}
	g.inflightMu.Lock()
	defer g.inflightMu.Unlock()
	if g.inflight[tenant] >= g.admission.MaxInflightPerTenant {
		return nil, &Error{Code: ErrRateLimited, Msg: fmt.Sprintf("tenant %s has too many in-flight model requests", tenant),
			RetryAfter: time.Second}
	}
	g.inflight[tenant]++
	return func() {
		g.inflightMu.Lock()
		g.inflight[tenant]--
		g.inflightMu.Unlock()
	}, nil
}
