package sre

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/world"
)

// CMDBConfig configures a CMDB over a small generic HTTP contract:
//
//	GET  {services_path}                 -> [service] or {"items":[service]}
//	GET  {services_path}/{ns}/{name}     -> service
//	POST {change_requests_path}          -> {"id": "..."}   (body: change request)
//	GET  {change_requests_path}?external_id=<key> -> [{"id","status"}]
//
// service = {"namespace","name","owner","team","tier","oncall","depends_on":["ns/name"]}.
//
// Mark the capability authoritative (capability Options.Authoritative)
// when the CMDB is the system of record: its facts are then T1.
type CMDBConfig struct {
	capability.HTTPConfig
	ServicesPath       string              `json:"services_path,omitempty"`        // default /api/v1/services
	ChangeRequestsPath string              `json:"change_requests_path,omitempty"` // default /api/v1/change-requests
	CollectEvery       capability.Duration `json:"collect_every,omitempty"`        // default 10m
	MaxServices        int                 `json:"max_services,omitempty"`         // default 5000
}

type cmdbService struct {
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Owner     string   `json:"owner"`
	Team      string   `json:"team"`
	Tier      string   `json:"tier"`
	OnCall    string   `json:"oncall"`
	DependsOn []string `json:"depends_on"`
}

func (s cmdbService) ref() string {
	if s.Namespace == "" {
		return "service://" + s.Name
	}
	return "service://" + s.Namespace + "/" + s.Name
}

type cmdb struct {
	capability.Base
	cfg CMDBConfig
	c   *capability.Client
}

func newCMDB(cfg CMDBConfig) (*cmdb, error) {
	if cfg.ServicesPath == "" {
		cfg.ServicesPath = "/api/v1/services"
	}
	if cfg.ChangeRequestsPath == "" {
		cfg.ChangeRequestsPath = "/api/v1/change-requests"
	}
	if cfg.CollectEvery == 0 {
		cfg.CollectEvery = capability.Duration(10 * time.Minute)
	}
	if cfg.MaxServices <= 0 {
		cfg.MaxServices = 5000
	}
	c, err := capability.NewClient(cfg.HTTPConfig)
	if err != nil {
		return nil, fmt.Errorf("cmdb: %w", err)
	}
	return &cmdb{cfg: cfg, c: c}, nil
}

func (c *cmdb) ID() string      { return "cmdb" }
func (c *cmdb) Version() string { return "1" }

func (c *cmdb) services(ctx context.Context) ([]cmdbService, error) {
	var raw json.RawMessage
	if err := c.c.Get(ctx, c.cfg.ServicesPath, nil, &raw); err != nil {
		return nil, err
	}
	var list []cmdbService
	if err := json.Unmarshal(raw, &list); err != nil {
		var wrapped struct {
			Items []cmdbService `json:"items"`
		}
		if err2 := json.Unmarshal(raw, &wrapped); err2 != nil {
			return nil, fmt.Errorf("cmdb: services: %w", err)
		}
		list = wrapped.Items
	}
	if len(list) > c.cfg.MaxServices {
		return nil, fmt.Errorf("cmdb: %d services exceed max_services %d", len(list), c.cfg.MaxServices)
	}
	return list, nil
}

func (c *cmdb) RegisterWorld(r capability.WorldRegistry) error {
	if c.cfg.CollectEvery <= 0 {
		return nil
	}
	return r.AddCollector("services", c.cfg.CollectEvery.D(), capability.CollectorFunc(c.collect))
}

func (c *cmdb) collect(ctx context.Context, _ time.Time) (capability.Observation, error) {
	list, err := c.services(ctx)
	if err != nil {
		return capability.Observation{}, err
	}
	var o capability.Observation
	for _, s := range list {
		ref, err := cpkit.NormalizeRef(s.ref())
		if err != nil || s.Name == "" {
			continue
		}
		for pred, v := range map[string]string{"owner": s.Owner, "team": s.Team, "tier": s.Tier, "oncall": s.OnCall} {
			if v != "" {
				o.Facts = append(o.Facts, world.Fact{SubjectRef: ref, Predicate: pred, Value: cpkit.Truncate(v, 200),
					TrustTier: world.TrustAuthoritative})
			}
		}
		for _, d := range s.DependsOn {
			to, err := cpkit.NormalizeRef("service://" + d)
			if err == nil && to != ref {
				o.Relations = append(o.Relations, world.Relation{FromRef: ref, Type: "depends_on", ToRef: to})
			}
		}
	}
	return o, nil
}

func (c *cmdb) RegisterTools(r capability.ToolRegistry) error {
	return r.AddTool(&tools.Func{Desc: tools.Descriptor{Name: "cmdb.lookup", Version: "1", Risk: domain.RiskR0, Trust: "T1",
		Source: "native", TargetArg: "service",
		Description: "Owner, team, tier, on-call and dependencies of a service from the CMDB (service: <namespace>/<name>).",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"}},"required":["service"],"additionalProperties":false}`)},
		Fn: c.toolLookup})
}

// servicePath validates "<ns>/<name>" (or "<name>").
func servicePath(s string) (string, error) {
	s = strings.TrimPrefix(s, "service://")
	parts := strings.Split(s, "/")
	if len(parts) > 2 {
		return "", fmt.Errorf("service %q must be <namespace>/<name>", s)
	}
	for _, p := range parts {
		if !dns1123.MatchString(p) {
			return "", fmt.Errorf("service %q must be <namespace>/<name>", s)
		}
	}
	return s, nil
}

func (c *cmdb) toolLookup(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct{ Service string }
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrNotDispatched, err)
	}
	sp, err := servicePath(a.Service)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrNotDispatched, err)
	}
	var s cmdbService
	if err := c.c.Get(ctx, c.cfg.ServicesPath+"/"+sp, nil, &s); err != nil {
		if capability.IsStatus(err, http.StatusNotFound) {
			return json.Marshal(map[string]any{"found": false})
		}
		return nil, err
	}
	return json.Marshal(map[string]any{"found": true, "service": s})
}

func (c *cmdb) RegisterActions(r capability.ActionRegistry) error { return r.AddAction(&cmdbUpdate{c}) }

// ---- cmdb.propose_update ----

// cmdbUpdate files a change request against a CMDB record (owner, team,
// tier, on-call, dependencies). It never edits the record directly: the
// CMDB's own workflow approves the change.
type cmdbUpdate struct{ c *cmdb }

var cmdbFields = map[string]bool{"owner": true, "team": true, "tier": true, "oncall": true, "depends_on": true}

func (a *cmdbUpdate) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "cmdb.propose_update", Version: "1", ResourceKinds: []string{"service"}, Risk: domain.RiskR1,
		Description: "File a CMDB change request for a service record (owner, team, tier, oncall, depends_on).",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"changes":{"type":"object"},"reason":{"type":"string","maxLength":2000}},` +
			`"required":["changes","reason"],"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, BlastRadius: "record", Timeout: 30 * time.Second}
}

type cmdbArgs struct {
	Changes map[string]any `json:"changes"`
	Reason  string         `json:"reason"`
}

func (a *cmdbUpdate) check(p action.Plan) (string, cmdbArgs, error) {
	var args cmdbArgs
	sp, err := servicePath(p.TargetRef)
	if err != nil || !strings.HasPrefix(p.TargetRef, "service://") {
		return "", args, capability.NotApplied("target must be service://<namespace>/<name>")
	}
	if err := parseArgs(p, &args); err != nil {
		return sp, args, err
	}
	if len(args.Changes) == 0 || strings.TrimSpace(args.Reason) == "" {
		return sp, args, capability.NotApplied("changes and reason are required")
	}
	for k := range args.Changes {
		if !cmdbFields[k] {
			return sp, args, capability.NotApplied("field %q cannot be changed (allowed: owner, team, tier, oncall, depends_on)", k)
		}
	}
	return sp, args, nil
}

func (a *cmdbUpdate) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	sp, _, err := a.check(p)
	if err != nil {
		return refused(err.Error()), nil
	}
	if err := a.c.c.Get(ctx, a.c.cfg.ServicesPath+"/"+sp, nil, nil); err != nil {
		return refused("cmdb record " + sp + ": " + err.Error()), nil
	}
	return action.PreflightResult{Allowed: true, EvidenceRefs: []string{p.TargetRef}}, nil
}

func (a *cmdbUpdate) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	sp, args, err := a.check(p)
	if err != nil {
		return nil, err
	}
	var out struct {
		ID string `json:"id"`
	}
	err = a.c.c.Do(ctx, capability.Request{Method: http.MethodPost, Path: a.c.cfg.ChangeRequestsPath,
		Header: map[string]string{"Idempotency-Key": key},
		Body: map[string]any{"external_id": key, "service": sp, "changes": args.Changes, "reason": args.Reason,
			"requested_by": "sre-harness", "work_id": p.WorkID}}, &out)
	if err != nil {
		return nil, capability.ActionError(err)
	}
	return json.Marshal(map[string]any{"change_request": out.ID, "service": sp})
}

func (a *cmdbUpdate) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	var list []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := a.c.c.Get(ctx, a.c.cfg.ChangeRequestsPath, url.Values{"external_id": {p.EffectKey}}, &list); err != nil {
		return action.InspectResult{}, err
	}
	ok := len(list) > 0
	det, _ := json.Marshal(list)
	return action.InspectResult{Applied: &ok, Detail: det}, nil
}
