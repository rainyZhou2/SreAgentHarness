package trigger

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
)

// Runs creates runs and follows up on them (implemented by api.Server).
type Runs interface {
	StartRun(ctx context.Context, req api.StartRunRequest) (domain.Run, bool, error)
	GetRun(ctx context.Context, tenantID, runID string) (domain.Run, error)
	InjectEvidence(ctx context.Context, tenantID, runID, actorID, source, content, key string) error
}

// Dedup is the shared deduplication window (PostgreSQL or SQLite).
type Dedup interface {
	ClaimTrigger(ctx context.Context, tenantID, key string, window time.Duration, extend bool) (domain.TriggerClaim, error)
	BindTrigger(ctx context.Context, tenantID, key, claimID, runID string) error
	ReleaseTrigger(ctx context.Context, tenantID, key, claimID string) error
}

// Errors returned by Handle.
var (
	ErrUnknownTrigger = errors.New("unknown trigger")
	ErrSignature      = errors.New("invalid or missing signature")
	ErrMisconfigured  = errors.New("trigger is misconfigured")
	ErrPayload        = errors.New("payload must be a JSON value")
)

// Item outcomes.
const (
	ActionCreated     = "created"
	ActionDuplicate   = "deduplicated"
	ActionFiltered    = "filtered"
	ActionResolved    = "resolved"
	ActionIgnored     = "ignored"
	ActionRateLimited = "rate_limited"
	ActionError       = "error"
)

// MaxItems bounds the fan-out of one delivery.
const MaxItems = 100

// Result reports what happened to each event of a delivery.
type Result struct {
	Trigger string       `json:"trigger"`
	Items   []ItemResult `json:"items"`
}

type ItemResult struct {
	Index    int    `json:"index"`
	Action   string `json:"action"`
	RunID    string `json:"run_id,omitempty"`
	DedupKey string `json:"dedup_key,omitempty"`
	Hits     int    `json:"hits,omitempty"`
	Error    string `json:"error,omitempty"`
}

// MirrorEvent is one webhook event as v1 handled it, offered to the v2
// event engine in shadow mode so both behaviours can be compared before
// the switch (RFC P0-3). It carries data only; v1 does not depend on it.
type MirrorEvent struct {
	Trigger     string
	TenantID    string
	ActorID     string
	Fingerprint string
	Goal        string
	Services    []string
	Incident    string
	Labels      map[string]string
	Item        any
	ReceivedAt  time.Time
	// V1Action and V1RunID are what the v1 trigger did with the event.
	V1Action string
	V1RunID  string
}

// Mirror receives v1 webhook events (e.g. the v2 event inbox). It must not
// block or fail the v1 path; errors are logged by the caller.
type Mirror interface {
	MirrorTriggerEvent(ctx context.Context, e MirrorEvent) error
}

// Service evaluates triggers.
type Service struct {
	Runs  Runs
	Dedup Dedup
	Log   *slog.Logger
	// Mirror, when set, receives every rendered webhook event after v1
	// handled it.
	Mirror Mirror
	// Now is the clock (tests); default time.Now.
	Now func() time.Time

	triggers map[string]*compiled
	order    []string
	mu       sync.Mutex
	buckets  map[string]*bucket
}

// New validates the trigger configuration.
func New(cfgs []Config, runs Runs, dedup Dedup, log *slog.Logger) (*Service, error) {
	if log == nil {
		log = slog.Default()
	}
	s := &Service{Runs: runs, Dedup: dedup, Log: log, triggers: map[string]*compiled{}, buckets: map[string]*bucket{}}
	for _, c := range cfgs {
		t, err := compile(c)
		if err != nil {
			return nil, err
		}
		if _, dup := s.triggers[c.Name]; dup {
			return nil, fmt.Errorf("trigger %s is defined twice", c.Name)
		}
		s.triggers[c.Name] = t
		s.order = append(s.order, c.Name)
	}
	sort.Strings(s.order)
	return s, nil
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

// Info is the public view of a trigger (no secrets).
type Info struct {
	Name        string     `json:"name"`
	Description string     `json:"description,omitempty"`
	Disabled    bool       `json:"disabled,omitempty"`
	Kind        string     `json:"kind"` // webhook | schedule
	Profile     ProfileRef `json:"profile,omitempty"`
	Signed      bool       `json:"signed"`
	DedupWindow string     `json:"dedup_window"`
	Interval    string     `json:"interval,omitempty"`
}

// List returns the triggers visible to a tenant: unscoped ones and the
// tenant's own.
func (s *Service) List(tenantID string) []Info {
	out := make([]Info, 0, len(s.order))
	for _, name := range s.order {
		t := s.triggers[name]
		if t.Tenant != "" && t.Tenant != tenantID {
			continue
		}
		in := Info{Name: t.Name, Description: t.Description, Disabled: t.Disabled, Kind: "webhook", Profile: t.Profile,
			Signed: t.SecretEnv != "", DedupWindow: t.dedupWindow.String()}
		if t.Schedule != nil {
			in.Kind, in.Interval = "schedule", t.Schedule.Interval.D().String()
		}
		out = append(out, in)
	}
	return out
}

// Handle processes one webhook delivery for the caller's tenant.
func (s *Service) Handle(ctx context.Context, tenantID, actorID, name string, body []byte, headers http.Header) (Result, error) {
	t, ok := s.triggers[name]
	if !ok || t.Disabled || t.Schedule != nil || (t.Tenant != "" && t.Tenant != tenantID) {
		// A trigger scoped to another tenant does not exist for the caller.
		return Result{}, ErrUnknownTrigger
	}
	res := Result{Trigger: name}
	if t.SecretEnv != "" {
		secret := os.Getenv(t.SecretEnv)
		if secret == "" {
			return res, fmt.Errorf("%w: %s is not set", ErrMisconfigured, t.SecretEnv)
		}
		if !validSignature(secret, body, headers.Get("X-Harness-Signature")) {
			return res, ErrSignature
		}
	}
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return res, ErrPayload
	}
	now := s.now()
	base := map[string]any{"payload": payload, "headers": safeHeaders(headers), "trigger": name,
		"now": now.Format(time.RFC3339)}
	items := []any{payload}
	if t.items != nil {
		v, err := t.items.Eval(base)
		if err != nil {
			return res, fmt.Errorf("%w: items: %v", ErrPayload, err)
		}
		list, ok := v.([]any)
		if !ok {
			if v == nil {
				list = nil
			} else {
				list = []any{v}
			}
		}
		items = list
	}
	if len(items) > MaxItems {
		return res, fmt.Errorf("%w: %d events in one delivery, limit is %d", ErrPayload, len(items), MaxItems)
	}
	for i, item := range items {
		ictx := make(map[string]any, len(base)+1)
		for k, v := range base {
			ictx[k] = v
		}
		ictx["item"] = item
		r, ev := s.handleItem(ctx, t, tenantID, actorID, ictx, item, now)
		r.Index = i
		res.Items = append(res.Items, r)
		if ev != nil {
			s.mirror(ctx, t, tenantID, actorID, *ev, item, now, r)
		}
	}
	return res, nil
}

// mirror offers a handled event to the v2 engine; it never affects v1.
func (s *Service) mirror(ctx context.Context, t *compiled, tenantID, actorID string, ev event, item any, now time.Time, r ItemResult) {
	if s.Mirror == nil {
		return
	}
	mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := s.Mirror.MirrorTriggerEvent(mctx, MirrorEvent{Trigger: t.Name, TenantID: tenantID, ActorID: actorID,
		Fingerprint: ev.fingerprint, Goal: ev.goal, Services: ev.ctx.Services, Incident: ev.ctx.Incident,
		Labels: ev.ctx.Labels, Item: item, ReceivedAt: now, V1Action: r.Action, V1RunID: r.RunID}); err != nil {
		s.Log.Warn("trigger mirror", "trigger", t.Name, "tenant", tenantID, "err", err)
	}
}

func (s *Service) handleItem(ctx context.Context, t *compiled, tenantID, actorID string, ictx map[string]any, item any, now time.Time) (ItemResult, *event) {
	if t.filter != nil {
		v, err := t.filter.Eval(ictx)
		if err != nil {
			return ItemResult{Action: ActionError, Error: "filter: " + err.Error()}, nil
		}
		if !flow.Truthy(v) {
			return ItemResult{Action: ActionFiltered}, nil
		}
	}
	resolveOnly := false
	if t.resolve != nil {
		v, err := t.resolve.Eval(ictx)
		if err != nil {
			return ItemResult{Action: ActionError, Error: "resolve: " + err.Error()}, nil
		}
		resolveOnly = flow.Truthy(v)
	}
	ev, err := render(t, ictx, now)
	if err != nil {
		return ItemResult{Action: ActionError, Error: err.Error()}, nil
	}
	ev.content = evidenceText(item)
	if t.Dedup.Disabled {
		if resolveOnly {
			return ItemResult{Action: ActionIgnored}, &ev
		}
		if !s.allow(tenantID, t, now) {
			return ItemResult{Action: ActionRateLimited}, &ev
		}
		key := "trigger:" + t.Name + ":" + shortHash(ev.content+"|"+now.Format(time.RFC3339Nano))
		run, _, err := s.start(ctx, t, tenantID, actorID, ev, key)
		if err != nil {
			return ItemResult{Action: ActionError, Error: err.Error()}, &ev
		}
		return ItemResult{Action: ActionCreated, RunID: run.ID}, &ev
	}
	return s.dedupItem(ctx, t, tenantID, actorID, ev, resolveOnly, now, 0), &ev
}

func (s *Service) dedupItem(ctx context.Context, t *compiled, tenantID, actorID string, ev event, resolveOnly bool, now time.Time, depth int) ItemResult {
	key := t.Name + ":" + ev.fingerprint
	claim, err := s.Dedup.ClaimTrigger(ctx, tenantID, key, t.dedupWindow, t.Dedup.ExtendOnHit)
	if err != nil {
		return ItemResult{Action: ActionError, DedupKey: key, Error: "dedup: " + err.Error()}
	}
	out := ItemResult{DedupKey: key, Hits: claim.Hits}
	if claim.Fresh {
		if resolveOnly {
			// Nothing to resolve: do not keep a claim without a run.
			_ = s.Dedup.ReleaseTrigger(ctx, tenantID, key, claim.ID)
			out.Action = ActionIgnored
			return out
		}
		if !s.allow(tenantID, t, now) {
			_ = s.Dedup.ReleaseTrigger(ctx, tenantID, key, claim.ID)
			out.Action = ActionRateLimited
			return out
		}
		return s.createForClaim(ctx, t, tenantID, actorID, ev, key, claim, out)
	}
	if claim.RunID == "" {
		if resolveOnly {
			out.Action = ActionIgnored
			return out
		}
		// The claim exists but its run is not bound yet (another delivery is
		// creating it, or that attempt failed). Creating with the claim's
		// idempotency key converges on the same run either way.
		return s.createForClaim(ctx, t, tenantID, actorID, ev, key, claim, out)
	}
	run, err := s.Runs.GetRun(ctx, tenantID, claim.RunID)
	if err == nil && run.Status.Terminal() && depth == 0 {
		// The run that handled this problem is over: a new event is a new
		// occurrence, not a duplicate.
		_ = s.Dedup.ReleaseTrigger(ctx, tenantID, key, claim.ID)
		if resolveOnly {
			out.Action, out.RunID = ActionIgnored, claim.RunID
			return out
		}
		return s.dedupItem(ctx, t, tenantID, actorID, ev, resolveOnly, now, depth+1)
	}
	out.RunID = claim.RunID
	source := "trigger:" + t.Name
	if resolveOnly {
		source += ":resolved"
	}
	evKey := "trigger-evidence:" + shortHash(claim.ID+"|"+ev.content)
	if err := s.Runs.InjectEvidence(ctx, tenantID, claim.RunID, actorID, source, ev.content, evKey); err != nil {
		var rj *domain.RejectedError
		if errors.As(err, &rj) {
			// e.g. the run finished between the check and the command.
			s.Log.Info("trigger evidence rejected", "trigger", t.Name, "run_id", claim.RunID, "reason", rj.Reason)
		} else {
			out.Action, out.Error = ActionError, "inject evidence: "+err.Error()
			return out
		}
	}
	out.Action = ActionDuplicate
	if resolveOnly {
		out.Action = ActionResolved
	}
	return out
}

func (s *Service) createForClaim(ctx context.Context, t *compiled, tenantID, actorID string, ev event, key string,
	claim domain.TriggerClaim, out ItemResult) ItemResult {
	run, created, err := s.start(ctx, t, tenantID, actorID, ev, "trigger:"+claim.ID)
	if err != nil {
		if claim.Fresh {
			// Let the next delivery try again instead of deduplicating into
			// a run that does not exist.
			_ = s.Dedup.ReleaseTrigger(ctx, tenantID, key, claim.ID)
		}
		out.Action, out.Error = ActionError, err.Error()
		return out
	}
	if err := s.Dedup.BindTrigger(ctx, tenantID, key, claim.ID, run.ID); err != nil {
		s.Log.Warn("trigger bind", "trigger", t.Name, "run_id", run.ID, "err", err)
	}
	out.RunID = run.ID
	out.Action = ActionCreated
	if !created {
		out.Action = ActionDuplicate
	}
	return out
}

func (s *Service) start(ctx context.Context, t *compiled, tenantID, actorID string, ev event, idemKey string) (domain.Run, bool, error) {
	initiator := "trigger"
	if t.Schedule != nil {
		initiator = "schedule"
	}
	run, created, err := s.Runs.StartRun(ctx, api.StartRunRequest{TenantID: tenantID, ActorID: actorID,
		InitiatorType: initiator, Goal: ev.goal, Context: ev.ctx, ProfileID: t.Profile.ID,
		ProfileVersion: t.Profile.Version, IdempotencyKey: idemKey})
	if err == nil && created {
		s.Log.Info("trigger started run", "trigger", t.Name, "tenant", tenantID, "run_id", run.ID)
	}
	return run, created, err
}

// event is a rendered trigger event.
type event struct {
	goal        string
	fingerprint string
	ctx         domain.GoalContext
	content     string
}

func render(t *compiled, ictx map[string]any, now time.Time) (event, error) {
	str := func(field, tpl string) (string, error) {
		if tpl == "" {
			return "", nil
		}
		v, err := flow.Interpolate(tpl, ictx)
		if err != nil {
			return "", fmt.Errorf("%s: %w", field, err)
		}
		return strings.TrimSpace(flow.Stringify(v)), nil
	}
	var ev event
	var err error
	if ev.goal, err = str("goal", t.Goal); err != nil {
		return ev, err
	}
	if ev.goal == "" {
		return ev, fmt.Errorf("goal rendered empty")
	}
	if len(ev.goal) > 8000 {
		ev.goal = ev.goal[:8000]
	}
	fp := ev.goal
	if t.Fingerprint != "" {
		if fp, err = str("fingerprint", t.Fingerprint); err != nil {
			return ev, err
		}
		if fp == "" {
			fp = ev.goal
		}
	}
	ev.fingerprint = shortHash(fp)
	gc := domain.GoalContext{ReceivedAt: now, Source: "trigger:" + t.Name, Mode: t.Mode}
	for _, tpl := range t.Services {
		v, err := str("services", tpl)
		if err != nil {
			return ev, err
		}
		gc.Services = append(gc.Services, v)
	}
	gc.Services = domain.NormalizeServices(gc.Services)
	if gc.Incident, err = str("incident", t.Incident); err != nil {
		return ev, err
	}
	if len(t.Labels) > 0 {
		gc.Labels = map[string]string{}
		for k, tpl := range t.Labels {
			v, err := str("labels."+k, tpl)
			if err != nil {
				return ev, err
			}
			if v != "" {
				gc.Labels[k] = v
			}
		}
	}
	if w := t.Window; w != nil {
		startS, err := str("window.start", w.Start)
		if err != nil {
			return ev, err
		}
		endS, err := str("window.end", w.End)
		if err != nil {
			return ev, err
		}
		start, okStart := parseTime(startS)
		end, okEnd := parseTime(endS)
		lookback := w.Lookback.D()
		switch {
		case okStart && lookback > 0:
			// Look back from the event start so the window covers the lead-up.
			win := &domain.TimeWindow{Start: start.Add(-lookback)}
			if okEnd && end.After(start) {
				win.End = end
			}
			gc.Window = win
		case okStart:
			win := &domain.TimeWindow{Start: start}
			if okEnd && end.After(start) {
				win.End = end
			}
			gc.Window = win
		case lookback > 0:
			gc.Window = &domain.TimeWindow{Start: now.Add(-lookback)}
		}
	}
	ev.ctx = gc
	return ev, nil
}

// parseTime accepts RFC 3339 and unix seconds or milliseconds.
func parseTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" || strings.HasPrefix(s, "0001-01-01") {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), true
	}
	if n, err := strconv.ParseFloat(s, 64); err == nil && n > 0 {
		if n > 1e12 {
			return time.UnixMilli(int64(n)).UTC(), true
		}
		return time.Unix(int64(n), 0).UTC(), true
	}
	return time.Time{}, false
}

// evidenceText renders an event for inject_evidence (bounded).
func evidenceText(item any) string {
	b, err := json.Marshal(item)
	if err != nil {
		return fmt.Sprint(item)
	}
	const max = 4000
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:16])
}

// validSignature checks "sha256=<hex>" HMAC-SHA256 over the raw body.
func validSignature(secret string, body []byte, header string) bool {
	sig, ok := strings.CutPrefix(strings.TrimSpace(header), "sha256=")
	if !ok {
		return false
	}
	got, err := hex.DecodeString(sig)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hmac.Equal(got, mac.Sum(nil))
}

// Sign returns the X-Harness-Signature value for body (for senders/tests).
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// safeHeaders exposes request headers to expressions, minus credentials.
func safeHeaders(h http.Header) map[string]any {
	out := map[string]any{}
	for k, v := range h {
		lk := strings.ToLower(k)
		switch lk {
		case "authorization", "cookie", "x-harness-signature", "proxy-authorization":
			continue
		}
		if len(v) > 0 {
			out[lk] = v[0]
		}
	}
	return out
}

// ---------- rate limiting ----------

type bucket struct {
	tokens float64
	last   time.Time
}

// allow takes one token from the (tenant, trigger) bucket.
func (s *Service) allow(tenantID string, t *compiled, now time.Time) bool {
	rate := t.RateLimit.PerMinute
	if rate <= 0 {
		return true
	}
	burst := float64(t.RateLimit.Burst)
	if burst < 1 {
		burst = max(1, rate)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := tenantID + "\x00" + t.Name
	b, ok := s.buckets[k]
	if !ok {
		b = &bucket{tokens: burst, last: now}
		s.buckets[k] = b
	}
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = min(burst, b.tokens+elapsed.Minutes()*rate)
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
