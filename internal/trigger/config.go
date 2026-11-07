// Package trigger turns inbound events (webhooks from alerting, ticketing or
// any other system) and schedules into runs. Nothing vendor-specific lives
// here: a trigger maps an arbitrary JSON payload with ${...} expressions
// (the flow DSL expression language) onto a goal and run context.
package trigger

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"go-sre-harness/internal/flow"
	"go-sre-harness/internal/tools"
)

// Config is one operator-owned trigger.
type Config struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Disabled    bool   `json:"disabled,omitempty"`
	// Tenant is required for schedules. Webhooks run in the caller's
	// tenant; when Tenant is set, only that tenant can deliver to it.
	Tenant  string     `json:"tenant,omitempty"`
	Profile ProfileRef `json:"profile,omitempty"`

	// Items is an expression yielding a list; each element is handled as
	// one event (e.g. "payload.alerts"). Empty: the payload is one event.
	Items string `json:"items,omitempty"`
	// Filter drops events for which it is false.
	Filter string `json:"filter,omitempty"`
	// Resolve marks events that may only update an existing run (attached
	// as evidence) and never start one, e.g. "item.status == 'resolved'".
	Resolve string `json:"resolve,omitempty"`

	// Goal is the goal text template (required).
	Goal string `json:"goal"`
	// Fingerprint identifies "the same problem" for deduplication
	// (default: the rendered goal).
	Fingerprint string            `json:"fingerprint,omitempty"`
	Services    []string          `json:"services,omitempty"`
	Incident    string            `json:"incident,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Window      *WindowConfig     `json:"window,omitempty"`
	// Mode is an optional routing hint for auto profiles: react or flow.
	Mode string `json:"mode,omitempty"`

	Dedup     DedupConfig `json:"dedup,omitempty"`
	RateLimit RateConfig  `json:"rate_limit,omitempty"`
	// SecretEnv names an environment variable holding an HMAC-SHA256 key;
	// when set, requests must carry X-Harness-Signature: sha256=<hex>.
	SecretEnv string `json:"secret_env,omitempty"`

	Schedule *ScheduleConfig `json:"schedule,omitempty"`
}

// ProfileRef selects the profile of created runs (default: the server default).
type ProfileRef struct {
	ID      string `json:"id,omitempty"`
	Version int64  `json:"version,omitempty"`
}

// WindowConfig renders the incident time window.
type WindowConfig struct {
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
	// Lookback opens the window before the event time when Start is empty
	// or cannot be parsed.
	Lookback tools.Duration `json:"lookback,omitempty"`
}

// DedupConfig is the sliding deduplication window.
type DedupConfig struct {
	Disabled bool           `json:"disabled,omitempty"`
	Window   tools.Duration `json:"window,omitempty"` // default 30m
	// ExtendOnHit keeps the window open while duplicates keep arriving.
	ExtendOnHit bool `json:"extend_on_hit,omitempty"`
}

// RateConfig bounds new runs per tenant and trigger on this replica.
type RateConfig struct {
	PerMinute float64 `json:"per_minute,omitempty"` // 0 = unlimited
	Burst     int     `json:"burst,omitempty"`
}

// ScheduleConfig creates one run per interval (patrols, reviews).
type ScheduleConfig struct {
	Interval tools.Duration `json:"interval"`
}

var nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// compiled is a validated trigger.
type compiled struct {
	Config
	items, filter, resolve flow.Expr
	dedupWindow            time.Duration
}

func exprSource(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") && strings.Count(s, "${") == 1 {
		return strings.TrimSpace(s[2 : len(s)-1])
	}
	return s
}

func compileExpr(field, src string) (flow.Expr, error) {
	if strings.TrimSpace(src) == "" {
		return nil, nil
	}
	e, err := flow.CompileExpr(exprSource(src))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", field, err)
	}
	return e, nil
}

func compile(c Config) (*compiled, error) {
	if !nameRE.MatchString(c.Name) {
		return nil, fmt.Errorf("trigger name %q must match %s", c.Name, nameRE)
	}
	t := &compiled{Config: c}
	wrap := func(err error) error { return fmt.Errorf("trigger %s: %w", c.Name, err) }
	if strings.TrimSpace(c.Goal) == "" {
		return nil, wrap(fmt.Errorf("goal is required"))
	}
	var err error
	if t.items, err = compileExpr("items", c.Items); err != nil {
		return nil, wrap(err)
	}
	if t.filter, err = compileExpr("filter", c.Filter); err != nil {
		return nil, wrap(err)
	}
	if t.resolve, err = compileExpr("resolve", c.Resolve); err != nil {
		return nil, wrap(err)
	}
	templates := []string{c.Goal, c.Fingerprint, c.Incident}
	templates = append(templates, c.Services...)
	for _, v := range c.Labels {
		templates = append(templates, v)
	}
	if c.Window != nil {
		templates = append(templates, c.Window.Start, c.Window.End)
		if c.Window.Lookback < 0 {
			return nil, wrap(fmt.Errorf("window.lookback must be >= 0"))
		}
	}
	for _, tpl := range templates {
		if err := flow.ValidateInterpolation(tpl, nil); err != nil {
			return nil, wrap(fmt.Errorf("template %q: %w", tpl, err))
		}
	}
	switch c.Mode {
	case "", "react", "flow":
	default:
		return nil, wrap(fmt.Errorf("mode must be react or flow"))
	}
	if len(c.Services) > 20 || len(c.Labels) > 50 {
		return nil, wrap(fmt.Errorf("at most 20 services and 50 labels"))
	}
	t.dedupWindow = c.Dedup.Window.D()
	if t.dedupWindow == 0 {
		t.dedupWindow = 30 * time.Minute
	}
	if t.dedupWindow < time.Second || t.dedupWindow > 7*24*time.Hour {
		return nil, wrap(fmt.Errorf("dedup.window must be between 1s and 168h"))
	}
	if c.RateLimit.PerMinute < 0 || c.RateLimit.Burst < 0 {
		return nil, wrap(fmt.Errorf("rate_limit values must be >= 0"))
	}
	if s := c.Schedule; s != nil {
		if s.Interval.D() < time.Minute {
			return nil, wrap(fmt.Errorf("schedule.interval must be at least 1m"))
		}
		if c.Tenant == "" {
			return nil, wrap(fmt.Errorf("schedule requires tenant"))
		}
		if c.Items != "" || c.Filter != "" || c.Resolve != "" {
			return nil, wrap(fmt.Errorf("schedule triggers have no payload: items, filter and resolve are not allowed"))
		}
	}
	return t, nil
}
