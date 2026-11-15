// Package alerting defines the SRE alert projection contract independently of
// event transport, persistence, and Incident execution.
package alerting

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
)

const (
	EventFiring   = "alert.firing"
	EventResolved = "alert.resolved"

	StateFiring   = "FIRING"
	StateResolved = "RESOLVED"

	ResolutionMatched    = "matched"
	ResolutionAmbiguous  = "ambiguous"
	ResolutionOutOfOrder = "out_of_order"
)

var ErrInvalid = errors.New("invalid alert projection")

type Policy struct {
	ReopenWindow      cpkit.Duration
	FlappingWindow    cpkit.Duration
	FlappingThreshold int
}

func (p *Policy) Normalize() error {
	if p.ReopenWindow == 0 {
		p.ReopenWindow = cpkit.Duration(time.Hour)
	}
	if p.FlappingWindow == 0 {
		p.FlappingWindow = cpkit.Duration(24 * time.Hour)
	}
	if p.FlappingThreshold == 0 {
		p.FlappingThreshold = 3
	}
	if p.ReopenWindow < 0 || p.ReopenWindow.D() > 7*24*time.Hour {
		return fmt.Errorf("%w: reopen window must be within [0, 7d]", ErrInvalid)
	}
	if p.FlappingWindow <= 0 || p.FlappingWindow.D() > 30*24*time.Hour {
		return fmt.Errorf("%w: flapping window must be within (0, 30d]", ErrInvalid)
	}
	if p.FlappingThreshold < 2 || p.FlappingThreshold > 100 {
		return fmt.Errorf("%w: flapping threshold must be within [2, 100]", ErrInvalid)
	}
	return nil
}

type Input struct {
	InboxID                string            `json:"inbox_id"`
	TenantID               string            `json:"tenant_id"`
	Fingerprint            string            `json:"fingerprint,omitempty"`
	AlertName              string            `json:"alert_name"`
	SubjectRef             string            `json:"subject_ref"`
	SymptomKind            string            `json:"symptom_kind"`
	ImpactScope            string            `json:"impact_scope"`
	Severity               string            `json:"severity"`
	EventType              string            `json:"event_type"`
	Labels                 map[string]string `json:"labels,omitempty"`
	OccurredAt             time.Time         `json:"occurred_at"`
	ObservedAt             time.Time         `json:"observed_at"`
	NotificationSuppressed bool              `json:"notification_suppressed"`
	SuppressionReasons     []string          `json:"suppression_reasons,omitempty"`
	SymptomKey             string            `json:"symptom_key"`
	Policy                 Policy            `json:"policy"`
}

type Instance struct {
	ID                     string            `json:"id"`
	TenantID               string            `json:"tenant_id"`
	InboxID                string            `json:"inbox_id"`
	EpisodeID              string            `json:"episode_id,omitempty"`
	SymptomID              string            `json:"symptom_id,omitempty"`
	Fingerprint            string            `json:"fingerprint,omitempty"`
	AlertName              string            `json:"alert_name"`
	SubjectRef             string            `json:"subject_ref"`
	SymptomKind            string            `json:"symptom_kind"`
	ImpactScope            string            `json:"impact_scope"`
	Severity               string            `json:"severity"`
	EventType              string            `json:"event_type"`
	Labels                 map[string]string `json:"labels,omitempty"`
	OccurredAt             time.Time         `json:"occurred_at"`
	ObservedAt             time.Time         `json:"observed_at"`
	NotificationSuppressed bool              `json:"notification_suppressed"`
	SuppressionReasons     []string          `json:"suppression_reasons,omitempty"`
	Resolution             string            `json:"resolution"`
}

type Episode struct {
	ID            string     `json:"id"`
	TenantID      string     `json:"tenant_id"`
	Fingerprint   string     `json:"fingerprint"`
	AlertName     string     `json:"alert_name"`
	SubjectRef    string     `json:"subject_ref"`
	State         string     `json:"state"`
	SymptomID     string     `json:"symptom_id"`
	OpenedAt      time.Time  `json:"opened_at"`
	LastEventAt   time.Time  `json:"last_event_at"`
	ResolvedAt    *time.Time `json:"resolved_at,omitempty"`
	ReopenCount   int        `json:"reopen_count"`
	FlappingCount int        `json:"flapping_count"`
	LastReopenAt  *time.Time `json:"last_reopen_at,omitempty"`
	Flapping      bool       `json:"flapping"`
	Version       int        `json:"version"`
}

type Symptom struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	Key            string    `json:"key"`
	SubjectRef     string    `json:"subject_ref"`
	Kind           string    `json:"kind"`
	ImpactScope    string    `json:"impact_scope"`
	State          string    `json:"state"`
	WindowStart    time.Time `json:"window_start"`
	WindowEnd      time.Time `json:"window_end"`
	ActiveEpisodes int       `json:"active_episodes"`
	Version        int       `json:"version"`
}

type Projection struct {
	Instance Instance `json:"instance"`
	Episode  *Episode `json:"episode,omitempty"`
	Symptom  Symptom  `json:"symptom"`
}

func SupportedEvent(env eventing.Envelope) bool {
	return env.Type == EventFiring || env.Type == EventResolved
}

func NewInput(env eventing.Envelope, inboxID string, p Policy) (Input, error) {
	if err := p.Normalize(); err != nil {
		return Input{}, err
	}
	if !SupportedEvent(env) {
		return Input{}, fmt.Errorf("%w: unsupported event type %q", ErrInvalid, env.Type)
	}
	if inboxID == "" || env.TenantID == "" || env.SubjectRef == "" || env.OccurredAt.IsZero() {
		return Input{}, fmt.Errorf("%w: inbox, tenant, subject, and occurrence time are required", ErrInvalid)
	}
	alertName := strings.TrimSpace(env.Labels["alertname"])
	if alertName == "" {
		alertName = strings.TrimSpace(env.Labels["alert_name"])
	}
	if alertName == "" {
		alertName = strings.TrimSpace(env.DedupKey)
	}
	if alertName == "" {
		alertName = "alert"
	}
	suppressed, reasons, err := suppression(env.Labels)
	if err != nil {
		return Input{}, err
	}
	fingerprint := strings.TrimSpace(env.DedupKey)
	if fingerprint == "" {
		fingerprint = strings.TrimSpace(env.Labels["fingerprint"])
	}
	if fingerprint == "" && env.Type == EventFiring {
		identity := map[string]string{}
		for _, k := range env.SortedLabelKeys() {
			switch k {
			case "severity", "fingerprint", "silenced", "inhibited":
				continue
			}
			identity[k] = env.Labels[k]
		}
		fingerprint = cpkit.MustHash(struct {
			AlertName string            `json:"alert_name"`
			Subject   string            `json:"subject"`
			Labels    map[string]string `json:"labels"`
		}{alertName, env.SubjectRef, identity})
	}
	symptomKind := strings.TrimSpace(env.Labels["symptom"])
	if symptomKind == "" {
		symptomKind = alertName
	}
	impactScope := strings.TrimSpace(env.Labels["impact_scope"])
	if impactScope == "" {
		impactScope = env.SubjectRef
	}
	severity := strings.TrimSpace(env.Labels["severity"])
	if severity == "" {
		severity = "unknown"
	}
	labels := make(map[string]string, len(env.Labels))
	for k, v := range env.Labels {
		labels[k] = v
	}
	return Input{InboxID: inboxID, TenantID: env.TenantID, Fingerprint: fingerprint, AlertName: alertName,
		SubjectRef: env.SubjectRef, SymptomKind: symptomKind, ImpactScope: impactScope, Severity: severity,
		EventType: env.Type, Labels: labels, OccurredAt: env.OccurredAt.UTC(), ObservedAt: env.ObservedAt.UTC(),
		NotificationSuppressed: suppressed, SuppressionReasons: reasons, Policy: p,
		SymptomKey: cpkit.MustHash(struct {
			TenantID, SubjectRef, Kind, ImpactScope string
		}{env.TenantID, env.SubjectRef, symptomKind, impactScope})}, nil
}

func suppression(labels map[string]string) (bool, []string, error) {
	var reasons []string
	for _, key := range []string{"silenced", "inhibited"} {
		value := labels[key]
		if value == "" {
			continue
		}
		ok, err := strconv.ParseBool(value)
		if err != nil {
			return false, nil, fmt.Errorf("%w: %s must be boolean", ErrInvalid, key)
		}
		if ok {
			reasons = append(reasons, key)
		}
	}
	return len(reasons) > 0, reasons, nil
}
