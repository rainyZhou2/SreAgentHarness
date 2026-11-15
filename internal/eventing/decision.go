package eventing

import (
	"time"

	"go-sre-harness/internal/cpkit"
)

// Version identifies a rule's content: a recorded decision names the rule
// version that made it, so a changed rule can be compared and rolled back.
func (r Rule) Version() string { return cpkit.MustHash(r)[7:19] }

// Decision records why an event went where it went (design §9.5).
type AlertReference struct {
	InstanceID             string `json:"instance_id"`
	EpisodeID              string `json:"episode_id,omitempty"`
	SymptomID              string `json:"symptom_id"`
	Resolution             string `json:"resolution,omitempty"`
	NotificationSuppressed bool   `json:"notification_suppressed"`
}

type Decision struct {
	TenantID    string `json:"tenant_id"`
	InboxID     string `json:"inbox_id"`
	RuleName    string `json:"rule_name,omitempty"`
	RuleVersion string `json:"rule_version,omitempty"`
	// Decision is the routing result (created, attached, no_rule, ...).
	Decision string `json:"decision"`
	WorkID   string `json:"work_id,omitempty"`
	// Basis: deterministic (a rule keyed the event to the Work), topology
	// (a dependency of an open Work's subject — a lead for investigation,
	// never grounds for a write), or none.
	Basis string `json:"basis"`
	// GroupKey is the correlation group the event fell into.
	GroupKey string          `json:"group_key,omitempty"`
	Alert    *AlertReference `json:"alert,omitempty"`
	// Candidates are the Works considered, with why each was or was not chosen.
	Candidates []Candidate `json:"candidates,omitempty"`
	Reason     string      `json:"reason,omitempty"`
	Late       bool        `json:"late,omitempty"`
	Shadow     bool        `json:"shadow,omitempty"`
	At         time.Time   `json:"at"`
}

// Candidate is one Work considered for an event.
type Candidate struct {
	WorkID   string  `json:"work_id"`
	Relation string  `json:"relation"` // same_group | dependency
	Score    float64 `json:"score"`
	Chosen   bool    `json:"chosen"`
	Reason   string  `json:"reason,omitempty"`
}
