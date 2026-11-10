package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/trigger"
)

// TriggerSourcePrefix marks inbox events mirrored from v1 triggers.
const TriggerSourcePrefix = "trigger:"

// TriggerMirror feeds v1 webhook events into the v2 event inbox (RFC P0-3:
// "trigger 新旧模式 Feature Flag，先使用 Shadow Mode 做事件关联比对"). v1 keeps
// creating runs; the inbox records what v2 would have done, labelled with
// what v1 did, for ShadowReport.
type TriggerMirror struct {
	Ingestor *Ingestor
}

// MirrorTriggerEvent implements trigger.Mirror.
func (m TriggerMirror) MirrorTriggerEvent(ctx context.Context, e trigger.MirrorEvent) error {
	env, err := MirrorEnvelope(e)
	if err != nil {
		return err
	}
	_, _, err = m.Ingestor.Ingest(ctx, env)
	return err
}

// MirrorEnvelope converts a v1 trigger event into a v2 envelope. The event
// id is derived from the trigger, fingerprint and item, so redeliveries of
// one webhook event are deduplicated by the inbox.
func MirrorEnvelope(e trigger.MirrorEvent) (eventing.Envelope, error) {
	item, err := json.Marshal(e.Item)
	if err != nil {
		return eventing.Envelope{}, err
	}
	subject := "trigger://" + e.Trigger
	if len(e.Services) > 0 {
		subject = e.Services[0]
		if !strings.Contains(subject, "://") {
			subject = "service://" + subject
		}
	}
	labels := map[string]string{}
	keys := make([]string, 0, len(e.Labels))
	for k := range e.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if len(labels) >= eventing.MaxLabels-4 {
			break
		}
		labels[k] = cpkit.Truncate(e.Labels[k], eventing.MaxLabelValue)
	}
	labels["trigger"] = cpkit.Truncate(e.Trigger, eventing.MaxLabelValue)
	labels["v1_action"] = e.V1Action
	if e.V1RunID != "" {
		labels["v1_run_id"] = e.V1RunID
	}
	if e.Incident != "" {
		labels["incident"] = cpkit.Truncate(e.Incident, eventing.MaxLabelValue)
	}
	id := cpkit.MustHash(struct {
		T, F string
		I    json.RawMessage
	}{e.Trigger, e.Fingerprint, item})[7:47]
	return eventing.Envelope{ID: id, SchemaVersion: 1, TenantID: e.TenantID, Source: TriggerSourcePrefix + e.Trigger,
		Type: "trigger." + e.Trigger, SubjectRef: subject, CorrelationID: e.Incident,
		DedupKey: "trigger:" + e.Trigger + ":" + e.Fingerprint, OccurredAt: e.ReceivedAt, ObservedAt: e.ReceivedAt,
		Confidence: 1, Labels: labels, Payload: item}, nil
}

// ShadowRow compares one mirrored event's v1 outcome with the v2 decision.
type ShadowRow struct {
	InboxID   string    `json:"inbox_id"`
	Trigger   string    `json:"trigger"`
	Subject   string    `json:"subject_ref"`
	At        time.Time `json:"occurred_at"`
	V1        string    `json:"v1_action"`
	V1RunID   string    `json:"v1_run_id,omitempty"`
	V2        string    `json:"v2_decision"`
	V2Would   string    `json:"v2_would"` // create | attach | ignore | watch | none
	Rule      string    `json:"rule,omitempty"`
	Agrees    bool      `json:"agrees"`
	Reason    string    `json:"reason,omitempty"`
	WorkID    string    `json:"work_id,omitempty"`
	Duplicate bool      `json:"-"`
}

// ShadowReport summarizes v1/v2 agreement over a period.
type ShadowReport struct {
	Since         time.Time         `json:"since"`
	Until         time.Time         `json:"until"`
	Events        int               `json:"events"`
	Agreements    int               `json:"agreements"`
	AgreementRate float64           `json:"agreement_rate"`
	Matrix        map[string]int    `json:"matrix"` // "v1_action→v2_would"
	Disagreements []ShadowRow       `json:"disagreements"`
	ByTrigger     map[string][2]int `json:"by_trigger"` // [events, agreements]
}

// v1 outcomes that mean "a new investigation was started".
func v1Creates(a string) bool { return a == trigger.ActionCreated }

// v1 outcomes that mean "folded into an existing investigation".
func v1Attaches(a string) bool { return a == trigger.ActionDuplicate || a == trigger.ActionResolved }

// ShadowReport compares mirrored v1 trigger events with the v2 routing over
// [since, until] (at most limit events, newest first). In shadow mode v2
// does not correlate, so "would create" vs "would attach" is reconstructed
// from the rule's group key and window, exactly as live correlation would.
func (in *Ingestor) ShadowReport(ctx context.Context, tenantID string, since, until time.Time, limit int) (ShadowReport, error) {
	recs, err := in.Store.ListEvents(ctx, cpsql.EventFilter{TenantID: tenantID, SourcePrefix: TriggerSourcePrefix,
		Since: since, Until: until, Limit: limit})
	if err != nil {
		return ShadowReport{}, err
	}
	sort.Slice(recs, func(i, j int) bool {
		if !recs[i].Envelope.OccurredAt.Equal(recs[j].Envelope.OccurredAt) {
			return recs[i].Envelope.OccurredAt.Before(recs[j].Envelope.OccurredAt)
		}
		return recs[i].InboxID < recs[j].InboxID
	})
	rep := ShadowReport{Since: since, Until: until, Matrix: map[string]int{}, ByTrigger: map[string][2]int{}}
	rules := map[string]eventing.Rule{}
	if in.Router != nil {
		for _, r := range in.Router.Rules() {
			rules[r.Name] = r
		}
	}
	lastSeen := map[string]time.Time{}
	for _, rec := range recs {
		env := rec.Envelope
		row := ShadowRow{InboxID: rec.InboxID, Trigger: env.Labels["trigger"], Subject: env.SubjectRef, At: env.OccurredAt,
			V1: env.Labels["v1_action"], V1RunID: env.Labels["v1_run_id"], V2: rec.Decision, WorkID: rec.WorkID}
		row.V2Would, row.Rule = in.wouldDo(rec, rules, lastSeen)
		row.Agrees, row.Reason = agree(row.V1, row.V2Would)
		rep.Events++
		bt := rep.ByTrigger[row.Trigger]
		bt[0]++
		if row.Agrees {
			rep.Agreements++
			bt[1]++
		} else if len(rep.Disagreements) < 200 {
			rep.Disagreements = append(rep.Disagreements, row)
		}
		rep.ByTrigger[row.Trigger] = bt
		rep.Matrix[row.V1+"→"+row.V2Would]++
	}
	if rep.Events > 0 {
		rep.AgreementRate = float64(rep.Agreements) / float64(rep.Events)
	}
	return rep, nil
}

func (in *Ingestor) wouldDo(rec eventing.Record, rules map[string]eventing.Rule, lastSeen map[string]time.Time) (string, string) {
	d := rec.Decision
	switch {
	case d == cpsql.CorrCreated:
		return "create", ""
	case d == cpsql.CorrAttached, d == cpsql.CorrThrottled:
		return "attach", ""
	case d == "watch":
		return "watch", ""
	case d == "ignored", d == "no_rule", d == "expired", d == cpsql.CorrNoWork:
		return "ignore", ""
	case strings.HasPrefix(d, "shadow:"):
		parts := strings.SplitN(d, ":", 3)
		if len(parts) != 3 {
			return "none", ""
		}
		action, name := parts[1], parts[2]
		if action == "attach" {
			return "attach", name
		}
		r, ok := rules[name]
		if !ok {
			return "create", name
		}
		env := rec.Envelope
		group := r.GroupKey(&env)
		window := r.Window.D()
		if window <= 0 {
			window = eventing.DefaultWindow
		}
		prev, seen := lastSeen[group]
		lastSeen[group] = env.OccurredAt
		if seen && env.OccurredAt.Sub(prev) <= window {
			return "attach", name
		}
		return "create", name
	}
	return "none", ""
}

func agree(v1, v2 string) (bool, string) {
	switch {
	case v1Creates(v1) && v2 == "create", v1Attaches(v1) && v2 == "attach":
		return true, ""
	case (v1 == trigger.ActionIgnored || v1 == trigger.ActionRateLimited) && (v2 == "ignore" || v2 == "attach"):
		return true, ""
	case v1 == trigger.ActionError:
		return false, "v1 failed to handle the event"
	case v1Creates(v1):
		return false, fmt.Sprintf("v1 started a run, v2 would %s", v2)
	case v1Attaches(v1) && v2 == "create":
		return false, "v1 deduplicated the event, v2 would open a new Work"
	}
	return false, fmt.Sprintf("v1 %s, v2 would %s", v1, v2)
}
