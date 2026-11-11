package impact

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

// Tool is the read-only agent tool sre.change_correlation. The tenant comes
// from the kernel's call info, never from an argument.
func Tool(store Store, topo Topology, clock cpkit.Clock) tools.Tool {
	return &tools.Func{Desc: tools.Descriptor{Name: "sre.change_correlation", Version: "1", Risk: domain.RiskR0, Trust: "T2",
		Source: "native", TargetArg: "subject",
		Description: "Changes (releases, config, topology, manual operations) around a symptom on a resource and its dependencies, " +
			"ranked as leads with the checks that would confirm or dismiss each. A lead is not a root cause.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"subject":{"type":"string"},"started_at":{"type":"string",` +
			`"description":"RFC 3339 onset of the symptom (default: now)"},"window":{"type":"string","description":"how far back, e.g. 2h"}},` +
			`"required":["subject"],"additionalProperties":false}`)},
		Fn: func(ctx context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
			ci, ok := tools.CallInfoFrom(ctx)
			if !ok || ci.TenantID == "" {
				return nil, fmt.Errorf("sre.change_correlation needs an authenticated run context")
			}
			var a struct {
				Subject   string         `json:"subject"`
				StartedAt time.Time      `json:"started_at"`
				Window    cpkit.Duration `json:"window"`
			}
			if err := json.Unmarshal(raw, &a); err != nil {
				return nil, fmt.Errorf("invalid arguments: %w", err)
			}
			leads, err := Leads(ctx, store, topo, ci.TenantID, Symptom{Subject: a.Subject, StartedAt: a.StartedAt, Window: a.Window.D()}, clock.Now())
			if err != nil {
				return nil, err
			}
			if len(leads) > 10 {
				leads = leads[:10]
			}
			return json.Marshal(map[string]any{"leads": leads, "relation": "correlates",
				"note": "Time order and proximity only. Confirm a lead with its distinguishing checks before calling it a cause."})
		}}
}
