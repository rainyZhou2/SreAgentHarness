package workclient

import (
	"context"
	"net/http"
)

// Attestation returns the deployment's launch attestation.
func (c *Client) Attestation(ctx context.Context) (Attestation, error) {
	var a Attestation
	_, err := c.do(ctx, http.MethodGet, "/api/governance/attestation", nil, nil, &a)
	return a, err
}

// Quotas returns the tenant's cluster-wide budgets.
func (c *Client) Quotas(ctx context.Context) ([]Budget, error) {
	var out struct {
		Budgets []Budget `json:"budgets"`
	}
	_, err := c.do(ctx, http.MethodGet, "/api/quotas", nil, nil, &out)
	return out.Budgets, err
}

// RouteIntent classifies a request into a Work kind, template and ceiling.
func (c *Client) RouteIntent(ctx context.Context, text string) (Intent, error) {
	var i Intent
	_, err := c.do(ctx, http.MethodPost, "/api/work-intents", nil, map[string]string{"text": text}, &i)
	return i, err
}

// DryRun simulates a template for a goal without side effects.
func (c *Client) DryRun(ctx context.Context, templateID string, goal Goal) (DryRunPlan, error) {
	var p DryRunPlan
	_, err := c.do(ctx, http.MethodPost, "/api/work-templates/"+esc(templateID)+"/dry-run", nil, map[string]any{"goal": goal}, &p)
	return p, err
}

// Works is a resource-style facade over the Work calls:
//
//	client := workclient.New(baseURL, token)
//	w, err := client.Works().Create(ctx, "incident-20261110-search-p99", workclient.CreateWork{...})
//	events, errs := client.Works().Subscribe(ctx, w.ID, w.LastSeq)
type Works struct{ c *Client }

// Works returns the Work facade.
func (c *Client) Works() Works { return Works{c} }

func (w Works) Create(ctx context.Context, key string, req CreateWork) (Work, error) {
	return w.c.CreateWork(ctx, key, req)
}
func (w Works) Get(ctx context.Context, id string) (Work, error)  { return w.c.GetWork(ctx, id) }
func (w Works) View(ctx context.Context, id string) (View, error) { return w.c.View(ctx, id) }
func (w Works) Subscribe(ctx context.Context, id string, afterSeq int64) (<-chan Event, <-chan error) {
	return w.c.Subscribe(ctx, id, afterSeq)
}
