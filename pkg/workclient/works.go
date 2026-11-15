package workclient

import (
	"context"
	"net/http"
	"strconv"

	"go-sre-harness/internal/protocol"
)

// Graph returns the Work tree a Work belongs to.
func (c *Client) Graph(ctx context.Context, workID string) (Graph, error) {
	var g Graph
	_, err := c.do(ctx, http.MethodGet, "/api/work-graphs/"+esc(workID), nil, nil, &g)
	return g, err
}

// CreateChild creates a child Work inside its parent's ceiling.
func (c *Client) CreateChild(ctx context.Context, parentID, key string, req CreateChild) (Work, error) {
	var w Work
	_, err := c.do(ctx, http.MethodPost, "/api/works/"+esc(parentID)+"/children", map[string]string{"Idempotency-Key": key}, req, &w)
	return w, err
}

// LinkIncidentCause backfills an Incident cause at the expected Work version.
func (c *Client) LinkIncidentCause(ctx context.Context, workID, key string, version int64, req LinkCauseRequest) (Work, error) {
	var w Work
	_, err := c.do(ctx, http.MethodPost, "/api/works/"+esc(workID)+"/caused-by",
		map[string]string{"Idempotency-Key": key, "If-Match": strconv.FormatInt(version, 10)}, req, &w)
	return w, err
}

// View returns the unified lifecycle view of a Work.
func (c *Client) View(ctx context.Context, workID string) (View, error) {
	var v View
	_, err := c.do(ctx, http.MethodGet, "/api/works/"+esc(workID)+"/view", nil, nil, &v)
	if err != nil {
		return View{}, err
	}
	if v.Work.ID != workID {
		return View{}, &protocol.Error{Code: protocol.InvalidFrame, Message: "snapshot belongs to another Work"}
	}
	return v, nil
}

// Findings lists the validated agent findings of a Work.
func (c *Client) Findings(ctx context.Context, workID string) ([]Finding, error) {
	var out struct {
		Items []Finding `json:"items"`
	}
	_, err := c.do(ctx, http.MethodGet, "/api/works/"+esc(workID)+"/findings", nil, nil, &out)
	return out.Items, err
}

// Improvements opens Improvement Works for a closed Work (items empty: the
// follow-ups of its postmortem report).
func (c *Client) Improvements(ctx context.Context, workID string, items []Improvement) ([]Work, error) {
	var out struct {
		Items []Work `json:"items"`
	}
	_, err := c.do(ctx, http.MethodPost, "/api/works/"+esc(workID)+"/improvements", nil, map[string]any{"items": items}, &out)
	return out.Items, err
}
