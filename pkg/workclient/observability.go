package workclient

import (
	"context"
	"net/http"
	"strconv"
)

// Sources lists the observability Source Catalog.
func (c *Client) Sources(ctx context.Context) ([]Source, error) {
	var out struct {
		Items []Source `json:"items"`
	}
	_, err := c.do(ctx, http.MethodGet, "/api/observability/sources", nil, nil, &out)
	return out.Items, err
}

// PutSource creates (version 0) or updates a source at the given version.
func (c *Client) PutSource(ctx context.Context, s Source, version int64, reason string) (Source, error) {
	hdr := map[string]string{}
	if version > 0 {
		hdr["If-Match"] = strconv.FormatInt(version, 10)
	}
	body := struct {
		Source
		Reason string `json:"reason,omitempty"`
	}{s, reason}
	var out Source
	_, err := c.do(ctx, http.MethodPost, "/api/observability/sources", hdr, body, &out)
	return out, err
}

// ProbeSource checks one source's reachability and authorization.
func (c *Client) ProbeSource(ctx context.Context, id string) (SourceProbe, error) {
	var h SourceProbe
	_, err := c.do(ctx, http.MethodPost, "/api/observability/sources/"+esc(id)+"/probe", nil, struct{}{}, &h)
	return h, err
}

// Observe runs one authorized read-only query; workID ties it to a Work
// (scope, budget and evidence).
func (c *Client) Observe(ctx context.Context, workID string, q Query) (QueryResult, string, error) {
	body := struct {
		Query
		WorkID string `json:"work_id,omitempty"`
	}{q, workID}
	var out struct {
		Result      QueryResult `json:"result"`
		EvidenceRef string      `json:"evidence_ref"`
	}
	_, err := c.do(ctx, http.MethodPost, "/api/observability/query", nil, body, &out)
	return out.Result, out.EvidenceRef, err
}
