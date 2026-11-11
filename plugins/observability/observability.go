// Package observability provides the read-only source connectors of the
// Query Broker: Prometheus-compatible metrics (Prometheus, VictoriaMetrics,
// Mimir), Loki logs, and Tempo and Jaeger traces. Importing the package
// registers the backends; the broker builds one provider per catalog
// source from the operator's endpoint and credential records.
//
// A connector only reads. It receives an expression the broker already
// authorized and rendered, the time window and a limit, and returns the raw
// body plus a bounded summary. Redaction, storage, caching, budgets and
// audit are the broker's job, so every backend gets them the same way.
package observability

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go-sre-harness/internal/capability"
	obsv "go-sre-harness/internal/observability"
)

func init() {
	for _, name := range []string{"prometheus", "victoriametrics", "mimir"} {
		obsv.RegisterBackend(name, newMetrics)
	}
	obsv.RegisterBackend("loki", newLoki)
	obsv.RegisterBackend("tempo", newTempo)
	obsv.RegisterBackend("jaeger", newJaeger)
}

// base is the HTTP plumbing shared by the connectors.
type base struct {
	src   obsv.Source
	ep    obsv.Endpoint
	creds obsv.CredentialBroker
	c     *capability.Client
}

func newBase(src obsv.Source, ep obsv.Endpoint, creds obsv.CredentialBroker) (base, error) {
	headers := map[string]string{}
	for k, v := range ep.Headers {
		headers[k] = v
	}
	if ep.OrgID != "" {
		headers["X-Scope-OrgID"] = ep.OrgID
	}
	c, err := capability.NewClient(capability.HTTPConfig{URL: ep.URL, CAFile: ep.CAFile, Headers: headers,
		Timeout: capability.Duration(src.QueryCaps.Timeout.D()), MaxBytes: src.QueryCaps.MaxDataBytes + 1<<20})
	if err != nil {
		return base{}, fmt.Errorf("endpoint %s: %w", src.EndpointRef, err)
	}
	return base{src: src, ep: ep, creds: creds, c: c}, nil
}

func (b base) ID() string          { return b.src.ID }
func (b base) Signal() obsv.Signal { return b.src.Kind }

// get performs an authenticated GET. The credential is resolved per call
// and only ever placed in the Authorization header.
func (b base) get(ctx context.Context, path string, q url.Values, out any) error {
	req := capability.Request{Method: http.MethodGet, Path: path, Query: q}
	if b.src.CredentialID != "" {
		if b.creds == nil {
			return fmt.Errorf("%w: no credential broker is configured", obsv.ErrUnauthorized)
		}
		sec, err := b.creds.Resolve(ctx, b.src.TenantID, b.src.CredentialID)
		if err != nil {
			return fmt.Errorf("%w: %v", obsv.ErrUnauthorized, err)
		}
		req.Header = map[string]string{"Authorization": sec.Scheme + " " + sec.Token}
	}
	return classify(b.c.Do(ctx, req, out))
}

// classify maps HTTP failures onto the broker's error classes.
func classify(err error) error {
	var se *capability.StatusError
	if !errors.As(err, &se) {
		return err
	}
	switch {
	case se.Code == http.StatusUnauthorized || se.Code == http.StatusForbidden:
		return fmt.Errorf("%w: HTTP %d", obsv.ErrUnauthorized, se.Code)
	case se.Code == http.StatusTooManyRequests:
		return &obsv.RetryAfterError{After: time.Second, Err: fmt.Errorf("backend is rate limiting (HTTP 429)")}
	case se.Code == http.StatusBadRequest || se.Code == http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: HTTP %d: %s", obsv.ErrBadQuery, se.Code, se.Body)
	}
	return fmt.Errorf("backend returned HTTP %d", se.Code)
}

func unix(t time.Time) string     { return strconv.FormatInt(t.Unix(), 10) }
func unixNano(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

func fnum(v float64) string { return strconv.FormatFloat(v, 'g', 6, 64) }
