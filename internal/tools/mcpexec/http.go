package mcpexec

import "net/http"

type headerRT struct {
	base    http.RoundTripper
	headers map[string]string
}

func (h headerRT) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	for k, v := range h.headers {
		r.Header.Set(k, v)
	}
	return h.base.RoundTrip(r)
}

func headerClient(base http.RoundTripper, headers map[string]string) *http.Client {
	if base == nil {
		base = http.DefaultTransport
	}
	if len(headers) == 0 {
		return &http.Client{Transport: base}
	}
	return &http.Client{Transport: headerRT{base: base, headers: headers}}
}
