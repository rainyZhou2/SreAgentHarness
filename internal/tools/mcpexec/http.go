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

func headerClient(headers map[string]string) *http.Client {
	if len(headers) == 0 {
		return http.DefaultClient
	}
	return &http.Client{Transport: headerRT{base: http.DefaultTransport, headers: headers}}
}
