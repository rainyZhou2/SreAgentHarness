package api

import (
	"net/http"
	"strings"
)

type serviceMetadata struct {
	Name           string `json:"name"`
	ReleaseVersion string `json:"release_version"`
	BuildRevision  string `json:"build_revision"`
	APIPrefix      string `json:"api_prefix"`
}

func (s *Server) service(w http.ResponseWriter, _ *http.Request) {
	release := strings.TrimSpace(s.ReleaseVersion)
	if release == "" {
		release = "dev"
	}
	revision := strings.TrimSpace(s.BuildRevision)
	if revision == "" {
		revision = "unknown"
	}
	writeJSON(w, http.StatusOK, serviceMetadata{
		Name:           "sre-agent-harness",
		ReleaseVersion: release,
		BuildRevision:  revision,
		APIPrefix:      "/api",
	})
}
