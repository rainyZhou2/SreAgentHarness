package api

import "net/http"

// listProfiles returns the tenant's registered profile versions for the
// client's mode picker. The server's configured default is marked.
func (s *Server) listProfiles(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	profiles, err := s.Store.ListProfiles(r.Context(), id.tenant)
	if err != nil {
		s.Log.Error("list profiles", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal", "cannot list profiles")
		return
	}
	for i := range profiles {
		profiles[i].Default = profiles[i].ID == s.DefaultProfile && profiles[i].Version == s.DefaultVersion
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"profiles":        profiles,
		"default_profile": s.DefaultProfile,
		"default_version": s.DefaultVersion,
	})
}
