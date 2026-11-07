package localstore

import (
	"context"

	"go-sre-harness/internal/extensions"
)

func extensionKey(tenantID string, kind extensions.Kind, name string) string {
	return tenantID + "/" + string(kind) + "/" + name
}

func (s *Store) ListExtensions(ctx context.Context, tenantID string) ([]extensions.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []extensions.Record
	for _, r := range s.extensions {
		if tenantID == "" || r.TenantID == tenantID {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *Store) UpsertExtension(ctx context.Context, r extensions.Record, expected *int64) (extensions.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := extensionKey(r.TenantID, r.Kind, r.Name)
	prev, exists := s.extensions[key]
	if expected != nil {
		if (*expected == 0 && exists) || (*expected > 0 && (!exists || prev.Version != *expected)) {
			return extensions.Record{}, extensions.ErrConflict
		}
	}
	r.Version = prev.Version + 1
	r.UpdatedAt = now()
	s.extensions[key] = r
	s.put("extension", key, r.TenantID, r.Version, map[string]any{"tenant_id": r.TenantID, "record": r})
	return r, nil
}

func (s *Store) DeleteExtension(ctx context.Context, tenantID string, kind extensions.Kind, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := extensionKey(tenantID, kind, name)
	if _, ok := s.extensions[key]; !ok {
		return extensions.ErrNotFound
	}
	delete(s.extensions, key)
	s.del("extension", key)
	return nil
}

// DisabledTools implements the extensions API's ToolToggles.
func (s *Store) DisabledTools(ctx context.Context, tenantID string) (map[string]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]bool{}
	for name, off := range s.toolOff[tenantID] {
		if off {
			out[name] = true
		}
	}
	return out, nil
}

// SetToolEnabled implements the extensions API's ToolToggles.
func (s *Store) SetToolEnabled(ctx context.Context, tenantID, name string, enabled bool, actor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.toolOff[tenantID] == nil {
		s.toolOff[tenantID] = map[string]bool{}
	}
	s.toolOff[tenantID][name] = !enabled
	s.put("tooltoggle", tenantID+"/"+name, tenantID, 0, map[string]any{
		"tenant_id": tenantID, "name": name, "enabled": enabled, "actor": actor})
	return nil
}

var _ extensions.Store = (*Store)(nil)
