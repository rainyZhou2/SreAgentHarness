package localstore

import (
	"context"
	"encoding/json"
	"fmt"

	"go-sre-harness/internal/domain"
)

func (s *Store) RegisterProfile(ctx context.Context, tenantID, profileID string, version int64, spec json.RawMessage) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := profileKey(tenantID, profileID, version)
	if _, exists := s.profiles[key]; exists {
		return false, nil
	}
	var parsed struct {
		Strategy string `json:"strategy"`
	}
	_ = json.Unmarshal(spec, &parsed)
	p := profileRec{TenantID: tenantID, ID: profileID, Version: version, Status: "ACTIVE",
		Strategy: parsed.Strategy, Spec: append(json.RawMessage(nil), spec...)}
	s.profiles[key] = p
	s.put("profile", key, tenantID, version, p)
	return true, nil
}

func (s *Store) ProfileSpec(ctx context.Context, tenantID, profileID string, version int64) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.profileSpec(tenantID, profileID, version)
}

func (s *Store) ListProfiles(ctx context.Context, tenantID string) ([]domain.ProfileInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.ProfileInfo
	// The default is the highest version of sre-diagnosis, matching the server.
	var defaultVersion int64
	for _, p := range s.profiles {
		if p.TenantID == tenantID && p.ID == "sre-diagnosis" && p.Version > defaultVersion {
			defaultVersion = p.Version
		}
	}
	for _, p := range s.sortedProfiles(tenantID) {
		out = append(out, domain.ProfileInfo{ID: p.ID, Version: p.Version, Status: p.Status,
			Strategy: p.Strategy, Default: p.ID == "sre-diagnosis" && p.Version == defaultVersion})
	}
	return out, nil
}

func (s *Store) CreateProfileVersion(ctx context.Context, tenantID, profileID string, spec json.RawMessage, status string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var version int64
	for key, p := range s.profiles {
		if p.TenantID == tenantID && p.ID == profileID && p.Version > version {
			version = p.Version
			_ = key
		}
	}
	version++
	var parsed struct {
		Strategy string `json:"strategy"`
	}
	_ = json.Unmarshal(spec, &parsed)
	p := profileRec{TenantID: tenantID, ID: profileID, Version: version, Status: status,
		Strategy: parsed.Strategy, Spec: append(json.RawMessage(nil), spec...)}
	key := profileKey(tenantID, profileID, version)
	s.profiles[key] = p
	s.put("profile", key, tenantID, version, p)
	return version, nil
}

func (s *Store) SetProfileStatus(ctx context.Context, tenantID, profileID string, version int64, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := profileKey(tenantID, profileID, version)
	p, ok := s.profiles[key]
	if !ok {
		return domain.ErrNotFound
	}
	p.Status = status
	s.profiles[key] = p
	s.put("profile", key, tenantID, version, p)
	return nil
}

func (s *Store) SetActiveVersion(ctx context.Context, tenantID, profileID string, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	for key, p := range s.profiles {
		if p.TenantID == tenantID && p.ID == profileID {
			p.Default = p.Version == version
			if p.Default {
				found = true
			}
			s.profiles[key] = p
			s.put("profile", key, tenantID, p.Version, p)
		}
	}
	if !found {
		return fmt.Errorf("%w: profile version", domain.ErrNotFound)
	}
	return nil
}

func (s *Store) ActiveVersion(ctx context.Context, tenantID, profileID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best int64
	for _, p := range s.profiles {
		if p.TenantID == tenantID && p.ID == profileID && p.Default && p.Version > best {
			best = p.Version
		}
	}
	if best == 0 {
		return 0, domain.ErrNotFound
	}
	return best, nil
}

func (s *Store) ListProfileVersions(ctx context.Context, tenantID, profileID string) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []int64
	for _, p := range s.profiles {
		if p.TenantID == tenantID && p.ID == profileID {
			out = append(out, p.Version)
		}
	}
	return out, nil
}
