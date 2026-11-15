package retention

import (
	"strings"
	"time"
)

type ArtifactReference struct {
	TenantID        string
	ArtifactRef     string
	ReferrerType    string
	ReferrerID      string
	ReferrerVersion int64
}

type ArtifactHold struct {
	TenantID    string
	ArtifactRef string
	HoldID      string
	Reason      string
	Actor       string
	ExpiresAt   *time.Time
}

func ValidArtifactRef(tenantID, ref string) bool {
	scheme, key, ok := strings.Cut(ref, "://")
	return ok && scheme != "" && tenantID != "" && strings.HasPrefix(key, tenantID+"/") &&
		len(key) > len(tenantID)+1 && !strings.Contains(key, "..")
}

type Hold struct {
	ExpiresAt  *time.Time
	ReleasedAt *time.Time
}

type RetentionInput struct {
	Now         time.Time
	References  int
	RetainUntil []time.Time
	Holds       []Hold
}

func CanDeleteArtifact(in RetentionInput) bool {
	if in.Now.IsZero() || in.References < 0 || in.References > 0 {
		return false
	}
	for _, until := range in.RetainUntil {
		if until.IsZero() || until.After(in.Now) {
			return false
		}
	}
	for _, hold := range in.Holds {
		released := hold.ReleasedAt != nil && !hold.ReleasedAt.After(in.Now)
		if !released && (hold.ExpiresAt == nil || hold.ExpiresAt.After(in.Now)) {
			return false
		}
	}
	return true
}
