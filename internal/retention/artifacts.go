package retention

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-sre-harness/internal/artifact"
)

type ArtifactClaim struct {
	TenantID   string
	Ref        string
	LeaseToken int64
}

type ArtifactLeaseStore interface {
	ClaimArtifacts(context.Context, time.Time, int, time.Duration) ([]ArtifactClaim, error)
	ConfirmArtifactDeleted(context.Context, ArtifactClaim) error
	RetryArtifact(context.Context, ArtifactClaim, time.Time, string) error
}

type ArtifactCleaner struct {
	Store   ArtifactLeaseStore
	Objects artifact.Deleter
	Batch   int
	Lease   time.Duration
	Backoff time.Duration
	Now     func() time.Time
}

type ArtifactCleanupResult struct {
	Claimed   int
	Deleted   int
	Confirmed int
}

func (c *ArtifactCleaner) Tick(ctx context.Context) error {
	_, err := c.Clean(ctx)
	return err
}

func (c *ArtifactCleaner) Clean(ctx context.Context) (ArtifactCleanupResult, error) {
	var result ArtifactCleanupResult
	if c.Store == nil || c.Objects == nil {
		return result, errors.New("artifact cleaner is not configured")
	}
	batch := c.Batch
	if batch <= 0 {
		batch = 100
	}
	lease := c.Lease
	if lease <= 0 {
		lease = 5 * time.Minute
	}
	backoff := c.Backoff
	if backoff <= 0 {
		backoff = time.Minute
	}
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	at := now().UTC()
	claims, err := c.Store.ClaimArtifacts(ctx, at, batch, lease)
	if err != nil {
		return result, err
	}
	result.Claimed = len(claims)
	var failures []error
	for _, claim := range claims {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(append(failures, err)...)
		}
		if err := c.Objects.Delete(ctx, claim.TenantID, claim.Ref); err != nil {
			retryAt := at.Add(backoff)
			if retryErr := c.Store.RetryArtifact(ctx, claim, retryAt, err.Error()); retryErr != nil {
				failures = append(failures, fmt.Errorf("artifact %s delete: %w; retry: %v", claim.Ref, err, retryErr))
			} else {
				failures = append(failures, fmt.Errorf("artifact %s delete: %w", claim.Ref, err))
			}
			continue
		}
		result.Deleted++
		if err := c.Store.ConfirmArtifactDeleted(ctx, claim); err != nil {
			failures = append(failures, fmt.Errorf("artifact %s confirm: %w", claim.Ref, err))
			continue
		}
		result.Confirmed++
	}
	return result, errors.Join(failures...)
}
