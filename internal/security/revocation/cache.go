package revocation

import (
	"context"
	"sync"
	"time"

	"go-sre-harness/internal/security"
)

const cacheTTL = 5 * time.Second

type cacheEntry struct {
	query     Query
	stamps    []security.RevocationStamp
	expiresAt time.Time
}

type Cache struct {
	store Store
	now   func() time.Time
	mu    sync.RWMutex
	items map[string]cacheEntry
}

func NewCache(store Store, now func() time.Time) *Cache {
	if now == nil {
		now = time.Now
	}
	return &Cache{store: store, now: now, items: map[string]cacheEntry{}}
}

func (c *Cache) Stamps(ctx context.Context, query Query) ([]security.RevocationStamp, error) {
	key := queryKey(query)
	now := c.now()
	c.mu.RLock()
	entry, ok := c.items[key]
	valid := ok && now.Before(entry.expiresAt)
	c.mu.RUnlock()
	if valid {
		return cloneStamps(entry.stamps), nil
	}
	stamps, err := c.store.Stamps(ctx, query)
	if err != nil {
		return nil, err
	}
	entry = cacheEntry{query: query, stamps: cloneStamps(stamps), expiresAt: now.Add(cacheTTL)}
	c.mu.Lock()
	c.items[key] = entry
	c.mu.Unlock()
	return cloneStamps(stamps), nil
}

func (c *Cache) Invalidate(tenantID, scopeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, entry := range c.items {
		query := entry.query
		if query.TenantID == tenantID && (scopeID == tenantID || query.Subject == scopeID || query.WorkID == scopeID || query.RunID == scopeID) {
			delete(c.items, key)
		}
	}
}

func queryKey(query Query) string {
	return query.TenantID + "\x00" + query.Subject + "\x00" + query.WorkID + "\x00" + query.RunID
}

func cloneStamps(in []security.RevocationStamp) []security.RevocationStamp {
	return append([]security.RevocationStamp(nil), in...)
}
