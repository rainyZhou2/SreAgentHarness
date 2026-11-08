package planner

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"
)

// PlanCache stores the raw submit_flow arguments of validated plans. A hit
// is re-validated against the current tools, rules and budget before use, so
// a cache can only save a model call, never admit an invalid plan.
type PlanCache interface {
	Get(key string) (CachedPlan, bool)
	Put(key string, p CachedPlan)
}

// CachedPlan is what the cache keeps for one planning input.
type CachedPlan struct {
	Args           json.RawMessage `json:"args"`
	ModelRequestID string          `json:"model_request_id,omitempty"`
	ModelAlias     string          `json:"model_alias,omitempty"`
	ManifestRef    string          `json:"manifest_ref,omitempty"`
	ResponseRef    string          `json:"response_ref,omitempty"`
}

// MemoryCache is an in-process LRU with a TTL.
type MemoryCache struct {
	TTL time.Duration
	Max int

	mu    sync.Mutex
	ll    *list.List
	items map[string]*list.Element
	now   func() time.Time
}

type cacheEntry struct {
	key  string
	plan CachedPlan
	at   time.Time
}

// NewMemoryCache returns a cache (ttl <= 0 or max <= 0 use 10m / 512).
func NewMemoryCache(ttl time.Duration, max int) *MemoryCache {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	if max <= 0 {
		max = 512
	}
	return &MemoryCache{TTL: ttl, Max: max, ll: list.New(), items: map[string]*list.Element{}, now: time.Now}
}

func (c *MemoryCache) Get(key string) (CachedPlan, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return CachedPlan{}, false
	}
	e := el.Value.(*cacheEntry)
	if c.now().Sub(e.at) > c.TTL {
		c.ll.Remove(el)
		delete(c.items, key)
		return CachedPlan{}, false
	}
	c.ll.MoveToFront(el)
	return e.plan, true
}

func (c *MemoryCache) Put(key string, p CachedPlan) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		el.Value = &cacheEntry{key: key, plan: p, at: c.now()}
		c.ll.MoveToFront(el)
		return
	}
	c.items[key] = c.ll.PushFront(&cacheEntry{key: key, plan: p, at: c.now()})
	for c.ll.Len() > c.Max {
		last := c.ll.Back()
		c.ll.Remove(last)
		delete(c.items, last.Value.(*cacheEntry).key)
	}
}

// cacheKey hashes every input that can change the plan. The tenant is part
// of the key, so tenants never share plans.
func cacheKey(tenantID string, in Input, cfg Config, maxNodes int, budget Budget) string {
	toolSig := make([]string, 0, len(in.Tools))
	for _, d := range in.Tools {
		toolSig = append(toolSig, d.Name+"|"+string(d.Risk)+"|"+d.Trust+"|"+d.Description)
	}
	sort.Strings(toolSig)
	exemplars := make([]string, 0, len(in.Exemplars))
	for _, e := range in.Exemplars {
		exemplars = append(exemplars, e.MemoryID)
	}
	payload := struct {
		Prompt     string
		Tenant     string
		Goal       string
		History    []Message
		Caps       []string
		Tools      []string
		Exemplars  []string
		Services   []string
		Hint       string
		MaxSteps   int
		MaxNodes   int
		Budget     Budget
		Ladder     []string
		Alias      string
		Approval   string
		OutputCeil int64
	}{systemPrompt, tenantID, strings.TrimSpace(in.Goal), in.History, in.Capabilities, toolSig, exemplars,
		in.Services, in.ModeHint, in.MaxSteps, maxNodes, budget, cfg.ModelLadder, cfg.ModelAlias, cfg.ApprovalMode,
		cfg.MaxOutputTokens}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
