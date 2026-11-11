package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/work"
)

// QueueLimit bounds one scheduling queue (RFC §20.2).
type QueueLimit struct {
	// Concurrency is how many Works of the queue one replica processes at
	// once; Batch how many one claim leases.
	Concurrency int `json:"concurrency,omitempty"`
	Batch       int `json:"batch,omitempty"`
	// ModelRunsPerTenant caps the active model runs a tenant's Works of
	// this queue may have (cluster-wide; 0 = only the pool total applies).
	ModelRunsPerTenant int `json:"model_runs_per_tenant,omitempty"`
	// AdmissionDeadline is the target delay from a Work being due to its
	// processing. A Work claimed later is marked queued_long (timeline event
	// and metric); it is never dropped.
	AdmissionDeadline cpkit.Duration `json:"admission_deadline,omitempty"`
	// MaxBacklog sheds load: new Works are refused (429) while this many
	// Works of the queue are due and unclaimed. It applies to batch and
	// background queues only; incident queues and the event inbox never shed.
	MaxBacklog int `json:"max_backlog,omitempty"`
}

// DefaultQueueLimits gives mitigation the most room and keeps evaluation
// and background improvement from crowding out incidents.
func DefaultQueueLimits() map[work.Queue]QueueLimit {
	return map[work.Queue]QueueLimit{
		work.QueueCritical:   {Concurrency: 8, Batch: 16, AdmissionDeadline: cpkit.Duration(3 * time.Second)},
		work.QueueIncident:   {Concurrency: 4, Batch: 16, AdmissionDeadline: cpkit.Duration(10 * time.Second)},
		work.QueueRoutine:    {Concurrency: 2, Batch: 8, ModelRunsPerTenant: 8, AdmissionDeadline: cpkit.Duration(time.Minute)},
		work.QueueBatch:      {Concurrency: 1, Batch: 4, ModelRunsPerTenant: 4, MaxBacklog: 2000},
		work.QueueBackground: {Concurrency: 1, Batch: 4, ModelRunsPerTenant: 2, MaxBacklog: 2000},
	}
}

// PoolLimits are the shared resource pools (RFC §20.2: models, production
// actions and world snapshots each get their own pool and tenant budget).
type PoolLimits struct {
	// ModelRunsPerTenant caps a tenant's active Work-started model runs
	// over all queues except critical-mitigation, which is bounded only by
	// its own queue limit so that mitigation never waits behind routine
	// work (cluster-wide, counted in the store; 0 = unlimited).
	ModelRunsPerTenant int `json:"model_runs_per_tenant,omitempty"`
	// CriticalReservedModelRuns is taken out of ModelRunsPerTenant for the
	// other queues, so that capacity stays free for mitigation even when
	// every other queue is at its budget.
	CriticalReservedModelRuns int `json:"critical_reserved_model_runs,omitempty"`
	// ReservationTTL is the lease of a model-run reservation (default 10m);
	// a running stage renews it, the reaper frees it after a crash.
	ReservationTTL cpkit.Duration `json:"reservation_ttl,omitempty"`
	// Actions and ActionsPerTenant cap concurrent production action
	// dispatches and reconciliations per replica (0 = unlimited).
	Actions          int `json:"actions,omitempty"`
	ActionsPerTenant int `json:"actions_per_tenant,omitempty"`
	// Snapshots caps concurrent world snapshot builds per replica.
	Snapshots int `json:"snapshots,omitempty"`
	// SnapshotWait is how long a snapshot waits for a slot (default 5s).
	SnapshotWait time.Duration `json:"-"`
}

// DefaultPoolLimits are conservative defaults; operators tune them.
func DefaultPoolLimits() PoolLimits {
	return PoolLimits{ModelRunsPerTenant: 32, CriticalReservedModelRuns: 4, Actions: 16, ActionsPerTenant: 4, Snapshots: 16}
}

// ValidateQueueLimits rejects unknown queues and negative limits.
func ValidateQueueLimits(m map[string]QueueLimit) (map[work.Queue]QueueLimit, error) {
	out := DefaultQueueLimits()
	for name, l := range m {
		q := work.Queue(name)
		if !work.ValidQueue(q) {
			return nil, fmt.Errorf("work.queues: unknown queue %q", name)
		}
		if l.Concurrency < 0 || l.Batch < 0 || l.ModelRunsPerTenant < 0 || l.Concurrency > 256 || l.Batch > 512 ||
			l.AdmissionDeadline < 0 || l.MaxBacklog < 0 {
			return nil, fmt.Errorf("work.queues.%s: limits out of range", name)
		}
		d := out[q]
		if l.Concurrency > 0 {
			d.Concurrency = l.Concurrency
		}
		if l.Batch > 0 {
			d.Batch = l.Batch
		}
		if l.ModelRunsPerTenant > 0 {
			d.ModelRunsPerTenant = l.ModelRunsPerTenant
		}
		if l.AdmissionDeadline > 0 {
			d.AdmissionDeadline = l.AdmissionDeadline
		}
		if l.MaxBacklog > 0 {
			if q == work.QueueCritical || q == work.QueueIncident {
				return nil, fmt.Errorf("work.queues.%s: incident queues never shed load (max_backlog)", name)
			}
			d.MaxBacklog = l.MaxBacklog
		}
		out[q] = d
	}
	return out, nil
}

// errDeferred postpones a Work quietly: a pool is full, nothing is wrong.
type errDeferred struct {
	pool  string
	after time.Duration
}

func (e *errDeferred) Error() string { return "deferred: " + e.pool + " pool is full" }

func deferred(pool string, after time.Duration) error { return &errDeferred{pool: pool, after: after} }

// ErrPoolTimeout is returned when no slot frees up in time.
var ErrPoolTimeout = errors.New("resource pool saturated")

// Limiter is a counting semaphore with an optional per-key cap (per
// tenant). It is per replica; cluster-wide budgets are counted in the store.
type Limiter struct {
	Name   string
	total  chan struct{}
	perKey int
	mu     sync.Mutex
	keys   map[string]int
}

// NewLimiter makes a limiter; total or perKey 0 means unlimited.
func NewLimiter(name string, total, perKey int) *Limiter {
	l := &Limiter{Name: name, perKey: perKey, keys: map[string]int{}}
	if total > 0 {
		l.total = make(chan struct{}, total)
	}
	return l
}

// TryAcquire takes a slot for key without waiting.
func (l *Limiter) TryAcquire(key string) (release func(), ok bool) {
	if l == nil {
		return func() {}, true
	}
	l.mu.Lock()
	if l.perKey > 0 && l.keys[key] >= l.perKey {
		l.mu.Unlock()
		return nil, false
	}
	if l.total != nil {
		select {
		case l.total <- struct{}{}:
		default:
			l.mu.Unlock()
			return nil, false
		}
	}
	l.keys[key]++
	l.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			if l.keys[key]--; l.keys[key] <= 0 {
				delete(l.keys, key)
			}
			l.mu.Unlock()
			if l.total != nil {
				<-l.total
			}
		})
	}, true
}

// Acquire waits for a slot until ctx ends or wait elapses.
func (l *Limiter) Acquire(ctx context.Context, key string, wait time.Duration) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	deadline := time.Now().Add(wait)
	for {
		if rel, ok := l.TryAcquire(key); ok {
			return rel, nil
		}
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("%w: %s", ErrPoolTimeout, l.Name)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// InFlight is the number of held slots.
func (l *Limiter) InFlight() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, c := range l.keys {
		n += c
	}
	return n
}

// Pools holds the per-replica limiters and the cluster-wide budgets.
type Pools struct {
	Limits    PoolLimits
	Actions   *Limiter
	Snapshots *Limiter
}

// NewPools builds the limiters from limits.
func NewPools(l PoolLimits) *Pools {
	if l.SnapshotWait <= 0 {
		l.SnapshotWait = 5 * time.Second
	}
	return &Pools{Limits: l, Actions: NewLimiter("actions", l.Actions, l.ActionsPerTenant),
		Snapshots: NewLimiter("snapshots", l.Snapshots, 0)}
}

// SnapshotGate adapts the snapshot limiter to world.Service.Gate.
func (p *Pools) SnapshotGate() func(ctx context.Context, tenantID string) (func(), error) {
	if p == nil || p.Snapshots == nil {
		return nil
	}
	return func(ctx context.Context, tenantID string) (func(), error) {
		return p.Snapshots.Acquire(ctx, tenantID, p.Limits.SnapshotWait)
	}
}

// actionSlot takes a production-action slot for a tenant (nil: unlimited).
func (p *Pools) actionSlot(tenantID string) (func(), bool) {
	if p == nil {
		return func() {}, true
	}
	return p.Actions.TryAcquire(tenantID)
}

// MergePoolLimits overlays the set fields of o on base; -1 means unlimited.
func MergePoolLimits(base PoolLimits, o *PoolLimits) PoolLimits {
	if o == nil {
		return base
	}
	pick := func(b, v int) int {
		switch {
		case v < 0:
			return 0
		case v > 0:
			return v
		}
		return b
	}
	base.ModelRunsPerTenant = pick(base.ModelRunsPerTenant, o.ModelRunsPerTenant)
	base.CriticalReservedModelRuns = pick(base.CriticalReservedModelRuns, o.CriticalReservedModelRuns)
	if o.ReservationTTL > 0 {
		base.ReservationTTL = o.ReservationTTL
	}
	base.Actions = pick(base.Actions, o.Actions)
	base.ActionsPerTenant = pick(base.ActionsPerTenant, o.ActionsPerTenant)
	base.Snapshots = pick(base.Snapshots, o.Snapshots)
	return base
}
