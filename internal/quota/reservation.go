// Package quota is the cluster-wide admission ledger of the control plane
// (design §6.2). A process-local limiter bounds one replica; a budget
// bounds all of them: units are reserved in the transaction that locks the
// budget row, so replicas can never admit more than the capacity together.
//
// A reservation is a lease:
//
//   - Reserve is idempotent on DedupKey: a retry of the same step returns
//     the reservation it already holds and takes no further units;
//   - Renew and Release are a fenced CAS on (id, token): a late call from a
//     previous owner cannot extend or free a newer owner's units;
//   - an owner that crashes between Reserve and the work it admitted leaves
//     a reservation that expires; Reap returns its units, unless InUse says
//     the admitted work really exists (then it is renewed, never freed).
//
// The package holds no SQL: internal/store/cpsql implements Store on
// PostgreSQL (row locks) and SQLite (single writer, local mode only).
package quota

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Resources a budget may bound.
const (
	ResourceModelRun       = "model_run"
	ResourceToolQuery      = "tool_query"
	ResourceActionDispatch = "action_dispatch"
	ResourceGPUQuery       = "gpu_query"
)

// AnyQueue is the queue of a tenant-wide budget (all queues together).
const AnyQueue = "*"

// ValidResource reports whether r is a known resource.
func ValidResource(r string) bool {
	switch r {
	case ResourceModelRun, ResourceToolQuery, ResourceActionDispatch, ResourceGPUQuery:
		return true
	}
	return false
}

// LeaseRequest asks for units of one budget.
type LeaseRequest struct {
	TenantID string
	WorkID   string
	Queue    string
	Resource string // model_run|tool_query|action_dispatch|gpu_query
	Units    int64
	// DedupKey identifies the admitted work (e.g. the run idempotency key);
	// the same key never holds units twice.
	DedupKey string
	TTL      time.Duration
	// Owner is the replica asking (diagnostics; the fence is the token).
	Owner string
	// Capacity is the budget capacity to enforce. Budgets are operator
	// configuration: the store creates the row on first use and follows a
	// changed capacity; a capacity below what is reserved admits nothing
	// new and frees nothing early.
	Capacity int64
	// Oversize admits a request larger than the whole capacity when nothing
	// else is reserved, so a stage wider than its cap cannot starve forever.
	Oversize bool
	Now      time.Time
}

// Validate checks a request.
func (r LeaseRequest) Validate() error {
	switch {
	case r.TenantID == "" || r.WorkID == "" || r.Queue == "":
		return fmt.Errorf("%w: tenant, work and queue are required", ErrInvalid)
	case !ValidResource(r.Resource):
		return fmt.Errorf("%w: unknown resource %q", ErrInvalid, r.Resource)
	case r.Units <= 0:
		return fmt.Errorf("%w: units must be positive", ErrInvalid)
	case r.DedupKey == "" || len(r.DedupKey) > 300:
		return fmt.Errorf("%w: dedup key is required (max 300 chars)", ErrInvalid)
	case r.TTL <= 0:
		return fmt.Errorf("%w: ttl must be positive", ErrInvalid)
	case r.Capacity < 0:
		return fmt.Errorf("%w: capacity must not be negative", ErrInvalid)
	}
	return nil
}

// Reservation is a held lease.
type Reservation struct {
	ID        string    `json:"id"`
	Token     int64     `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
	Owner     string    `json:"owner"`
	Units     int64     `json:"units"`
	// The fields below locate the reservation for release and diagnostics.
	TenantID string `json:"tenant_id"`
	WorkID   string `json:"work_id"`
	Queue    string `json:"queue"`
	Resource string `json:"resource"`
	DedupKey string `json:"dedup_key"`
	// Existing is true when Reserve returned a reservation it already held.
	Existing bool `json:"existing,omitempty"`
}

// Budget is the state of one budget row.
type Budget struct {
	TenantID string `json:"tenant_id"`
	Queue    string `json:"queue"`
	Resource string `json:"resource"`
	Capacity int64  `json:"capacity"`
	Reserved int64  `json:"reserved"`
	Version  int64  `json:"version"`
}

// Free is the unreserved capacity (never negative).
func (b Budget) Free() int64 {
	if f := b.Capacity - b.Reserved; f > 0 {
		return f
	}
	return 0
}

// Errors.
var (
	ErrInvalid = errors.New("invalid quota request")
	// ErrExhausted: the budget has no room for the request.
	ErrExhausted = errors.New("admission budget exhausted")
	// ErrFenced: the reservation is gone or belongs to a newer owner.
	ErrFenced = errors.New("reservation is not held with this token")
)

// ExhaustedError carries the budget state of a refused request.
type ExhaustedError struct {
	Budget Budget
	Units  int64
}

func (e *ExhaustedError) Error() string {
	return fmt.Sprintf("%s: %s/%s %d of %d reserved, %d requested", ErrExhausted, e.Budget.Queue, e.Budget.Resource,
		e.Budget.Reserved, e.Budget.Capacity, e.Units)
}

func (e *ExhaustedError) Is(t error) bool { return t == ErrExhausted }

// Store is the persistence port.
type Store interface {
	Reserve(ctx context.Context, r LeaseRequest) (Reservation, error)
	Renew(ctx context.Context, tenantID, id string, token int64, ttl time.Duration) error
	Release(ctx context.Context, tenantID, id string, token int64) error
	// Reap expires ACTIVE reservations past their expiry and returns their
	// units; it returns how many were expired.
	Reap(ctx context.Context, before time.Time, limit int) (int, error)
}

// InUse reports whether the work a reservation admitted still exists and
// runs. The reaper asks before it frees an expired reservation: a
// reservation whose owner crashed after the run was created is renewed,
// not freed, so the budget keeps counting a run that is really in flight.
type InUse func(ctx context.Context, r Reservation) (bool, error)

// Group reserves several budgets as one admission: either every request is
// admitted, or the ones already taken are released again. Requests are
// taken in the given order; callers order them consistently (queue budget
// before the tenant-wide one), so two replicas cannot deadlock.
func Group(ctx context.Context, s Store, reqs ...LeaseRequest) ([]Reservation, error) {
	var held []Reservation
	for _, r := range reqs {
		res, err := s.Reserve(ctx, r)
		if err != nil {
			for _, h := range held {
				if !h.Existing {
					_ = s.Release(context.WithoutCancel(ctx), h.TenantID, h.ID, h.Token)
				}
			}
			return nil, err
		}
		held = append(held, res)
	}
	return held, nil
}
