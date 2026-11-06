package gateway

import (
	"context"
	"sync"
	"time"
)

// MemLedger is an in-memory Ledger for unit tests and single-process tools.
type MemLedger struct {
	mu       sync.Mutex
	Requests map[string]*MemRequest
	Attempts []AttemptRecord
	Results  []AttemptResult
	Usage    []UsageRecord
	// FenceOK lets tests simulate a lost fence.
	FenceOK func(sc Scope) bool
}

type MemRequest struct {
	Record RequestRecord
	Result *RequestResult
}

func NewMemLedger() *MemLedger { return &MemLedger{Requests: map[string]*MemRequest{}} }

func (m *MemLedger) CreateModelRequest(_ context.Context, sc Scope, rec RequestRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FenceOK != nil && !m.FenceOK(sc) {
		return &Error{Code: ErrFenceLost, Msg: "fence token no longer current"}
	}
	m.Requests[rec.ID] = &MemRequest{Record: rec}
	return nil
}

func (m *MemLedger) StartModelAttempt(_ context.Context, _ Scope, rec AttemptRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Attempts = append(m.Attempts, rec)
	return nil
}

func (m *MemLedger) AppendUsage(_ context.Context, _ string, rec UsageRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Usage = append(m.Usage, rec)
	return nil
}

func (m *MemLedger) FinishModelAttempt(_ context.Context, _ Scope, rec AttemptResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Results = append(m.Results, rec)
	return nil
}

func (m *MemLedger) FinishModelRequest(_ context.Context, _ Scope, rec RequestResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.Requests[rec.ID]; ok {
		r.Result = &rec
	}
	return nil
}

func (m *MemLedger) SpentMicros(_ context.Context, _ string, _ time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// effective = latest revision per attempt
	latest := map[string]UsageRecord{}
	for _, u := range m.Usage {
		if cur, ok := latest[u.AttemptID]; !ok || u.Revision > cur.Revision {
			latest[u.AttemptID] = u
		}
	}
	var total int64
	for _, u := range latest {
		total += u.CostMicros
	}
	return total, nil
}

// MemArtifacts is an in-memory Artifacts store.
type MemArtifacts struct {
	mu   sync.Mutex
	Data map[string][]byte
	n    int
}

func NewMemArtifacts() *MemArtifacts { return &MemArtifacts{Data: map[string][]byte{}} }

func (a *MemArtifacts) Put(_ context.Context, tenant, kind string, data []byte) (string, string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n++
	ref := tenant + "/" + kind + "/" + time.Now().Format("150405.000000") + "-" + itoa(a.n)
	a.Data[ref] = append([]byte(nil), data...)
	return ref, "sha256:test", nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
