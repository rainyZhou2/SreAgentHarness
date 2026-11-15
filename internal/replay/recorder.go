package replay

import (
	"sync"

	"go-sre-harness/internal/security"
)

type Record struct {
	Result    string
	Operation security.Operation
}

type Recorder interface {
	Record(result string, operation security.Operation) error
}

type MemoryRecorder struct {
	mu      sync.RWMutex
	entries []Record
}

func NewMemoryRecorder() *MemoryRecorder { return &MemoryRecorder{} }

func (r *MemoryRecorder) Record(result string, operation security.Operation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, Record{Result: result, Operation: operation})
	return nil
}

func (r *MemoryRecorder) Entries() []Record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]Record(nil), r.entries...)
}
