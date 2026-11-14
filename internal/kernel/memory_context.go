package kernel

import (
	"context"
	"encoding/json"
	"fmt"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
)

// revalidateMemorySearches filters the prompt view, including batch results.
// Original observation artifacts remain immutable audit records.
func (w *Worker) revalidateMemorySearches(ctx context.Context, lease domain.Lease, in *cognitive.ReasoningInput) error {
	validator, configured := w.cfg.Memory.(MemoryRecallValidator)
	for i := range in.Observations {
		o := &in.Observations[i]
		if o.Tool != domain.MemorySearchToolName || o.Outcome != "ok" {
			continue
		}
		var result struct {
			Query   string            `json:"query,omitempty"`
			Service string            `json:"service,omitempty"`
			Hits    []json.RawMessage `json:"hits"`
			Note    string            `json:"note"`
		}
		if err := json.Unmarshal(o.Content, &result); err != nil {
			result.Hits = nil
		}
		kept := make([]json.RawMessage, 0, len(result.Hits))
		if configured {
			for _, raw := range result.Hits {
				var hit struct {
					ID      string `json:"memory_id"`
					Version int64  `json:"version"`
				}
				if json.Unmarshal(raw, &hit) != nil || hit.ID == "" || hit.Version < 1 {
					continue
				}
				valid, err := validator.RecallableMemory(ctx, lease.TenantID, lease.RunID, hit.ID, hit.Version)
				if err != nil {
					return fmt.Errorf("revalidate memory search: %w", err)
				}
				if valid {
					kept = append(kept, raw)
				}
			}
		}
		result.Hits = kept
		result.Note = "Past experience, not current evidence. Only currently recallable versions are shown; verify before relying on them."
		content, err := json.Marshal(result)
		if err != nil {
			return err
		}
		o.Content = content
		in.RecalledMemories = append(in.RecalledMemories, recalledMemories(content)...)
	}
	return nil
}
