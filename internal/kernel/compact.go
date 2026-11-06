package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
)

// needsCompaction counts tool observations since the last compact step.
// A profile's compact_every (> 0) overrides the worker default.
func (w *Worker) needsCompaction(ctx context.Context, lease domain.Lease, profileEvery int) bool {
	every := w.cfg.CompactEvery
	if profileEvery > 0 {
		every = profileEvery
	}
	if every < 0 {
		return false
	}
	hist, err := w.store.History(ctx, lease.TenantID, lease.RunID, lease.StepSeq)
	if err != nil {
		return false
	}
	n := 1 // the observation being committed now
	for i := len(hist) - 1; i >= 0; i-- {
		if hist[i].Kind == domain.StepCompact && hist[i].Status == domain.StepSucceeded {
			break
		}
		if hist[i].Kind == domain.StepTool {
			n++
		}
	}
	return n >= every
}

// Compactor writes the narrative part of a compaction summary (the
// compactor plugin kind). The kernel decides when to compact, which
// observations are folded, and always appends the safety-relevant facts
// itself, so a compactor can shorten history but never hide a denied,
// failed or executed side-effecting action (§33.4). Compact must be
// deterministic for the same input: a retried compact step has to produce
// the same summary.
type Compactor interface {
	Name() string
	// KeepRecent is how many of the newest observations stay verbatim.
	KeepRecent() int
	Compact(ctx context.Context, in CompactInput) (string, error)
}

// CompactInput is what a compactor summarizes.
type CompactInput struct {
	TenantID     string
	RunID        string
	Goal         string
	PriorSummary string
	// Observations are the ones being folded, oldest first.
	Observations    []domain.Observation
	IsSideEffecting func(tool string) bool
}

// DeterministicCompactor lists every folded observation on one line with a
// truncated body. It is the default compactor.
type DeterministicCompactor struct {
	Keep     int // newest observations kept verbatim (default 2)
	MaxBytes int // per-observation body bytes (default 400)
}

func (DeterministicCompactor) Name() string { return "deterministic-v1" }

func (c DeterministicCompactor) KeepRecent() int {
	if c.Keep <= 0 {
		return 2
	}
	return c.Keep
}

func (c DeterministicCompactor) Compact(_ context.Context, in CompactInput) (string, error) {
	max := c.MaxBytes
	if max <= 0 {
		max = 400
	}
	var b strings.Builder
	if in.PriorSummary != "" {
		b.WriteString(in.PriorSummary)
		b.WriteString("\n")
	}
	for _, o := range in.Observations {
		body := strings.TrimSpace(string(o.Content))
		if len(body) > max {
			body = body[:max] + "…"
		}
		fmt.Fprintf(&b, "- step %d %s %s → %s", o.StepSeq, o.Tool, compactArgs(o.Args), o.Outcome)
		if body != "" {
			fmt.Fprintf(&b, ": %s", body)
		}
		if o.Note != "" {
			fmt.Fprintf(&b, " (note: %s)", o.Note)
		}
		b.WriteString("\n")
	}
	return b.String(), nil
}

const safetyHeader = "Safety-relevant facts (never repeat a denied action; executed actions already took effect):"

// executeCompact replaces older observations with the compactor's summary
// plus the kernel's safety facts (§33.4).
func (w *Worker) executeCompact(ctx context.Context, lease domain.Lease) (domain.StepSuccess, error) {
	in, err := w.renderContext(ctx, lease)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	c := w.cfg.Compactor
	keep := c.KeepRecent()
	if keep < 1 {
		keep = 1
	}
	if len(in.Observations) <= keep {
		return w.compactResult(ctx, lease, in.Summary, in.CompactedBefore, c.Name())
	}
	old := in.Observations[:len(in.Observations)-keep]
	cut := in.Observations[len(in.Observations)-keep].StepSeq

	text, err := c.Compact(ctx, CompactInput{TenantID: lease.TenantID, RunID: lease.RunID, Goal: in.Goal,
		PriorSummary: priorNarrative(in.Summary), Observations: old, IsSideEffecting: w.isSideEffecting})
	if err != nil {
		return domain.StepSuccess{}, domain.Retryable(fmt.Errorf("compactor %s: %w", c.Name(), err), 0)
	}
	var b strings.Builder
	b.WriteString(strings.TrimSpace(text))
	safety := priorSafety(in.Summary)
	for _, o := range old {
		switch {
		case o.Outcome != "ok":
			safety = append(safety, fmt.Sprintf("step %d: %s was NOT executed successfully (%s) %s", o.StepSeq, o.Tool, o.Outcome, o.Note))
		case w.isSideEffecting(o.Tool):
			safety = append(safety, fmt.Sprintf("step %d: side-effecting %s WAS executed with %s", o.StepSeq, o.Tool, compactArgs(o.Args)))
		}
	}
	if len(safety) > 0 {
		b.WriteString("\n" + safetyHeader + "\n")
		for _, s := range safety {
			b.WriteString("- " + strings.TrimSpace(strings.TrimPrefix(s, "- ")) + "\n")
		}
	}
	return w.compactResult(ctx, lease, strings.TrimSpace(b.String()), cut, c.Name())
}

// priorNarrative / priorSafety split an earlier summary so its safety facts
// are carried forward by the kernel even if a compactor drops the prior text.
func priorNarrative(summary string) string {
	if i := strings.Index(summary, safetyHeader); i >= 0 {
		return strings.TrimSpace(summary[:i])
	}
	return summary
}

func priorSafety(summary string) []string {
	i := strings.Index(summary, safetyHeader)
	if i < 0 {
		return nil
	}
	var out []string
	for _, l := range strings.Split(summary[i+len(safetyHeader):], "\n") {
		if l = strings.TrimSpace(l); strings.HasPrefix(l, "- ") {
			out = append(out, l)
		}
	}
	return out
}

func (w *Worker) compactResult(ctx context.Context, lease domain.Lease, text string, before int64, algorithm string) (domain.StepSuccess, error) {
	summary := map[string]any{"text": text, "compacted_before": before, "algorithm": algorithm}
	ref, h, err := w.put(ctx, lease.TenantID, "compaction", summary)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	sj, _ := json.Marshal(map[string]string{"text": text})
	return domain.StepSuccess{OutputRef: ref, OutputHash: h, ArtifactKind: "compaction",
		Compaction: &domain.Compaction{SummaryRef: ref, Summary: sj, CompactedBefore: before},
		Plan:       nil, // the reason step was planned together with this compact step
		Summary:    mustJSON(map[string]any{"compacted_before": before, "summary_bytes": len(text)})}, nil
}

func (w *Worker) isSideEffecting(tool string) bool {
	if t, ok := w.tools.Lookup(tool); ok {
		return t.Descriptor().Risk.HasSideEffects()
	}
	return true // unknown: be conservative
}

func compactArgs(b json.RawMessage) string {
	if len(b) == 0 {
		return "{}"
	}
	var v any
	if json.Unmarshal(b, &v) != nil {
		return string(b)
	}
	out, _ := json.Marshal(v)
	return string(out)
}

// attachCheckpoint uploads the checkpoint manifest for a successful step. A
// failed upload only skips the checkpoint; it never fails the step.
func (w *Worker) attachCheckpoint(ctx context.Context, lease domain.Lease, res *domain.StepSuccess) {
	man := map[string]any{"manifest_version": 1, "run_id": lease.RunID, "step_id": lease.StepID,
		"step_seq": lease.StepSeq, "attempt_id": lease.AttemptID, "context_snapshot_id": lease.ContextSnapshotID,
		"output_ref": res.OutputRef, "output_hash": res.OutputHash, "planned": len(res.Plan)}
	data, _ := json.Marshal(man)
	ref, _, err := w.artifacts.Put(ctx, lease.TenantID, "checkpoint", data)
	if err != nil {
		w.log.Warn("checkpoint manifest not stored", "run_id", lease.RunID, "err", err)
		return
	}
	res.CheckpointRef, res.CheckpointHash = ref, artifact.Hash(data)
}
