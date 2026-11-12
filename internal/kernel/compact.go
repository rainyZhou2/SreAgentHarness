package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"go-sre-harness/internal/cognitive"
	"sort"
	"strings"
	"unicode/utf8"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools/artifactq"
	"go-sre-harness/internal/untrusted"
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
	// Lease is the compact step's lease: a compactor that calls a model
	// uses it to fence the model request ledger.
	Lease domain.Lease
}

// DeterministicCompactor lists every folded observation on one line with a
// truncated body. It is the default compactor.
type DeterministicCompactor struct {
	Keep     int // newest observations kept verbatim (default 2)
	MaxBytes int // per-observation body bytes (default 400)
	// Salient keeps the unusual and failure-related lines of a long body
	// instead of its first MaxBytes (used for the digest a model summarizes;
	// the default stays reproducible and cheap).
	Salient bool
}

func (DeterministicCompactor) Name() string { return "deterministic-v2" }

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
		// One line per observation, led by its citable ref.
		fmt.Fprintf(&b, "- %s %s %s → %s", cognitive.ObsPartRef(o.StepSeq, o.Part), o.Tool, compactArgs(o.Args), o.Outcome)
		// Structured facts first: they must survive the excerpt limit.
		if v := o.Verification; v != nil {
			fmt.Fprintf(&b, " [verification %s of %s@step%d after %d attempt(s)", v.Verdict, v.OfTool, v.OfStep, v.Attempts)
			if v.Compensation != "" {
				fmt.Fprintf(&b, "; compensation proposed: %s", v.Compensation)
			}
			b.WriteString("]")
		}
		body := strings.TrimSpace(string(o.Content))
		if a := o.Artifact; a != nil {
			fmt.Fprintf(&b, " [offloaded %d bytes, handle=%s — query with %s]", a.Bytes, a.Handle, artifactq.Name)
			body = offloadDigest(o.Content)
		}
		flags := o.Injection
		if len(flags) == 0 {
			flags = untrusted.Scan(body)
		}
		if len(flags) > 0 {
			fmt.Fprintf(&b, " [%s; excerpt withheld, see step %d output]", untrusted.FlagNote(flags), o.StepSeq)
			body = ""
		}
		if c.Salient {
			body = salientExcerpt(body, max)
		} else {
			if d := jsonDigest(body, max); d != "" {
				body = d
			}
			body = clipBytes(body, max)
		}
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
	if limit := w.compactTokens(); limit > 0 && keep > 1 {
		size := 0
		for _, o := range in.Observations {
			size += len(o.Content)
		}
		if size/3 > limit {
			keep = 1 // over the token budget: fold all but the newest result
		}
	}
	if keep < 1 {
		keep = 1
	}
	if len(in.Observations) <= keep {
		return w.compactResult(ctx, lease, in.Summary, in.CompactedBefore, c.Name())
	}
	old := in.Observations[:len(in.Observations)-keep]
	cut := in.Observations[len(in.Observations)-keep].StepSeq

	text, err := c.Compact(ctx, CompactInput{TenantID: lease.TenantID, RunID: lease.RunID, Goal: in.Goal,
		PriorSummary: priorNarrative(in.Summary), Observations: old, IsSideEffecting: w.isSideEffecting, Lease: lease})
	if err != nil {
		return domain.StepSuccess{}, domain.Retryable(fmt.Errorf("compactor %s: %w", c.Name(), err), 0)
	}
	var b strings.Builder
	b.WriteString(strings.TrimSpace(text))
	safety := priorSafety(in.Summary)
	for _, o := range old {
		switch {
		case o.Outcome != "ok":
			safety = append(safety, fmt.Sprintf("%s: %s was NOT executed successfully (%s) %s", cognitive.ObsPartRef(o.StepSeq, o.Part), o.Tool, o.Outcome, o.Note))
		case w.isSideEffecting(o.Tool):
			safety = append(safety, fmt.Sprintf("%s: side-effecting %s WAS executed with %s", cognitive.ObsPartRef(o.StepSeq, o.Part), o.Tool, compactArgs(o.Args)))
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

// offloadDigest keeps the compact parts of an offload summary (shape, size,
// levels, time range) and drops samples.
func offloadDigest(content []byte) string {
	var env struct {
		ContentType string         `json:"content_type"`
		Summary     map[string]any `json:"summary"`
	}
	if json.Unmarshal(content, &env) != nil || env.Summary == nil {
		return ""
	}
	keep := map[string]any{}
	for _, k := range []string{"type", "length", "lines", "levels", "time_range", "numeric", "time_fields", "trimmed"} {
		if v, ok := env.Summary[k]; ok {
			keep[k] = v
		}
	}
	if env.ContentType != "" {
		keep["content_type"] = env.ContentType
	}
	b, _ := json.Marshal(keep)
	return string(b)
}

// jsonDigest renders a JSON object result as its top-level fields: scalars
// with their values, arrays as their length, objects as their keys. It
// keeps every field visible within the excerpt limit, where a byte cut
// would keep only the first ones. "" when body is not a JSON object.
func jsonDigest(body string, max int) string {
	if len(body) <= max || !strings.HasPrefix(body, "{") {
		return ""
	}
	var m map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &m) != nil || len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v := strings.TrimSpace(string(m[k]))
		switch {
		case strings.HasPrefix(v, "["):
			var arr []json.RawMessage
			_ = json.Unmarshal(m[k], &arr)
			v = fmt.Sprintf("[%d items]", len(arr))
		case strings.HasPrefix(v, "{"):
			var obj map[string]json.RawMessage
			_ = json.Unmarshal(m[k], &obj)
			sub := make([]string, 0, len(obj))
			for sk := range obj {
				sub = append(sub, sk)
			}
			sort.Strings(sub)
			if len(sub) > 6 {
				sub = append(sub[:6], "…")
			}
			v = "{" + strings.Join(sub, ",") + "}"
		default:
			v = clipBytes(v, 80)
		}
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, " ")
}

// clipBytes cuts s to at most n bytes on a rune boundary.
func clipBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
