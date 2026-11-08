package reasoning

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

// PatternSubject is the memory subject suffix of reasoning patterns.
const PatternSubject = "reasoning_pattern"

// DistillerID is the creator recorded on distilled patterns.
const DistillerID = "reasoning-distiller"

// RecorderStore is what the recorder reads and writes.
type RecorderStore interface {
	RunTrace(ctx context.Context, tenantID, runID string) (map[string][]domain.Thought, map[string][]domain.CapabilityRef, error)
	RunMemoryIDs(ctx context.Context, tenantID, runID string) ([]string, error)
	PutReasoningScore(ctx context.Context, sc RunScore) error
	CreateMemory(ctx context.Context, m memory.NewMemory) (memory.Memory, bool, error)
}

// Recorder is a consolidation hook: it scores every finished run and
// distills reasoning patterns from verified ones.
type Recorder struct {
	Store     RecorderStore
	Artifacts artifact.Store
	Writer    memory.Writer
	// Distill enables pattern distillation (default on via NewRecorder).
	Distill bool
	Log     *slog.Logger
}

// NewRecorder returns a recorder with distillation enabled.
func NewRecorder(store RecorderStore, arts artifact.Store, w memory.Writer, log *slog.Logger) *Recorder {
	return &Recorder{Store: store, Artifacts: arts, Writer: w, Distill: true, Log: log}
}

func (r *Recorder) Name() string { return "reasoning" }

type goalDoc struct {
	Goal    string `json:"goal"`
	Context *struct {
		Services []string `json:"services"`
	} `json:"context,omitempty"`
}

// AfterRun scores the run and, when it qualifies, distills a pattern.
func (r *Recorder) AfterRun(ctx context.Context, rec memory.RunRecord, quality string) error {
	run := rec.Run
	if !run.Status.Terminal() {
		return nil
	}
	thoughts, _, err := r.Store.RunTrace(ctx, run.TenantID, run.ID)
	if err != nil {
		return fmt.Errorf("run trace: %w", err)
	}
	var g goalDoc
	if data, err := r.Artifacts.Get(ctx, run.TenantID, run.GoalRef); err == nil {
		_ = json.Unmarshal(data, &g)
	}
	trace := Trace{Completed: run.Status == domain.RunCompleted, Correct: feedbackCorrect(rec.Feedback)}
	steps := append([]domain.Step(nil), rec.Steps...)
	sort.Slice(steps, func(i, j int) bool { return steps[i].Seq < steps[j].Seq })
	for _, st := range steps {
		s := Step{Seq: st.Seq, Reason: st.Kind == domain.StepReason, Thoughts: thoughts[st.ID]}
		if st.Kind == domain.StepTool && st.InputRef != "" {
			var call domain.ToolCall
			if data, err := r.Artifacts.Get(ctx, run.TenantID, st.InputRef); err == nil && json.Unmarshal(data, &call) == nil {
				s.Tool, s.Args = call.Name, call.Args
			}
			if s.Tool == domain.BatchToolName {
				// One trace step per call of a parallel batch.
				var ba struct {
					Calls []struct {
						Name string          `json:"name"`
						Args json.RawMessage `json:"args"`
					} `json:"calls"`
				}
				if json.Unmarshal(s.Args, &ba) == nil && len(ba.Calls) > 0 {
					for i, c := range ba.Calls {
						part := Step{Seq: st.Seq, Tool: c.Name, Args: c.Args}
						if i == 0 {
							part.Thoughts = s.Thoughts
						}
						trace.Steps = append(trace.Steps, part)
					}
					continue
				}
			}
		}
		if s.Reason || s.Tool != "" || len(s.Thoughts) > 0 {
			trace.Steps = append(trace.Steps, s)
		}
	}
	m := Score(trace)
	pm, plan := PlanMetricsOf(ctx, r.Artifacts, run, rec.Steps)
	m.Plan = pm
	ids, err := r.Store.RunMemoryIDs(ctx, run.TenantID, run.ID)
	if err != nil {
		return fmt.Errorf("run memories: %w", err)
	}
	service := serviceOf(g)
	if err := r.Store.PutReasoningScore(ctx, RunScore{TenantID: run.TenantID, RunID: run.ID, ProfileID: run.ProfileID,
		ProfileVersion: run.ProfileVersion, Service: service, Outcome: string(run.Status), Metrics: m,
		MemoryIDs: ids}); err != nil {
		return fmt.Errorf("store score: %w", err)
	}
	if !r.Distill {
		return nil
	}
	if nm, ok := DistillPattern(run, g.Goal, service, trace, m, quality); ok {
		if err := r.store(ctx, run.ID, "reasoning pattern", nm); err != nil {
			return err
		}
	}
	if nm, ok := DistillPlanExemplar(run, g.Goal, service, plan, pm, quality, trace.Correct); ok {
		if err := r.store(ctx, run.ID, "plan exemplar", nm); err != nil {
			return err
		}
	}
	return nil
}

func (r *Recorder) store(ctx context.Context, runID, what string, nm memory.NewMemory) error {
	prepared, err := r.Writer.Prepare(ctx, nm)
	if err != nil {
		// Poisoned or otherwise unfit content is not stored.
		if r.Log != nil {
			r.Log.Info(what+" not stored", "run_id", runID, "err", err)
		}
		return nil
	}
	_, _, err = r.Store.CreateMemory(ctx, prepared)
	return err
}

func feedbackCorrect(fs []memory.Feedback) *bool {
	var out *bool
	for _, f := range fs {
		if f.RootCauseCorrect != nil {
			v := *f.RootCauseCorrect
			out = &v
		}
	}
	return out
}

func serviceOf(g goalDoc) string {
	if g.Context != nil {
		if s := domain.NormalizeServices(g.Context.Services); len(s) > 0 {
			return s[0]
		}
	}
	return memory.ServiceFromGoal(g.Goal)
}

// DistillPattern turns a verified run into a reasoning-pattern memory. It
// returns false when the run does not qualify: it must have completed,
// concluded VERIFIED, not be disputed, name a service and have settled at
// least one hypothesis.
func DistillPattern(run domain.Run, goal, service string, t Trace, m Metrics, quality string) (memory.NewMemory, bool) {
	if run.Status != domain.RunCompleted || quality == "DISPUTED" || service == "" ||
		!m.VerifiedConclusion() || m.Verified+m.Refuted == 0 {
		return memory.NewMemory{}, false
	}
	type entry struct {
		statement string
		status    domain.ThoughtStatus
		conf      float64
	}
	var order []string
	final := map[string]entry{}
	var tools []string
	seenTool := map[string]bool{}
	verdictReached := false
	verification := ""
	for _, s := range t.Steps {
		if s.Tool != "" && !verdictReached && !seenTool[s.Tool] {
			seenTool[s.Tool] = true
			tools = append(tools, s.Tool)
		}
		for _, th := range s.Thoughts {
			switch th.Kind {
			case domain.ThoughtHypothesis:
				k := normalize(th.Summary)
				if k == "" {
					continue
				}
				if _, ok := final[k]; !ok {
					order = append(order, k)
				}
				e := entry{statement: strings.TrimSpace(th.Summary), status: th.Status, conf: th.Confidence}
				if e.conf == 0 {
					e.conf = final[k].conf
				}
				final[k] = e
				if th.Status == domain.ThoughtVerified || th.Status == domain.ThoughtRefuted {
					verdictReached = true
				}
			case domain.ThoughtVerification:
				verification = strings.TrimSpace(th.Summary)
			}
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Reasoning pattern for %s (source run %s)\n", service, run.ID)
	fmt.Fprintf(&b, "Situation: %s\n", clip(oneLine(goal), 300))
	b.WriteString("Hypotheses checked, in order:\n")
	n := 0
	firstVerified := ""
	for _, k := range order {
		e := final[k]
		if e.status != domain.ThoughtVerified && e.status != domain.ThoughtRefuted {
			continue
		}
		n++
		if n > 8 {
			break
		}
		fmt.Fprintf(&b, "%d. %s → %s", n, clip(oneLine(e.statement), 240), e.status)
		if e.conf > 0 {
			fmt.Fprintf(&b, " (confidence %.2f)", e.conf)
		}
		b.WriteString("\n")
		if e.status == domain.ThoughtVerified && firstVerified == "" {
			firstVerified = e.statement
		}
	}
	if len(tools) > 0 {
		if len(tools) > 8 {
			tools = tools[:8]
		}
		fmt.Fprintf(&b, "Tools in order until the first verdict: %s\n", strings.Join(tools, ", "))
	}
	if verification != "" {
		fmt.Fprintf(&b, "Conclusion check: %s\n", clip(oneLine(verification), 400))
	}
	title := "Reasoning pattern for " + service
	if firstVerified != "" {
		title += ": " + clip(oneLine(firstVerified), 120)
	}
	status := "CANDIDATE"
	if quality == "VERIFIED" {
		status = "VALIDATING"
	}
	conf := m.Quality
	shards := []domain.MemoryShard{{ShardKey: "service:" + service, Weight: 1, Source: "scope"},
		{ShardKey: "run:" + run.ID, Weight: 0.4, Source: "run"}}
	for _, tool := range tools {
		shards = append(shards, domain.MemoryShard{ShardKey: "tool:" + tool, Weight: 0.6, Source: "pattern"})
	}
	return memory.NewMemory{TenantID: run.TenantID, Scope: "service", ScopeID: service, Type: "procedural",
		Status: status, Subject: service + ":" + PatternSubject, Title: title, Content: strings.TrimSpace(b.String()),
		SourceRefs: []string{"run:" + run.ID}, Trust: "T3", Confidence: &conf, CreatedBy: DistillerID,
		Shards: shards}, true
}

// IsPattern reports whether a memory is a learned procedure whose
// effectiveness is measured and can be retired: a reasoning pattern or a
// plan exemplar.
func IsPattern(m memory.Memory) bool {
	return m.Type == "procedural" && (strings.HasSuffix(m.Subject, ":"+PatternSubject) || IsPlanExemplar(m))
}

// IsReasoningPattern reports whether a memory is a reasoning pattern only.
func IsReasoningPattern(m memory.Memory) bool {
	return m.Type == "procedural" && strings.HasSuffix(m.Subject, ":"+PatternSubject)
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
