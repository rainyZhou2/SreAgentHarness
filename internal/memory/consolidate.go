package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
)

// NewMemory is a memory write request (from the consolidator or the API).
type NewMemory struct {
	TenantID     string
	Scope        string
	ScopeID      string
	Type         string
	Status       string // CANDIDATE (default) or ACTIVE for verified writes
	Subject      string
	Title        string
	Content      string
	SourceRefs   []string
	EvidenceRefs []string
	Trust        string
	Sensitivity  string
	Confidence   *float64
	VerifiedBy   *string
	Environment  *string
	CreatedBy    string
	ExpiresAt    *time.Time
	// Filled by Writer.Prepare.
	ContentRef     string
	ContentHash    string
	Embedding      []float32
	EmbeddingModel string
	Redactions     int
}

var ErrPoisoned = errors.New("memory content looks like instructions (possible poisoning)")

// Writer applies the write-path checks shared by every memory source.
type Writer struct {
	Embedder  Embedder
	Artifacts artifact.Store
}

func (w Writer) Prepare(ctx context.Context, m NewMemory) (NewMemory, error) {
	switch m.Scope {
	case "organization", "service", "incident", "agent":
	default:
		return m, fmt.Errorf("invalid scope %q", m.Scope)
	}
	switch m.Type {
	case "semantic", "episodic", "procedural":
	default:
		return m, fmt.Errorf("invalid memory_type %q", m.Type)
	}
	if strings.TrimSpace(m.Subject) == "" || strings.TrimSpace(m.Title) == "" || strings.TrimSpace(m.Content) == "" {
		return m, errors.New("subject, title and content are required")
	}
	var n1, n2 int
	m.Title, n1 = Redact(m.Title)
	m.Content, n2 = Redact(m.Content)
	m.Redactions = n1 + n2
	if bad, match := PoisonCheck(m.Title + "\n" + m.Content); bad {
		return m, fmt.Errorf("%w: %q", ErrPoisoned, match)
	}
	if m.Status == "" {
		m.Status = "CANDIDATE"
	}
	if m.Trust == "" {
		m.Trust = "T3"
	}
	if m.Sensitivity == "" {
		m.Sensitivity = "internal"
	}
	doc, _ := json.Marshal(map[string]any{"subject": m.Subject, "title": m.Title, "content": m.Content,
		"source_refs": m.SourceRefs, "evidence_refs": m.EvidenceRefs})
	ref, hash, err := w.Artifacts.Put(ctx, m.TenantID, "memory", doc)
	if err != nil {
		return m, err
	}
	m.ContentRef, m.ContentHash = ref, artifact.Hash([]byte(m.Scope+"|"+m.ScopeID+"|"+m.Subject+"|"+m.Content))
	_ = hash
	m.Embedding, m.EmbeddingModel = w.Embedder.Embed(m.Title+"\n"+m.Content), w.Embedder.Model()
	return m, nil
}

// ---------- consolidator (cold path, §11.3) ----------

type Feedback struct {
	ActorID          string  `json:"actor_id"`
	RootCauseCorrect *bool   `json:"root_cause_correct,omitempty"`
	RootCauseLabel   *string `json:"root_cause_label,omitempty"`
	Rating           *int    `json:"rating,omitempty"`
	Notes            *string `json:"notes,omitempty"`
}

type RunRecord struct {
	Run      domain.Run
	Steps    []domain.Step
	Feedback []Feedback
}

type Experience struct {
	TenantID         string
	RunID            string
	IncidentID       *string
	Outcome          string
	OutcomeRef       string
	LessonsRef       string
	SourceRefs       []string
	QualityState     string
	ExtractorVersion string
}

type ConsolidationJob struct {
	TenantID string
	RunID    string
}

type ConsolidationStore interface {
	ClaimConsolidation(ctx context.Context, limit int, claimFor time.Duration) ([]ConsolidationJob, error)
	CompleteConsolidation(ctx context.Context, runID string, failure error) error
	RunRecord(ctx context.Context, tenantID, runID string) (RunRecord, error)
	InsertExperience(ctx context.Context, e Experience) (bool, error)
	CreateMemory(ctx context.Context, m NewMemory) (Memory, bool, error)
}

const ExtractorVersion = "consolidator-v1"

type Consolidator struct {
	Store     ConsolidationStore
	Artifacts artifact.Store
	Writer    Writer
	// SideEffecting reports whether a tool changes external state.
	SideEffecting func(tool string) bool
	Interval      time.Duration
	Log           *slog.Logger
}

func (c *Consolidator) Run(ctx context.Context) error {
	if c.Interval == 0 {
		c.Interval = 5 * time.Second
	}
	t := time.NewTicker(c.Interval)
	defer t.Stop()
	for {
		if _, err := c.Tick(ctx); err != nil && c.Log != nil && ctx.Err() == nil {
			c.Log.Warn("consolidation tick", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Tick processes up to 20 finished runs.
func (c *Consolidator) Tick(ctx context.Context) (int, error) {
	jobs, err := c.Store.ClaimConsolidation(ctx, 20, time.Minute)
	if err != nil {
		return 0, err
	}
	for _, j := range jobs {
		err := c.process(ctx, j)
		if cerr := c.Store.CompleteConsolidation(ctx, j.RunID, err); cerr != nil {
			return 0, cerr
		}
		if err != nil && c.Log != nil {
			c.Log.Warn("consolidation failed", "run_id", j.RunID, "err", err)
		}
	}
	return len(jobs), nil
}

type lessons struct {
	Goal        string   `json:"goal"`
	Outcome     string   `json:"outcome"`
	Conclusion  string   `json:"conclusion,omitempty"`
	ToolsUsed   []string `json:"tools_used"`
	Actions     []string `json:"actions_executed"`
	Blocked     []string `json:"actions_blocked"`
	Feedback    []string `json:"feedback,omitempty"`
	MemoryNotes []string `json:"memory_notes,omitempty"`
}

func (c *Consolidator) process(ctx context.Context, j ConsolidationJob) error {
	rec, err := c.Store.RunRecord(ctx, j.TenantID, j.RunID)
	if err != nil {
		return err
	}
	run := rec.Run
	var goal struct {
		Goal string `json:"goal"`
	}
	if data, err := c.Artifacts.Get(ctx, j.TenantID, run.GoalRef); err == nil {
		_ = json.Unmarshal(data, &goal)
	}
	l := lessons{Goal: goal.Goal, Outcome: string(run.Status)}
	if run.ResultRef != nil {
		var res struct {
			Answer string `json:"answer"`
		}
		if data, err := c.Artifacts.Get(ctx, j.TenantID, *run.ResultRef); err == nil {
			_ = json.Unmarshal(data, &res)
		}
		l.Conclusion = res.Answer
	}
	for _, s := range rec.Steps {
		if s.Kind != domain.StepTool {
			continue
		}
		var call domain.ToolCall
		if data, err := c.Artifacts.Get(ctx, j.TenantID, s.InputRef); err == nil {
			_ = json.Unmarshal(data, &call)
		}
		desc := fmt.Sprintf("%s %s", call.Name, strings.TrimSpace(string(call.Args)))
		l.ToolsUsed = append(l.ToolsUsed, call.Name)
		switch {
		case s.Status == domain.StepSucceeded && c.SideEffecting != nil && c.SideEffecting(call.Name):
			l.Actions = append(l.Actions, desc)
		case s.Status == domain.StepSkipped || s.Status == domain.StepFailed:
			l.Blocked = append(l.Blocked, desc+" ("+string(s.Status)+")")
		}
	}
	quality := "UNVERIFIED"
	for _, f := range rec.Feedback {
		if f.RootCauseCorrect != nil {
			if *f.RootCauseCorrect {
				quality = "VERIFIED"
			} else {
				quality = "DISPUTED"
			}
		}
		if f.Notes != nil {
			l.Feedback = append(l.Feedback, *f.Notes)
		}
	}

	service := ServiceFromGoal(goal.Goal)
	sourceRefs := []string{"run:" + run.ID}
	if run.Status == domain.RunCompleted && l.Conclusion != "" && service != "" && quality != "DISPUTED" {
		// Operator feedback is not approval: a verified run only yields a
		// VALIDATING memory, which an approver activates (TransitionMemory).
		status, trust := "CANDIDATE", "T3"
		if quality == "VERIFIED" {
			status = "VALIDATING"
		}
		content := fmt.Sprintf("Incident goal: %s\nConclusion: %s", goal.Goal, l.Conclusion)
		if len(l.Actions) > 0 {
			content += "\nActions executed: " + strings.Join(l.Actions, "; ")
		}
		mems := []NewMemory{{TenantID: j.TenantID, Scope: "service", ScopeID: service, Type: "episodic", Status: status,
			Subject: service + ":incident_resolution", Title: "Past incident on " + service + ": " + firstSentence(l.Conclusion),
			Content: content, SourceRefs: sourceRefs, Trust: trust, CreatedBy: "consolidator"}}
		if len(l.Actions) > 0 && quality == "VERIFIED" {
			mems = append(mems, NewMemory{TenantID: j.TenantID, Scope: "service", ScopeID: service, Type: "procedural",
				Status: status, Subject: service + ":remediation", Title: "Remediation that worked for " + service,
				Content:    "After verifying: " + firstSentence(l.Conclusion) + "\nRemediation: " + strings.Join(l.Actions, "; "),
				SourceRefs: sourceRefs, Trust: trust, CreatedBy: "consolidator"})
		}
		for _, nm := range mems {
			prepared, err := c.Writer.Prepare(ctx, nm)
			if errors.Is(err, ErrPoisoned) {
				l.MemoryNotes = append(l.MemoryNotes, "memory not stored: "+err.Error())
				continue
			}
			if err != nil {
				return err
			}
			if _, _, err := c.Store.CreateMemory(ctx, prepared); err != nil {
				return err
			}
		}
	}
	data, _ := json.Marshal(l)
	redacted, _ := Redact(string(data))
	ref, _, err := c.Artifacts.Put(ctx, j.TenantID, "lessons", []byte(redacted))
	if err != nil {
		return err
	}
	outcomeRef := ""
	if run.ResultRef != nil {
		outcomeRef = *run.ResultRef
	}
	_, err = c.Store.InsertExperience(ctx, Experience{TenantID: j.TenantID, RunID: run.ID, IncidentID: run.IncidentID,
		Outcome: string(run.Status), OutcomeRef: outcomeRef, LessonsRef: ref, SourceRefs: sourceRefs,
		QualityState: quality, ExtractorVersion: ExtractorVersion})
	return err
}

func firstSentence(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexAny(s, ".\n"); i > 0 && i < 160 {
		return s[:i]
	}
	if len(s) > 160 {
		return s[:160] + "…"
	}
	return s
}
