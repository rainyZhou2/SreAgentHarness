package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
)

// Service is the application API of Works used by HTTP handlers and SDKs.
type Service struct {
	Store   *cpsql.Store
	Runs    Runs // optional: cancelling a Work cancels its owner runs
	Catalog *Catalog
	Clock   cpkit.Clock
	Log     *slog.Logger
	// AllowInlineTemplates lets callers submit a template body instead of a
	// catalog id (admin-only at the HTTP layer).
	AllowInlineTemplates bool
}

func (s *Service) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// CreateRequest creates a Work.
type CreateRequest struct {
	TenantID       string          `json:"-"`
	ActorID        string          `json:"-"`
	ActorType      string          `json:"-"`
	IdempotencyKey string          `json:"-"`
	Kind           work.Kind       `json:"kind"`
	Title          string          `json:"title,omitempty"`
	Goal           work.GoalSpec   `json:"goal"`
	TemplateID     string          `json:"template,omitempty"`
	Template       *work.Template  `json:"template_spec,omitempty"`
	Priority       int             `json:"priority,omitempty"`
	Meta           json.RawMessage `json:"-"`
}

// Create creates a Work (idempotent on the key; a different body under the
// same key is ErrIdempotencyConflict).
func (s *Service) Create(ctx context.Context, req CreateRequest) (work.Work, bool, error) {
	if req.Kind == "" {
		req.Kind = work.KindGeneral
	}
	var tpl work.Template
	switch {
	case req.Template != nil:
		if !s.AllowInlineTemplates {
			return work.Work{}, false, fmt.Errorf("%w: inline templates are not allowed", work.ErrInvalid)
		}
		tpl = *req.Template
	default:
		t, err := s.Catalog.ForKind(req.TemplateID, req.Kind)
		if err != nil {
			return work.Work{}, false, err
		}
		tpl = t
	}
	var subjects []work.SubjectLink
	refs, err := cpkit.NormalizeRefs(req.Goal.SubjectRefs)
	if err != nil {
		return work.Work{}, false, fmt.Errorf("%w: subject_refs: %v", work.ErrInvalid, err)
	}
	req.Goal.SubjectRefs = refs
	for _, r := range refs {
		subjects = append(subjects, work.SubjectLink{Kind: "subject", ID: r})
	}
	actorType := req.ActorType
	if actorType == "" {
		actorType = "user"
	}
	return s.Store.CreateWork(ctx, work.NewWork{TenantID: req.TenantID, Kind: req.Kind, Title: req.Title, Goal: req.Goal,
		Template: tpl, Priority: req.Priority, ActorType: actorType, ActorID: req.ActorID,
		IdempotencyKey: req.IdempotencyKey, Subjects: subjects, Now: s.Clock.Now()})
}

// CommandRequest is an operator command.
type CommandRequest struct {
	TenantID        string          `json:"-"`
	WorkID          string          `json:"-"`
	ActorID         string          `json:"-"`
	IdempotencyKey  string          `json:"-"`
	ExpectedVersion int64           `json:"-"` // If-Match; 0 = any
	Type            string          `json:"type"`
	Reason          string          `json:"reason,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
}

// CommandPayload are the per-command options.
type CommandPayload struct {
	// replan: stage id to restart from (default: current stage).
	Stage string `json:"stage,omitempty"`
	// resolve: retry the current stage in a new generation, or skip it.
	Retry bool `json:"retry,omitempty"`
	Skip  bool `json:"skip,omitempty"`
	// confirm: satisfied | not_satisfied.
	Verdict string `json:"verdict,omitempty"`
	Comment string `json:"comment,omitempty"`
}

// Command applies an operator command. Commands are idempotent on the key:
// a replay returns the original result.
func (s *Service) Command(ctx context.Context, req CommandRequest) (work.Work, error) {
	if !work.ValidCommand(req.Type) {
		return work.Work{}, fmt.Errorf("%w: unknown command %q", work.ErrInvalid, req.Type)
	}
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 200 {
		return work.Work{}, fmt.Errorf("%w: idempotency key is required", work.ErrInvalid)
	}
	var p CommandPayload
	if len(req.Payload) > 0 {
		if len(req.Payload) > 16<<10 {
			return work.Work{}, fmt.Errorf("%w: payload too large", work.ErrInvalid)
		}
		if err := json.Unmarshal(req.Payload, &p); err != nil {
			return work.Work{}, fmt.Errorf("%w: payload: %v", work.ErrInvalid, err)
		}
	}
	cur, err := s.Store.GetWork(ctx, req.TenantID, req.WorkID)
	if err != nil {
		return work.Work{}, err
	}
	now := s.Clock.Now()
	reason := cpkit.Truncate(strings.TrimSpace(req.Reason), 500)
	reqHash := cpkit.MustHash(struct {
		T, R string
		P    CommandPayload
		V    int64
	}{req.Type, reason, p, req.ExpectedVersion})
	if prev, hash, ok, err := s.Store.CommandResult(ctx, req.TenantID, req.WorkID, req.IdempotencyKey); err != nil {
		return work.Work{}, err
	} else if ok {
		if hash != reqHash {
			return work.Work{}, work.ErrIdempotencyConflict
		}
		return prev, nil
	}
	m := work.Mutation{TenantID: req.TenantID, WorkID: req.WorkID, ExpectedVersion: req.ExpectedVersion,
		ActorType: "user", ActorID: req.ActorID, Now: now,
		Command: &work.CommandRecord{Key: req.IdempotencyKey, Type: req.Type,
			RequestHash: reqHash}}
	wake := func() { m.WakeAt = &now }
	setStatus := func(st work.Status) { m.Status = &st }
	if reason != "" {
		m.Reason = &reason
	}
	switch req.Type {
	case work.CmdPause:
		setStatus(work.StatusSuspended)
		m.ClearWake = true
	case work.CmdResume:
		if cur.Status != work.StatusSuspended {
			return work.Work{}, &work.TransitionError{From: cur.Status, To: work.StatusActive}
		}
		setStatus(work.StatusActive)
		wake()
	case work.CmdCancel:
		setStatus(work.StatusCancelled)
		m.CancelAllWatches = true
	case work.CmdWake:
		wake()
	case work.CmdReplan, work.CmdReopen:
		idx := cur.Stage
		if req.Type == work.CmdReopen {
			idx = 0
		}
		if p.Stage != "" {
			i, ok := cur.Template.Index(p.Stage)
			if !ok {
				return work.Work{}, fmt.Errorf("%w: unknown stage %q", work.ErrInvalid, p.Stage)
			}
			idx = i
		}
		if idx >= len(cur.Template.Stages) {
			idx = len(cur.Template.Stages) - 1
		}
		if req.Type == work.CmdReplan && cur.Replans >= cur.Template.Limits.MaxReplans {
			return work.Work{}, fmt.Errorf("%w: replan budget (%d) is exhausted", work.ErrInvalid, cur.Template.Limits.MaxReplans)
		}
		gen := cur.Generation + 1
		m.Stage, m.Generation, m.ClearState, m.CancelAllWatches = &idx, &gen, true, true
		if req.Type == work.CmdReplan {
			m.AddReplans = 1
		}
		setStatus(work.StatusActive)
		wake()
	case work.CmdResolve:
		if cur.Status != work.StatusBlocked {
			return work.Work{}, &work.TransitionError{From: cur.Status, To: work.StatusActive}
		}
		switch {
		case p.Skip:
			next := cur.Stage + 1
			m.Stage, m.ClearState = &next, true
		case p.Retry:
			gen := cur.Generation + 1
			m.Generation, m.ClearState = &gen, true
		}
		setStatus(work.StatusActive)
		wake()
	case work.CmdConfirm:
		var v outcome.Verdict
		switch strings.ToLower(p.Verdict) {
		case "satisfied", "yes", "true":
			v = outcome.Satisfied
		case "not_satisfied", "no", "false":
			v = outcome.NotSatisfied
		default:
			return work.Work{}, fmt.Errorf("%w: confirm needs verdict satisfied|not_satisfied", work.ErrInvalid)
		}
		if cur.Status.Terminal() {
			return work.Work{}, &work.TransitionError{From: cur.Status, To: cur.Status}
		}
		stageID := "final"
		if st, ok := cur.CurrentStage(); ok {
			stageID = st.ID
		}
		m.Outcome = &outcome.Evaluation{ID: cpkit.NewID(), SpecHash: "human", Verdict: v, RawVerdict: v,
			EvaluatedAt: now, Stage: stageID, Generation: cur.Generation,
			Objectives: []outcome.ObjectiveResult{{ID: "human_confirmation", Kind: outcome.KindHuman, Verdict: v,
				Reason: cpkit.Truncate(p.Comment, 500)}},
			Reasons: []string{"confirmed by " + req.ActorID}}
		m.Events = append(m.Events, work.NewEvent{Type: "work.confirmed", Payload: map[string]any{"verdict": v,
			"stage_id": stageID, "comment": cpkit.Truncate(p.Comment, 500)}})
		wake()
	}
	w, err := s.Store.ApplyWork(ctx, m)
	if err != nil {
		return w, err
	}
	if req.Type == work.CmdCancel {
		s.cancelRuns(ctx, w, req.ActorID)
	}
	return w, nil
}

// cancelRuns cancels the owner runs of a cancelled Work (best effort; the
// kernel cancel is idempotent). Dispatched actions are not undone.
func (s *Service) cancelRuns(ctx context.Context, w work.Work, actor string) {
	if s.Runs == nil {
		return
	}
	links, err := s.Store.ListRunLinks(ctx, w.TenantID, w.ID)
	if err != nil {
		s.log().Warn("list runs of cancelled work failed", "work_id", w.ID, "err", err)
		return
	}
	for _, l := range links {
		if l.Role != work.RoleOwner {
			continue
		}
		switch l.Status {
		case "COMPLETED", "FAILED", "CANCELLED":
			continue
		}
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		err := s.Runs.CancelRun(cctx, w.TenantID, l.RunID, actor, "work cancelled", "work-cancel:"+w.ID+":"+l.RunID)
		cancel()
		if err != nil {
			s.log().Warn("cancel run of cancelled work failed", "work_id", w.ID, "run_id", l.RunID, "err", err)
		}
	}
}
