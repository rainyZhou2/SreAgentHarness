// Package playbook turns verified Works into reusable experience and keeps
// that experience honest (RFC §14.3):
//
//   - a Playbook candidate is proposed only from a Work whose outcome was
//     verified (machine evaluation or human confirmation), never from a
//     model's claim of success;
//   - it is a CANDIDATE procedural memory with applicability metadata
//     (services, subject kinds, environments, versions, source Work/runs);
//   - it becomes ACTIVE only through the memory transition API, approved by
//     a reviewer other than its creator, with at least one verified success
//     (memory.CanPromote, enforced by the memory stores);
//   - every later Work that retrieved it updates its success/failure
//     counters from that Work's verified outcome, once per Work.
//
// The package has no SQL: it works against ports implemented by the
// control-plane store (Works, actions) and the memory store.
package playbook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
)

// WorkLedger is the read side of the control plane used here.
type WorkLedger interface {
	GetWork(ctx context.Context, tenantID, workID string) (work.Work, error)
	ListWorks(ctx context.Context, f work.Filter) ([]work.Work, error)
	ListRunLinks(ctx context.Context, tenantID, workID string) ([]work.RunLink, error)
	ListOutcomes(ctx context.Context, tenantID, workID string, limit int) ([]outcome.Evaluation, error)
	ListActions(ctx context.Context, tenantID, workID string) ([]action.Record, error)
}

// MemoryLedger is the memory store port.
type MemoryLedger interface {
	CreateMemory(ctx context.Context, m memory.NewMemory) (memory.Memory, bool, error)
	GetMemory(ctx context.Context, tenantID, id string) (memory.Memory, error)
	RunMemoryIDs(ctx context.Context, tenantID, runID string) ([]string, error)
	RecordMemoryOutcome(ctx context.Context, tenantID, memoryID, workID string, success bool, evaluationID string) (bool, error)
}

// Service proposes playbooks and records outcome feedback.
type Service struct {
	Works    WorkLedger
	Memories MemoryLedger
	// Prepare runs the memory write-path checks (redaction, poisoning,
	// content ref, embedding): memory.Writer.Prepare.
	Prepare func(ctx context.Context, m memory.NewMemory) (memory.NewMemory, error)
	Clock   cpkit.Clock
	Log     *slog.Logger
}

// Errors.
var (
	ErrNotVerified = errors.New("work outcome is not verified")
	ErrInvalid     = errors.New("invalid playbook request")
)

// Verdict is the verified result of a closed Work.
type Verdict struct {
	Success      bool
	EvaluationID string
	// Source is "machine" or "human".
	Source string
}

// VerifiedVerdict returns the verified verdict of a terminal Work: a
// SUCCEEDED Work whose latest evaluation is SATISFIED, or a FAILED Work
// whose latest evaluation is NOT_SATISFIED. Cancelled Works, Works closed
// without an evaluation and inconclusive evaluations have no verdict.
func VerifiedVerdict(w work.Work, evals []outcome.Evaluation) (Verdict, bool) {
	if len(evals) == 0 {
		return Verdict{}, false
	}
	e := evals[0] // newest first
	src := "machine"
	if e.SpecHash == "human" {
		src = "human"
	}
	switch {
	case w.Status == work.StatusSucceeded && e.Verdict == outcome.Satisfied:
		return Verdict{Success: true, EvaluationID: e.ID, Source: src}, true
	case w.Status == work.StatusFailed && e.Verdict == outcome.NotSatisfied:
		return Verdict{Success: false, EvaluationID: e.ID, Source: src}, true
	}
	return Verdict{}, false
}

// ProposeRequest describes a candidate.
type ProposeRequest struct {
	TenantID string
	WorkID   string
	ActorID  string
	Title    string
	// Optional applicability overrides (defaults derive from the Work).
	Environments       []string
	VersionConstraints []string
	RiskOfReuse        string
	ValidFor           time.Duration // 0 = no expiry
}

// Propose creates a CANDIDATE playbook from a verified successful Work and
// records that Work as its first verified success. Proposing twice returns
// the existing candidate (content-deduplicated by the memory store).
func (s *Service) Propose(ctx context.Context, req ProposeRequest) (memory.Memory, error) {
	if req.TenantID == "" || req.WorkID == "" || req.ActorID == "" {
		return memory.Memory{}, fmt.Errorf("%w: tenant, work and actor are required", ErrInvalid)
	}
	w, err := s.Works.GetWork(ctx, req.TenantID, req.WorkID)
	if err != nil {
		return memory.Memory{}, err
	}
	evals, err := s.Works.ListOutcomes(ctx, req.TenantID, req.WorkID, 5)
	if err != nil {
		return memory.Memory{}, err
	}
	v, ok := VerifiedVerdict(w, evals)
	if !ok || !v.Success {
		return memory.Memory{}, fmt.Errorf("%w: only a SUCCEEDED Work with a SATISFIED outcome becomes a playbook (status %s)",
			ErrNotVerified, w.Status)
	}
	acts, err := s.Works.ListActions(ctx, req.TenantID, req.WorkID)
	if err != nil {
		return memory.Memory{}, err
	}
	links, err := s.Works.ListRunLinks(ctx, req.TenantID, req.WorkID)
	if err != nil {
		return memory.Memory{}, err
	}
	appl := memory.Applicability{Kind: "playbook", Environments: req.Environments, VersionConstraints: req.VersionConstraints,
		RiskOfReuse: req.RiskOfReuse, SourceWorkIDs: []string{w.ID}, EvidenceRefs: evals[0].EvidenceRefs}
	if appl.RiskOfReuse == "" {
		appl.RiskOfReuse = riskOf(acts)
	}
	for _, l := range links {
		appl.SourceRunIDs = append(appl.SourceRunIDs, l.RunID)
	}
	kinds := map[string]bool{}
	for _, ref := range w.Goal.SubjectRefs {
		kind, name := splitRef(ref)
		if kind != "" && !kinds[kind] {
			kinds[kind] = true
			appl.SubjectKinds = append(appl.SubjectKinds, kind)
		}
		if name != "" && !contains(appl.Services, name) {
			appl.Services = append(appl.Services, name)
		}
	}
	if env := w.Goal.Labels["environment"]; env != "" && len(appl.Environments) == 0 {
		appl.Environments = []string{env}
	}
	if err := appl.Normalize(); err != nil {
		return memory.Memory{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = "Playbook: " + cpkit.Truncate(w.Title, 180)
	}
	scope, scopeID := "organization", ""
	if len(appl.Services) == 1 {
		scope, scopeID = "service", appl.Services[0]
	}
	n := memory.NewMemory{TenantID: req.TenantID, Scope: scope, ScopeID: scopeID, Type: "procedural", Status: "CANDIDATE",
		Subject: "playbook:" + w.Template.ID + ":" + strings.Join(appl.SubjectKinds, ","),
		Title:   title, Content: Content(w, acts, evals[0]),
		SourceRefs: []string{"work:" + w.ID}, EvidenceRefs: evals[0].EvidenceRefs, Trust: "T3",
		CreatedBy: req.ActorID, Applicability: &appl}
	if req.ValidFor > 0 {
		exp := s.Clock.Now().Add(req.ValidFor)
		n.ExpiresAt = &exp
	}
	if s.Prepare != nil {
		if n, err = s.Prepare(ctx, n); err != nil {
			return memory.Memory{}, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
	}
	m, _, err := s.Memories.CreateMemory(ctx, n)
	if err != nil {
		return memory.Memory{}, err
	}
	if _, err := s.Memories.RecordMemoryOutcome(ctx, req.TenantID, m.ID, w.ID, true, v.EvaluationID); err != nil {
		return m, err
	}
	return s.Memories.GetMemory(ctx, req.TenantID, m.ID)
}

// riskOf derives the reuse risk from the most dangerous applied action.
func riskOf(acts []action.Record) string {
	risk := "low"
	for _, a := range acts {
		if !applied(a.State) {
			continue
		}
		switch a.Risk {
		case "R2", "R3":
			return "high"
		case "R1":
			risk = "medium"
		}
	}
	return risk
}

func applied(s action.State) bool {
	switch s {
	case action.StateApplied, action.StateVerifying, action.StateVerified, action.StateDegraded:
		return true
	}
	return false
}

func splitRef(ref string) (string, string) {
	kind, rest, ok := strings.Cut(ref, "://")
	if !ok {
		return "", ""
	}
	if i := strings.LastIndex(rest, "/"); i >= 0 {
		rest = rest[i+1:]
	}
	if i := strings.IndexAny(rest, "?#"); i >= 0 {
		rest = rest[:i]
	}
	return kind, rest
}

func contains(l []string, v string) bool {
	for _, x := range l {
		if x == v {
			return true
		}
	}
	return false
}

// Content renders the playbook text: trigger, the actions that were
// applied (in order), and the objectives that verified the outcome. It is
// derived from ledgers, not from model prose, so it states what was done
// and measured — not a guessed root cause.
func Content(w work.Work, acts []action.Record, e outcome.Evaluation) string {
	var b strings.Builder
	fmt.Fprintf(&b, "When: %s (%s) on %s.\n", cpkit.Truncate(w.Goal.Intent, 200), w.Kind, strings.Join(w.Goal.SubjectRefs, ", "))
	if d := strings.TrimSpace(w.Goal.Description); d != "" {
		fmt.Fprintf(&b, "Context: %s\n", cpkit.Truncate(d, 1000))
	}
	sort.SliceStable(acts, func(i, j int) bool { return acts[i].UpdatedAt.Before(acts[j].UpdatedAt) })
	n := 0
	for _, a := range acts {
		if !applied(a.State) {
			continue
		}
		if n == 0 {
			b.WriteString("Steps that restored the objective:\n")
		}
		n++
		fmt.Fprintf(&b, "%d. %s on %s (risk %s) with %s -> %s\n", n, a.ActionName, a.TargetRef, a.Risk,
			cpkit.Truncate(string(a.Args), 300), a.State)
	}
	if n == 0 {
		b.WriteString("No production action was needed.\n")
	}
	b.WriteString("Verified by:\n")
	for _, o := range e.Objectives {
		if o.Verdict != outcome.Satisfied {
			continue
		}
		fmt.Fprintf(&b, "- %s (%s): %s\n", o.ID, o.Kind, cpkit.Truncate(o.Reason, 200))
	}
	b.WriteString("Preconditions: re-check the current topology, versions and blast radius before reusing; this playbook grants no permission.\n")
	return b.String()
}

// WorkClosed records a closed Work's verified outcome on every playbook or
// RCA memory its runs retrieved. It is idempotent (per memory and Work)
// and best-effort: errors are returned for the caller to log or retry.
func (s *Service) WorkClosed(ctx context.Context, w work.Work) (int, error) {
	evals, err := s.Works.ListOutcomes(ctx, w.TenantID, w.ID, 5)
	if err != nil {
		return 0, err
	}
	v, ok := VerifiedVerdict(w, evals)
	if !ok {
		return 0, nil
	}
	links, err := s.Works.ListRunLinks(ctx, w.TenantID, w.ID)
	if err != nil {
		return 0, err
	}
	seen := map[string]bool{}
	counted := 0
	var errs []error
	for _, l := range links {
		ids, err := s.Memories.RunMemoryIDs(ctx, w.TenantID, l.RunID)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, id := range ids {
			if seen[id] {
				continue
			}
			seen[id] = true
			m, err := s.Memories.GetMemory(ctx, w.TenantID, id)
			if err != nil || m.Applicability == nil || (m.Applicability.Kind != "playbook" && m.Applicability.Kind != "rca") {
				continue
			}
			ok, err := s.Memories.RecordMemoryOutcome(ctx, w.TenantID, id, w.ID, v.Success, v.EvaluationID)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if ok {
				counted++
			}
		}
	}
	return counted, errors.Join(errs...)
}

// ClosedLister lists recently closed Works across tenants (system sweep).
type ClosedLister interface {
	RecentlyClosedWorks(ctx context.Context, since time.Time, limit int) ([]work.Work, error)
}

// Sweep re-applies WorkClosed to Works closed since the given time, so a
// crash between a Work's close and its feedback loses nothing (counting is
// idempotent per memory and Work).
func (s *Service) Sweep(ctx context.Context, closed ClosedLister, since time.Time, limit int) (int, error) {
	ws, err := closed.RecentlyClosedWorks(ctx, since, limit)
	if err != nil {
		return 0, err
	}
	total := 0
	var errs []error
	for _, w := range ws {
		n, err := s.WorkClosed(ctx, w)
		total += n
		if err != nil {
			errs = append(errs, err)
		}
	}
	return total, errors.Join(errs...)
}
