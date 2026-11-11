package orchestration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/work"
)

// stageChild is one child Work a children stage spawned.
type stageChild struct {
	Key      string `json:"key"`
	WorkID   string `json:"work_id"`
	Required bool   `json:"required"`
	Status   string `json:"status,omitempty"`
}

// ChildRequest creates a child Work under a parent (POST
// /v1/works/{id}/children). The child inherits the parent's subjects and
// constraints and may only narrow them.
type ChildRequest struct {
	TenantID       string     `json:"-"`
	ParentID       string     `json:"-"`
	ActorID        string     `json:"-"`
	ActorType      string     `json:"-"`
	IdempotencyKey string     `json:"-"`
	Kind           work.Kind  `json:"kind,omitempty"`
	Title          string     `json:"title,omitempty"`
	TemplateID     string     `json:"template,omitempty"`
	Queue          work.Queue `json:"queue,omitempty"`
	// Goal may leave subject_refs and constraints empty to inherit.
	Goal work.GoalSpec `json:"goal"`
	// DependsOn are Works (typically siblings) the child waits for.
	DependsOn []ChildDependency `json:"depends_on,omitempty"`
}

// ChildDependency is one upstream of a new child.
type ChildDependency struct {
	WorkID       string              `json:"work_id"`
	Kind         work.DependencyKind `json:"kind,omitempty"`
	PredicateRef string              `json:"predicate_ref,omitempty"`
	// Required defaults to true.
	Optional bool `json:"optional,omitempty"`
}

// CreateChild creates a child Work. Templates come from the catalog only:
// a child cannot bring its own stages.
func (s *Service) CreateChild(ctx context.Context, req ChildRequest) (work.Work, bool, error) {
	parent, err := s.Store.GetWork(ctx, req.TenantID, req.ParentID)
	if err != nil {
		return work.Work{}, false, err
	}
	kind := req.Kind
	if kind == "" {
		kind = parent.Kind
	}
	tpl, err := s.Catalog.ForKind(req.TemplateID, kind)
	if err != nil {
		return work.Work{}, false, err
	}
	if req.Goal.Intent == "" {
		req.Goal.Intent = parent.Goal.Intent
	}
	actorType := req.ActorType
	if actorType == "" {
		actorType = "user"
	}
	n := work.NewChild{ParentID: parent.ID, Work: work.NewWork{TenantID: req.TenantID, Kind: kind, Title: req.Title, Goal: req.Goal,
		Template: tpl, Queue: req.Queue, ActorType: actorType, ActorID: req.ActorID, IdempotencyKey: req.IdempotencyKey,
		Subjects: []work.SubjectLink{{Kind: "parent_work", ID: parent.ID}}, Now: s.Clock.Now()}}
	for _, d := range req.DependsOn {
		n.DependsOn = append(n.DependsOn, work.Dependency{ParentWorkID: d.WorkID, Kind: d.Kind, PredicateRef: d.PredicateRef,
			Required: !d.Optional})
	}
	w, created, err := s.Store.CreateChild(ctx, n)
	switch {
	case err == nil && created:
		s.Metrics.graph("child_created")
	case work.LimitName(err) != "":
		s.Metrics.graph("limit_refused")
	}
	return w, created, err
}

// AddDependency adds an edge between two Works of the tenant.
func (s *Service) AddDependency(ctx context.Context, tenantID string, dep work.Dependency, expectedVersion int64) error {
	err := s.Store.AddDependency(ctx, tenantID, dep, expectedVersion)
	switch {
	case err == nil:
		s.Metrics.graph("dependency_added")
	case errors.Is(err, work.ErrInvalid) && strings.Contains(err.Error(), "cycle"):
		s.Metrics.graph("cycle_refused")
	}
	return err
}

// childrenStage starts the child Works of the template graph and waits
// until the required ones ended. Children are created idempotently (their
// key is derived from the parent, generation, stage and child key), so a
// retried step never creates a second set.
//
// The stage completes when every required child succeeded. That moves the
// parent to its next stage; it does not make the parent succeed — that
// still takes the parent's own verified outcome.
func (d *driver) childrenStage(ctx context.Context, stage work.Stage, st stageState) (step, error) {
	w := &d.w
	g := w.Template.Graph
	if g == nil || len(g.Children) == 0 {
		return d.block(fmt.Sprintf("stage %s: the template has no graph children", stage.ID)), nil
	}
	now := d.o.now()
	if len(st.Children) == 0 {
		if d.o.Catalog == nil {
			return d.block("children stages need the template catalog"), nil
		}
		want := map[string]bool{}
		if stage.Children != nil {
			for _, k := range stage.Children.Keys {
				want[k] = true
			}
		}
		ids := map[string]string{}
		// Declaration order with upstream-first resolution: a child's
		// upstream siblings exist before it is created.
		pending := append([]work.ChildSpec(nil), g.Children...)
		for progress := true; len(pending) > 0 && progress; {
			progress = false
			rest := pending[:0]
			for _, c := range pending {
				if len(want) > 0 && !want[c.Key] {
					progress = true
					continue
				}
				ready := true
				for _, a := range c.After {
					if _, ok := ids[a]; !ok && (len(want) == 0 || want[a]) {
						ready = false
					}
				}
				if !ready {
					rest = append(rest, c)
					continue
				}
				child, err := d.spawnChild(ctx, stage, c, ids)
				if err != nil {
					if errors.Is(err, work.ErrInvalid) || errors.Is(err, work.ErrLimit) {
						d.o.Metrics.graph("limit_refused")
						return d.fail(stage, work.OnFailBlock, fmt.Sprintf("child %s: %v", c.Key, err)), nil
					}
					return step{}, err
				}
				ids[c.Key] = child.ID
				st.Children = append(st.Children, stageChild{Key: c.Key, WorkID: child.ID, Required: c.Required,
					Status: string(child.Status)})
				progress = true
			}
			pending = rest
		}
		d.o.Metrics.stage(string(stage.Type), "started")
		s := d.waitFor(st, fmt.Sprintf("waiting for %d child work(s)", len(st.Children)), d.childDeadline(stage, st))
		s.m.Events = append(s.m.Events, work.NewEvent{Type: "work.children_started", Payload: map[string]any{
			"stage_id": stage.ID, "children": st.Children}})
		return s, nil
	}

	open, failed, changed := 0, []string{}, false
	summary := map[string]string{}
	for i := range st.Children {
		c := &st.Children[i]
		cw, err := d.o.Store.GetWork(ctx, w.TenantID, c.WorkID)
		if err != nil {
			return step{}, fmt.Errorf("read child %s: %w", c.WorkID, err)
		}
		if string(cw.Status) != c.Status {
			c.Status, changed = string(cw.Status), true
		}
		summary[c.Key] = c.Status
		switch {
		case !cw.Status.Terminal():
			if c.Required {
				open++
			}
		case cw.Status != work.StatusSucceeded && c.Required:
			failed = append(failed, fmt.Sprintf("%s (%s)", c.Key, cw.Status))
		}
	}
	if len(failed) > 0 {
		return d.fail(stage, stage.OnFailure, "required child works did not succeed: "+strings.Join(failed, ", ")), nil
	}
	if open == 0 {
		return d.advance(stage, map[string]any{"children": summary}), nil
	}
	if stage.Children != nil && stage.Children.Timeout > 0 && !now.Before(st.StartedAt.Add(stage.Children.Timeout.D())) {
		return d.timeout(stage, fmt.Sprintf("%d required child work(s) still open", open)), nil
	}
	s := d.waitFor(st, fmt.Sprintf("waiting for %d child work(s)", open), d.childDeadline(stage, st))
	if !changed {
		s.m.StageState = nil
	}
	return s, nil
}

// childDeadline is when the stage looks again without a wake from the
// graph: the timeout when one is set, else a slow safety poll.
func (d *driver) childDeadline(stage work.Stage, st stageState) *time.Time {
	t := d.o.now().Add(5 * time.Minute)
	if stage.Children != nil && stage.Children.Timeout > 0 {
		if dl := st.StartedAt.Add(stage.Children.Timeout.D()); dl.Before(t) {
			t = dl
		}
	}
	return &t
}

// spawnChild creates one child of the template graph.
func (d *driver) spawnChild(ctx context.Context, stage work.Stage, c work.ChildSpec, ids map[string]string) (work.Work, error) {
	w := &d.w
	kind := c.Kind
	if kind == "" {
		kind = w.Kind
	}
	tpl, err := d.o.Catalog.ForKind(c.Template, kind)
	if err != nil {
		return work.Work{}, err
	}
	intent := cpkit.Truncate(work.Render(firstNonEmpty(c.Intent, "${goal.intent}"), w, nil), work.MaxIntent)
	goal := work.GoalSpec{Intent: intent, Description: cpkit.Truncate(work.Render(c.Description, w, nil), work.MaxDescription),
		Constraints: c.Constraints, Labels: map[string]string{"parent_work": w.ID, "child_key": c.Key}}
	title := cpkit.Truncate(work.Render(firstNonEmpty(c.Title, c.Key+": ${title}"), w, nil), work.MaxTitle)
	n := work.NewChild{ParentID: w.ID, Key: c.Key, Work: work.NewWork{TenantID: w.TenantID, Kind: kind, Title: title, Goal: goal,
		Template: tpl, Queue: c.Queue, ActorType: "system", ActorID: "work:" + w.ID, Now: d.o.now(),
		IdempotencyKey: fmt.Sprintf("child:%s:g%d:%s:%s", w.ID, w.Generation, stage.ID, c.Key),
		Subjects:       []work.SubjectLink{{Kind: "parent_work", ID: w.ID}}}}
	for _, a := range c.After {
		if up, ok := ids[a]; ok {
			n.DependsOn = append(n.DependsOn, work.Dependency{ParentWorkID: up, Kind: c.AfterKind, Required: true})
		}
	}
	child, created, err := d.o.Store.CreateChild(ctx, n)
	if err == nil && created {
		d.o.Metrics.graph("child_created")
	}
	return child, err
}
