package memory

import (
	"context"
	"errors"
	"fmt"

	"go-sre-harness/internal/domain"
)

// applyRunMemory turns a finished run's memory writes into stored memory:
//   - user notes become (or revise, or archive) the initiator's personal
//     memories, one per key, ACTIVE at once: they affect only that user's
//     own runs, and the user can list, edit and delete them;
//   - long_term notes become CANDIDATE memories (VALIDATING when feedback
//     verified the run) that an approver activates; a disputed run
//     proposes nothing.
//
// It is idempotent: consolidation runs again when feedback arrives.
func (c *Consolidator) applyRunMemory(ctx context.Context, rec RunRecord, rm *domain.RunMemory, service, quality string,
	shards []domain.MemoryShard, l *lessons) error {
	run := rec.Run
	source := "run:" + run.ID
	if run.InitiatorType == "user" && run.InitiatorID != nil && *run.InitiatorID != "" {
		user := *run.InitiatorID
		for _, n := range rm.User {
			note, err := c.applyPersonal(ctx, run.TenantID, user, n, source)
			if err != nil {
				return err
			}
			if note != "" {
				l.MemoryNotes = append(l.MemoryNotes, note)
			}
		}
	} else if len(rm.User) > 0 {
		l.MemoryNotes = append(l.MemoryNotes, "user notes ignored: the run was not started by a user")
	}
	if quality == "DISPUTED" || run.Status != domain.RunCompleted {
		if len(rm.LongTerm) > 0 {
			l.MemoryNotes = append(l.MemoryNotes, "long_term notes not proposed: run disputed or not completed")
		}
		return nil
	}
	scope, scopeID := "organization", ""
	if service != "" {
		scope, scopeID = "service", service
	}
	status := "CANDIDATE"
	if quality == "VERIFIED" {
		status = "VALIDATING"
	}
	for _, n := range rm.LongTerm {
		if n.Deleted {
			l.MemoryNotes = append(l.MemoryNotes, "agent asked to retire team memory note "+n.Key+" (left to reviewers)")
			continue
		}
		prepared, err := c.Writer.Prepare(ctx, NewMemory{TenantID: run.TenantID, Scope: scope, ScopeID: scopeID,
			Type: "semantic", Status: status, Subject: NoteSubject(n.Key), Title: n.Key,
			Content: n.Value, SourceRefs: []string{source}, Trust: "T3", CreatedBy: "agent", Shards: shards})
		if errors.Is(err, ErrPoisoned) {
			l.MemoryNotes = append(l.MemoryNotes, "long_term note not stored: "+err.Error())
			continue
		}
		if err != nil {
			return err
		}
		if _, _, err := c.Store.CreateMemory(ctx, prepared); err != nil {
			return err
		}
	}
	return nil
}

// applyPersonal writes one user note: create, reinforce, revise or archive
// the user's memory for that key.
func (c *Consolidator) applyPersonal(ctx context.Context, tenantID, user string, n domain.Note, source string) (string, error) {
	subject := PersonalSubject(n.Key)
	up, canUpdate := c.Store.(Updater)
	var cur Memory
	found := false
	if canUpdate {
		var err error
		if cur, found, err = up.LiveMemoryBySubject(ctx, tenantID, "user", user, subject); err != nil {
			return "", err
		}
	}
	actor := "agent:" + source
	if n.Deleted {
		if !found {
			return "", nil
		}
		from, to := cur.Status, "ARCHIVED"
		if from != "ACTIVE" {
			to = "REJECTED"
		}
		if _, err := up.TransitionMemory(ctx, tenantID, cur.ID, from, to, actor, "the user asked to forget it"); err != nil &&
			!errors.Is(err, domain.ErrVersionConflict) {
			return "", err
		}
		return "personal memory " + n.Key + " forgotten", nil
	}
	if found && cur.Content != n.Value {
		prepared, err := c.Writer.PrepareRevision(ctx, cur, n.Key, n.Value, nil, source)
		if errors.Is(err, ErrPoisoned) {
			return "personal memory not updated: " + err.Error(), nil
		}
		if err != nil {
			return "", err
		}
		if _, err := up.UpdateMemory(ctx, Revision{TenantID: tenantID, ID: cur.ID, ExpectedVersion: cur.Version,
			Prepared: prepared, Actor: actor, Reason: "the user restated a preference"}); err != nil &&
			!errors.Is(err, domain.ErrVersionConflict) {
			return "", err
		}
		return fmt.Sprintf("personal memory %s revised to v%d", n.Key, cur.Version+1), nil
	}
	// New, or the same value again (reinforced by CreateMemory's dedup).
	prepared, err := c.Writer.Prepare(ctx, NewMemory{TenantID: tenantID, Scope: "user", ScopeID: user, Type: "semantic",
		Status: "ACTIVE", Subject: subject, Title: n.Key, Content: n.Value, SourceRefs: []string{source}, Trust: "T3",
		Sensitivity: "restricted", CreatedBy: actor})
	if errors.Is(err, ErrPoisoned) {
		return "personal memory not stored: " + err.Error(), nil
	}
	if err != nil {
		return "", err
	}
	_, created, err := c.Store.CreateMemory(ctx, prepared)
	if err != nil {
		return "", err
	}
	if created {
		return "personal memory " + n.Key + " saved", nil
	}
	return "", nil
}
