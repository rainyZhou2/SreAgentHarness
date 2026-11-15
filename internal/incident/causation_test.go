package incident

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go-sre-harness/internal/work"
)

func TestCausationProjection(t *testing.T) {
	source := incidentWork()
	secondary := source
	secondary.ID, secondary.Status, secondary.CreatedAt = "secondary", work.StatusActive, testAt
	store := &fakeStore{secondary: []work.SecondaryIncident{{Work: secondary, ActionID: "act", MitigationWorkID: "mit"}}}
	got := projectForTest(t, source, store)
	if got.CausedBy != nil || len(got.SecondaryIncidents) != 1 || got.SecondaryIncidentsTruncated {
		t.Fatalf("source causation=%+v", got)
	}
	item := got.SecondaryIncidents[0]
	if item.WorkID != secondary.ID || item.Status != secondary.Status || !item.CreatedAt.Equal(testAt) ||
		item.ActionID != "act" || item.MitigationWorkID != "mit" || item.Relation != RelationMitigationCaused {
		t.Fatalf("secondary projection=%+v", item)
	}
	store.subjects = map[string][]work.SubjectLink{secondary.ID: {
		{Kind: work.SubjectCausedByWork, ID: source.ID},
		{Kind: work.SubjectCausedByAction, ID: "act"},
	}}
	reverse := projectForTest(t, secondary, store)
	if reverse.CausedBy == nil || reverse.CausedBy.IncidentID != source.ID || reverse.CausedBy.ActionID != "act" ||
		reverse.CausedBy.Relation != RelationMitigationCaused {
		t.Fatalf("caused-by projection=%+v", reverse.CausedBy)
	}
	empty := projectForTest(t, source, &fakeStore{})
	if empty.CausedBy != nil || empty.SecondaryIncidents == nil || len(empty.SecondaryIncidents) != 0 {
		t.Fatalf("empty causation=%+v", empty)
	}
}

func TestCausationProjectionBoundsResults(t *testing.T) {
	store := &fakeStore{}
	for i := 0; i < 101; i++ {
		secondary := incidentWork()
		secondary.ID = fmt.Sprintf("secondary-%03d", i)
		store.secondary = append(store.secondary, work.SecondaryIncident{Work: secondary, ActionID: "act", MitigationWorkID: "mit"})
	}
	got := projectForTest(t, incidentWork(), store)
	if len(got.SecondaryIncidents) != 100 || !got.SecondaryIncidentsTruncated || got.SecondaryIncidents[99].WorkID != "secondary-099" {
		t.Fatalf("bounded causation len=%d truncated=%v", len(got.SecondaryIncidents), got.SecondaryIncidentsTruncated)
	}
}

func TestCausationProjectionRejectsBrokenEvidence(t *testing.T) {
	w := incidentWork()
	store := &fakeStore{subjects: map[string][]work.SubjectLink{w.ID: {{Kind: work.SubjectCausedByWork, ID: "source"}}}}
	if _, err := Project(context.Background(), store, w, testNow); !errors.Is(err, work.ErrInvalid) {
		t.Fatalf("single causal link accepted: %v", err)
	}
	sentinel := errors.New("causal store failed")
	for _, store := range []*fakeStore{{subjectsErr: sentinel}, {secondaryErr: sentinel}} {
		if _, err := Project(context.Background(), store, w, testNow); !errors.Is(err, sentinel) {
			t.Fatalf("causal store error hidden: %v", err)
		}
	}
}
