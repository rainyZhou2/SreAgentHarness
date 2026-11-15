package cpsql

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/work"
)

func TestPostmortemsForScopesTenantAndSource(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		source, _, err := s.CreateWork(ctx, newWork("t1", cpkit.NewID()))
		if err != nil {
			t.Fatal(err)
		}
		other, _, err := s.CreateWork(ctx, newWork("t1", cpkit.NewID()))
		if err != nil {
			t.Fatal(err)
		}
		var template work.Template
		for _, candidate := range work.Builtin() {
			if candidate.ID == work.TemplatePostmortem {
				template = candidate
			}
		}
		create := func(tenant, sourceID, key string, createdAt time.Time) work.Work {
			t.Helper()
			n := newWork(tenant, key)
			n.Kind = work.KindRequest
			n.Template = template
			n.Goal.Labels = map[string]string{"source_work": sourceID}
			n.Now = createdAt
			w, _, err := s.CreateWork(ctx, n)
			if err != nil {
				t.Fatal(err)
			}
			return w
		}
		first := create("t1", source.ID, "pm-first", t0)
		second := create("t1", source.ID, "pm-second", t0.Add(time.Minute))
		create("t1", other.ID, "pm-other-source", t0.Add(2*time.Minute))
		create("t2", source.ID, "pm-other-tenant", t0.Add(3*time.Minute))

		got, err := s.PostmortemsFor(ctx, "t1", source.ID)
		if err != nil || len(got) != 2 {
			t.Fatalf("postmortems=%+v err=%v", got, err)
		}
		if got[0].ID != second.ID || got[1].ID != first.ID {
			t.Fatalf("wrong order or association: %+v", got)
		}
		if _, err := s.PostmortemsFor(ctx, "t1", ""); !errors.Is(err, work.ErrInvalid) {
			t.Fatalf("empty source accepted: %v", err)
		}
	})
}
