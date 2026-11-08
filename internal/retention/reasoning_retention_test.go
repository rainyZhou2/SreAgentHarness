package retention

import (
	"context"
	"testing"
	"time"

	"go-sre-harness/internal/store/pg"
)

type fakeStore struct{ reasoningCutoff time.Time }

func (f *fakeStore) DeletePublishedOutbox(context.Context, time.Time, int) (int64, error) { return 0, nil }
func (f *fakeStore) ExpiredSegments(context.Context, time.Time, int) ([]pg.ExpiredSegment, error) {
	return nil, nil
}
func (f *fakeStore) DeleteSegment(context.Context, pg.ExpiredSegment) error              { return nil }
func (f *fakeStore) ExpireMemories(context.Context, time.Time, int) (int64, error)       { return 0, nil }
func (f *fakeStore) DeleteOldAudit(context.Context, time.Time, int) (int64, error)       { return 0, nil }
func (f *fakeStore) DeleteOldReasoningScores(_ context.Context, c time.Time, _ int) (int64, error) {
	f.reasoningCutoff = c
	return 3, nil
}

func TestReasoningRetention(t *testing.T) {
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	st := &fakeStore{}
	c := &Cleaner{Store: st, Now: func() time.Time { return now }}
	r, err := c.Tick(context.Background())
	if err != nil || r.ReasoningScores != 3 || !st.reasoningCutoff.Equal(now.Add(-DefaultReasoningRetention)) {
		t.Fatalf("%+v %v %v", r, err, st.reasoningCutoff)
	}
	c = &Cleaner{Store: st, Now: func() time.Time { return now }, Cfg: Config{ReasoningRetention: 24 * time.Hour}}
	_, _ = c.Tick(context.Background())
	if !st.reasoningCutoff.Equal(now.Add(-24 * time.Hour)) {
		t.Fatal("configured retention")
	}
}
