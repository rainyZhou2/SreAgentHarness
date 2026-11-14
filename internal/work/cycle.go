package work

import (
	"bytes"
	"encoding/json"
	"time"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/outcome"
)

// ValidateCycle protects the atomic outcome-to-wait transition.
func ValidateCycle(w Work, m Mutation, now time.Time) error {
	idx, ok := w.Template.Index(w.Template.RepeatFrom)
	if w.Template.RepeatFrom == "" || !ok || w.Template.Stages[idx].Type != StageWait || w.Goal.Success == nil ||
		w.Status != StatusVerifying || w.Stage < len(w.Template.Stages)-1 || m.ExpectedVersion != w.Version || m.LeaseOwner == "" ||
		m.Status == nil || *m.Status != StatusWaiting || m.Stage == nil || *m.Stage != idx ||
		m.Generation == nil || *m.Generation != w.Generation+1 || m.Goal != nil ||
		m.Outcome == nil || m.Outcome.Generation != w.Generation || m.Outcome.Verdict != outcome.Satisfied ||
		!m.CancelAllWatches || !m.ClearState || len(m.Watches) != 1 || m.AddRuns != 0 || m.AddReplans != 0 || m.AddCost != 0 {
		return invalidf("invalid cycle completion")
	}
	wt := m.Watches[0]
	if wt.WorkID != w.ID || wt.TenantID != w.TenantID || wt.Generation != w.Generation+1 || wt.ID == "" ||
		wt.CreatedAt.IsZero() || wt.CreatedAt.After(now) || now.Sub(wt.CreatedAt) > time.Minute || w.Template.Stages[idx].Wait == nil {
		return invalidf("cycle watch belongs to another owner or generation")
	}
	expected, err := continuous.Build(*w.Template.Stages[idx].Wait, w.TenantID, w.ID, wt.Generation, w.Goal.SubjectRefs, wt.CreatedAt)
	if err != nil {
		return invalidf("invalid cycle wait: %v", err)
	}
	expected.ID = wt.ID
	actualJSON, err := json.Marshal(wt)
	if err != nil {
		return invalidf("invalid cycle watch: %v", err)
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil || !bytes.Equal(actualJSON, expectedJSON) {
		return invalidf("cycle watch does not match the next wait")
	}
	var state struct {
		StageID    string    `json:"stage_id"`
		Generation int       `json:"generation"`
		WatchID    string    `json:"watch_id"`
		StartedAt  time.Time `json:"started_at"`
	}
	if json.Unmarshal(m.StageState, &state) != nil || state.StageID != w.Template.RepeatFrom ||
		state.Generation != wt.Generation || state.WatchID != wt.ID || !state.StartedAt.Equal(wt.CreatedAt) {
		return invalidf("cycle stage state does not reference the next watch")
	}
	_, err = PrepareOutcome(w, *m.Outcome, now, true)
	return err
}
