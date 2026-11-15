package incident

import (
	"fmt"

	"go-sre-harness/internal/work"
)

// PostmortemStatus describes drafting and human publication independently.
type PostmortemStatus string

const (
	PostmortemNotStarted     PostmortemStatus = "NOT_STARTED"
	PostmortemDrafting       PostmortemStatus = "DRAFTING"
	PostmortemReviewRequired PostmortemStatus = "REVIEW_REQUIRED"
	PostmortemPublished      PostmortemStatus = "PUBLISHED"
)

// PostmortemView points to the reviewed Postmortem Work.
type PostmortemView struct {
	Status     PostmortemStatus `json:"status"`
	WorkID     string           `json:"work_id,omitempty"`
	WorkStatus work.Status      `json:"work_status,omitempty"`
	Stage      string           `json:"stage,omitempty"`
}

func projectPostmortem(incident work.Work, postmortems []work.Work) (PostmortemView, error) {
	if len(postmortems) > 200 {
		return PostmortemView{}, fmt.Errorf("incident %s has more than 200 postmortem records", incident.ID)
	}
	for _, candidate := range postmortems {
		if candidate.Template.ID != work.TemplatePostmortem || candidate.Goal.Labels["source_work"] != incident.ID {
			continue
		}
		view := PostmortemView{Status: PostmortemDrafting, WorkID: candidate.ID, WorkStatus: candidate.Status}
		for i, stage := range candidate.Template.Stages {
			if stage.ID != "review" || stage.Type != work.StageConfirm {
				continue
			}
			view.Stage = stage.ID
			switch {
			case candidate.Status == work.StatusSucceeded && candidate.Stage > i:
				view.Status = PostmortemPublished
			case !candidate.Status.Terminal() && candidate.Stage == i:
				view.Status = PostmortemReviewRequired
			}
			break
		}
		return view, nil
	}
	return PostmortemView{Status: PostmortemNotStarted}, nil
}
