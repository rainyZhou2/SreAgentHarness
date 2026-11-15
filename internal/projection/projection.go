// Package projection turns persisted run state into ordered, read-only UI blocks.
package projection

import (
	"encoding/json"
	"sort"
	"strconv"
	"time"

	"go-sre-harness/internal/domain"
)

const (
	SchemaVersion     = 1
	ProjectionVersion = 1
)

type View struct {
	SchemaVersion        int      `json:"schema_version"`
	ProjectionVersion    int      `json:"projection_version"`
	MinimumClientVersion string   `json:"minimum_client_version,omitempty"`
	Capabilities         []string `json:"capabilities,omitempty"`
	RunID                string   `json:"run_id"`
	RunVersion           int64    `json:"run_version"`
	SessionID            string   `json:"session_id,omitempty"`
	TurnID               string   `json:"turn_id,omitempty"`
	Status               string   `json:"status"`
	WorkIDs              []string `json:"work_ids,omitempty"`
	Blocks               []Block  `json:"blocks"`
}

type Block struct {
	ID      string
	Kind    string
	Ordinal int
	Value   any
}

func (b Block) MarshalJSON() ([]byte, error) {
	out := map[string]any{"id": b.ID, "kind": b.Kind, "ordinal": b.Ordinal}
	if b.Value != nil {
		raw, err := json.Marshal(b.Value)
		if err != nil {
			return nil, err
		}
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, err
		}
		for k, v := range fields {
			out[k] = v
		}
	}
	return json.Marshal(out)
}

func (b *Block) UnmarshalJSON(data []byte) error {
	var header struct {
		ID      string `json:"id"`
		Kind    string `json:"kind"`
		Ordinal int    `json:"ordinal"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	delete(fields, "id")
	delete(fields, "kind")
	delete(fields, "ordinal")
	b.ID, b.Kind, b.Ordinal = header.ID, header.Kind, header.Ordinal
	if len(fields) == 0 {
		b.Value = nil
		return nil
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	b.Value = json.RawMessage(raw)
	return nil
}

type Cost struct {
	ModelRequests int64   `json:"model_requests"`
	InputTokens   int64   `json:"input_tokens"`
	OutputTokens  int64   `json:"output_tokens"`
	CostUSD       float64 `json:"cost_usd"`
}

type StatusBlock struct {
	Status     string     `json:"status"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Cost       Cost       `json:"cost"`
}

type ReasoningBlock struct {
	Summary   string `json:"summary"`
	Truncated bool   `json:"truncated"`
}

type MarkdownBlock struct {
	Markdown string `json:"markdown"`
}

type ToolGroupBlock struct {
	StepIDs     []string `json:"step_ids"`
	StepCount   int      `json:"step_count"`
	FailedCount int      `json:"failed_count"`
}

type Window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type Quality struct {
	Partial  bool `json:"partial"`
	Stale    bool `json:"stale"`
	Conflict bool `json:"conflict"`
}

type EvidenceBlock struct {
	EvidenceRef string  `json:"evidence_ref,omitempty"`
	DataRef     string  `json:"data_ref,omitempty"`
	Signal      string  `json:"signal"`
	Source      string  `json:"source"`
	Status      string  `json:"status"`
	Window      Window  `json:"window"`
	Quality     Quality `json:"quality"`
	Summary     string  `json:"summary,omitempty"`
}

type HypothesisItem struct {
	ID        string   `json:"id"`
	Statement string   `json:"statement"`
	Status    string   `json:"status"`
	Supports  []string `json:"supports,omitempty"`
	Refutes   []string `json:"refutes,omitempty"`
}

type HypothesisBlock struct {
	WorkID string           `json:"work_id,omitempty"`
	Items  []HypothesisItem `json:"items"`
}

type ConclusionBlock struct {
	Verdict      string   `json:"verdict"`
	Answer       string   `json:"answer"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
	Unverified   []string `json:"unverified,omitempty"`
}

type WorkBlock struct {
	WorkID    string `json:"work_id"`
	Status    string `json:"status"`
	Stage     string `json:"stage,omitempty"`
	WaitingOn string `json:"waiting_on,omitempty"`
}

type ActionBlock struct {
	ActionID          string     `json:"action_id"`
	ApprovalID        string     `json:"approval_id,omitempty"`
	State             string     `json:"state"`
	Risk              string     `json:"risk"`
	Target            string     `json:"target"`
	IntentHash        string     `json:"intent_hash"`
	RequiredApprovals int        `json:"required_approvals"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	MayHaveDispatched bool       `json:"may_have_dispatched"`
	Controls          []string   `json:"controls"`
}

type AttachmentsBlock struct {
	Items []domain.Attachment `json:"items"`
}

type UnknownBlock struct {
	RawKind string `json:"raw_kind"`
}

type Input struct {
	Run        domain.Run
	Steps      []domain.Step
	Thoughts   map[string][]domain.Thought
	StepOutput map[string]json.RawMessage
	Approvals  []domain.ApprovalRequest
	Cost       Cost
	Result     *domain.Decision
	// CanApprove hides action controls from principals without the approver role.
	CanApprove bool
}

func Build(in Input) View {
	view := View{
		SchemaVersion:     SchemaVersion,
		ProjectionVersion: ProjectionVersion,
		RunID:             in.Run.ID,
		RunVersion:        in.Run.Version,
		Status:            in.Run.DisplayStatus(),
		Blocks:            make([]Block, 0, len(in.Steps)+4),
	}
	if in.Run.SessionID != nil {
		view.SessionID = *in.Run.SessionID
	}
	if in.Run.TurnID != nil {
		view.TurnID = *in.Run.TurnID
	}
	if in.Run.WorkOwner != nil && in.Run.WorkOwner.WorkID != "" {
		view.WorkIDs = []string{in.Run.WorkOwner.WorkID}
	}
	var finished *time.Time
	if in.Run.Status.Terminal() && !in.Run.UpdatedAt.IsZero() {
		at := in.Run.UpdatedAt
		finished = &at
	}
	view.Blocks = append(view.Blocks, Block{
		ID:   "run:" + in.Run.ID + ":status",
		Kind: "status",
		Value: StatusBlock{
			Status:     in.Run.DisplayStatus(),
			StartedAt:  in.Run.CreatedAt,
			FinishedAt: finished,
			Cost:       in.Cost,
		},
	})

	steps := append([]domain.Step(nil), in.Steps...)
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].Seq < steps[j].Seq })
	var conclusion *domain.Decision
	for _, step := range steps {
		raw := in.StepOutput[step.ID]
		obs, decision := decodeOutput(raw)
		switch step.Kind {
		case domain.StepReason:
			if decision == nil {
				continue
			}
			if decision.Kind == domain.DecisionFinal && decision.Answer != "" {
				conclusion = decision
			}
			if summary := reasonSummary(decision, in.Thoughts[step.ID]); summary != "" {
				view.Blocks = append(view.Blocks, Block{
					ID:   "run:" + in.Run.ID + ":reason:" + step.ID,
					Kind: "reasoning",
					Value: ReasoningBlock{
						Summary: summary,
					},
				})
			}
		case domain.StepTool:
			view.Blocks = append(view.Blocks, toolGroup(in.Run.ID, step))
			view.Blocks = append(view.Blocks, evidence(in.Run.ID, step, obs)...)
		}
	}
	if conclusion == nil {
		conclusion = in.Result
	}
	if conclusion != nil {
		if block, ok := hypothesisBlock(in, conclusion); ok {
			view.Blocks = append(view.Blocks, block)
		}
		if block, ok := conclusionBlock(in, conclusion); ok {
			view.Blocks = append(view.Blocks, block)
		}
	}
	for _, approval := range in.Approvals {
		if approval.Status != "PENDING" {
			continue
		}
		expires := approval.ExpiresAt
		controls := []string{}
		if in.CanApprove {
			controls = []string{"approve", "reject"}
		}
		view.Blocks = append(view.Blocks, Block{
			ID:   "run:" + in.Run.ID + ":action:" + approval.ID,
			Kind: "action",
			Value: ActionBlock{
				ActionID:          approval.ActionIntentID,
				ApprovalID:        approval.ID,
				State:             "WAITING_APPROVAL",
				Risk:              string(approval.RiskClass),
				Target:            approval.Target,
				IntentHash:        approval.ActionHash,
				RequiredApprovals: approval.RequiredApprovals,
				ExpiresAt:         &expires,
				Controls:          controls,
			},
		})
	}
	if conclusion != nil && len(conclusion.Attachments) > 0 {
		view.Blocks = append(view.Blocks, Block{
			ID:    "run:" + in.Run.ID + ":attachments",
			Kind:  "attachments",
			Value: AttachmentsBlock{Items: conclusion.Attachments},
		})
	}
	for i := range view.Blocks {
		view.Blocks[i].Ordinal = i
	}
	return view
}

func reasonSummary(d *domain.Decision, thoughts []domain.Thought) string {
	if d != nil && d.ReasonSummary != "" {
		return d.ReasonSummary
	}
	for _, thought := range thoughts {
		if thought.Summary != "" {
			return thought.Summary
		}
	}
	return ""
}

func toolGroup(runID string, step domain.Step) Block {
	failed := 0
	if step.Status == domain.StepFailed || step.Status == domain.StepCancelled {
		failed = 1
	}
	return Block{
		ID:   "run:" + runID + ":tool:" + step.ID,
		Kind: "tool-group",
		Value: ToolGroupBlock{
			StepIDs:     []string{step.ID},
			StepCount:   1,
			FailedCount: failed,
		},
	}
}

type bundleEnvelope struct {
	Bundle struct {
		Items []struct {
			Role        string    `json:"role"`
			Status      string    `json:"status"`
			Signal      string    `json:"signal"`
			SourceID    string    `json:"source_id"`
			DataRef     string    `json:"data_ref"`
			EvidenceRef string    `json:"evidence_ref"`
			Summary     string    `json:"summary"`
			Start       time.Time `json:"start"`
			End         time.Time `json:"end"`
		} `json:"items"`
	} `json:"bundle"`
}

func evidence(runID string, step domain.Step, obs *domain.Observation) []Block {
	if obs == nil || len(obs.Content) == 0 {
		return nil
	}
	var env bundleEnvelope
	if json.Unmarshal(obs.Content, &env) != nil || len(env.Bundle.Items) == 0 {
		return nil
	}
	out := make([]Block, 0, len(env.Bundle.Items))
	for i, item := range env.Bundle.Items {
		status := item.Status
		if status == "" {
			status = observationStatus(obs.Outcome)
		}
		signal := item.Signal
		if signal == "" {
			signal = "unknown"
		}
		source := item.SourceID
		if source == "" {
			source = "unknown"
		}
		out = append(out, Block{
			ID:   "run:" + runID + ":evidence:" + step.ID + ":" + item.Role + ":" + strconv.Itoa(i),
			Kind: "evidence",
			Value: EvidenceBlock{
				EvidenceRef: item.EvidenceRef,
				DataRef:     item.DataRef,
				Signal:      signal,
				Source:      source,
				Status:      status,
				Window:      Window{Start: item.Start, End: item.End},
				Quality: Quality{
					Partial:  status == "partial",
					Stale:    status == "stale",
					Conflict: status == "conflict",
				},
				Summary: item.Summary,
			},
		})
	}
	return out
}

func observationStatus(outcome string) string {
	switch outcome {
	case "ok":
		return "ok"
	case "denied", "rejected":
		return "denied"
	case "expired":
		return "stale"
	default:
		return "unavailable"
	}
}

func hypothesisBlock(in Input, d *domain.Decision) (Block, bool) {
	if d == nil || len(d.Hypotheses) == 0 {
		return Block{}, false
	}
	items := make([]HypothesisItem, 0, len(d.Hypotheses))
	for _, h := range d.Hypotheses {
		items = append(items, HypothesisItem{
			ID:        h.ID,
			Statement: h.Statement,
			Status:    h.Status,
			Supports:  h.Supports,
			Refutes:   h.Refutes,
		})
	}
	block := Block{ID: "run:" + in.Run.ID + ":hypothesis", Kind: "hypothesis", Value: HypothesisBlock{Items: items}}
	if in.Run.WorkOwner != nil {
		block.Value = HypothesisBlock{WorkID: in.Run.WorkOwner.WorkID, Items: items}
	}
	return block, true
}

func conclusionBlock(in Input, d *domain.Decision) (Block, bool) {
	if d == nil || d.Answer == "" {
		return Block{}, false
	}
	unverified := make([]string, 0, len(d.Hypotheses))
	verified, refuted := 0, 0
	for _, h := range d.Hypotheses {
		switch h.Status {
		case "VERIFIED":
			verified++
		case "REFUTED":
			refuted++
		default:
			if h.Statement != "" {
				unverified = append(unverified, h.Statement)
			}
		}
	}
	verdict := "UNVERIFIED"
	if verified > 0 {
		verdict = "VERIFIED"
	} else if refuted > 0 && len(unverified) == 0 {
		verdict = "REFUTED"
	}
	return Block{
		ID:   "run:" + in.Run.ID + ":conclusion",
		Kind: "conclusion",
		Value: ConclusionBlock{
			Verdict:      verdict,
			Answer:       d.Answer,
			EvidenceRefs: d.EvidenceRefs,
			Unverified:   unverified,
		},
	}, true
}

func decodeOutput(raw json.RawMessage) (*domain.Observation, *domain.Decision) {
	if len(raw) == 0 {
		return nil, nil
	}
	var d domain.Decision
	if json.Unmarshal(raw, &d) == nil && d.Kind != "" {
		return nil, &d
	}
	var obs domain.Observation
	if json.Unmarshal(raw, &obs) != nil || obs.Tool == "" {
		return nil, nil
	}
	if len(obs.Content) > 0 {
		var inner domain.Decision
		if json.Unmarshal(obs.Content, &inner) == nil && inner.Kind != "" {
			return &obs, &inner
		}
	}
	return &obs, nil
}
