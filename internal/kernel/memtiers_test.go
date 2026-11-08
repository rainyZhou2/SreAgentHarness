package kernel_test

import (
	"context"
	"encoding/json"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
)

// Working notes stay with the run; session notes reach the next turn of
// the conversation through the previous turn's result.
func TestSessionAndWorkingMemoryAcrossTurns(t *testing.T) {
	ctx := context.Background()
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		switch n {
		case 0:
			d := toolDecision("metrics.a", `{}`)
			d.Notes = []domain.NoteOp{{Key: "finding", Value: "cpu high", Scope: "run"},
				{Key: "user_goal", Value: "keep checkout fast", Scope: "session"}}
			return d, nil
		case 1:
			d := finalDecision("cpu saturation")
			d.Notes = []domain.NoteOp{{Key: "answer_language", Value: "zh", Scope: "user"}}
			return d, nil
		}
		return finalDecision("second turn"), nil
	}}
	env := newLoopEnv(t, kernel.Config{}, agent, "", nil, fixedTool{name: "metrics.a", out: `{"cpu":0.95}`})
	sess, _, err := env.st.CreateSession(ctx, domain.CreateSessionRequest{TenantID: "t1", ActorID: "u1", ProfileID: "p",
		ProfileVersion: 1, Title: "s"})
	if err != nil {
		t.Fatal(err)
	}
	submit := func(key, text string) string {
		goal, _ := json.Marshal(map[string]string{"goal": text})
		ref, hash, _ := env.arts.Put(ctx, "t1", "goal", goal)
		sub, err := env.st.SubmitTurn(ctx, domain.SubmitTurnRequest{TenantID: "t1", ActorID: "u1", SessionID: sess.ID,
			ClientRequestID: key, InputRef: ref, InputHash: hash, ProfileID: "p", ProfileVersion: 1,
			InitialSteps: []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", InputRef: ref, InputHash: hash,
				Status: domain.StepPending}}}, domain.BranchQueue, 10)
		if err != nil {
			t.Fatal(err)
		}
		return sub.RunID
	}
	env.run.ID = submit("c1", "why is checkout slow")
	r := env.drive(t, 10)
	if r.Status != domain.RunCompleted {
		t.Fatalf("first turn: %s", r.Status)
	}
	if got := agent.inputs[1].Notes; len(got) != 2 || got[0].Key != "finding" || got[1].Key != "user_goal" {
		t.Fatalf("working notes on step 2: %+v", got)
	}
	res := env.result(t, r)
	mem, _ := res["memory"].(map[string]any)
	if mem == nil || len(mem["session"].([]any)) != 1 || len(mem["user"].([]any)) != 1 {
		t.Fatalf("run memory: %v", res["memory"])
	}

	env.run.ID = submit("c2", "and now?")
	if r := env.drive(t, 10); r.Status != domain.RunCompleted {
		t.Fatalf("second turn: %s", r.Status)
	}
	in := agent.inputs[2]
	if len(in.Notes) != 1 || in.Notes[0].Key != "user_goal" || in.Notes[0].Step != 0 {
		t.Fatalf("only session notes carry over: %+v", in.Notes)
	}
	if len(in.Conversation) != 2 {
		t.Fatalf("conversation: %+v", in.Conversation)
	}
}
