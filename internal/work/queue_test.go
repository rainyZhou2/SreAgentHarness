package work

import (
	"errors"
	"testing"

	"go-sre-harness/internal/cpkit"
)

func TestDefaultQueue(t *testing.T) {
	tpl := Template{ID: "t", Stages: []Stage{{ID: "c", Type: StageConfirm}}}
	if q := DefaultQueue(KindIncident, tpl); q != QueueIncident {
		t.Fatalf("incident: %s", q)
	}
	if q := DefaultQueue(KindAIInfra, tpl); q != QueueIncident {
		t.Fatalf("ai_infra: %s", q)
	}
	if q := DefaultQueue(KindMonitoring, tpl); q != QueueRoutine {
		t.Fatalf("monitoring: %s", q)
	}
	tpl.Queue = QueueBatch
	if q := DefaultQueue(KindIncident, tpl); q != QueueBatch {
		t.Fatalf("template queue wins: %s", q)
	}
	for _, b := range Builtin() {
		if (b.ID == TemplatePostmortem || b.ID == TemplateFollowUp) && b.Queue != QueueBackground {
			t.Fatalf("%s should run in the background queue, got %q", b.ID, b.Queue)
		}
	}
	if QueueCritical.Rank() != 0 || QueueBackground.Rank() != 4 || Queue("x").Rank() != -1 {
		t.Fatal("rank order")
	}
}

func TestCriticalQueueIsReserved(t *testing.T) {
	base := func() NewWork {
		return NewWork{TenantID: "t", Kind: KindIncident, Title: "x", Goal: GoalSpec{Intent: "mitigate"},
			Template: Template{ID: "t", Stages: []Stage{{ID: "c", Type: StageConfirm}}}, IdempotencyKey: "k",
			Queue: QueueCritical}
	}
	n := base()
	n.ActorType, n.ActorID = "user", "oncall"
	if err := n.Prepare(); err != nil {
		t.Fatalf("operator: %v", err)
	}
	n = base()
	n.ActorType, n.ActorID = "system", "event:alertmanager"
	if err := n.Prepare(); err != nil {
		t.Fatalf("authenticated event: %v", err)
	}
	n = base()
	n.ActorType, n.ActorID = "system", "work:01abc"
	if err := n.Prepare(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a Work derived by the system must not claim the critical queue: %v", err)
	}
	n = base()
	n.Template.Queue, n.Queue = QueueCritical, ""
	n.ActorType, n.ActorID = "system", "playbook-sweep"
	if err := n.Prepare(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nor through a template default: %v", err)
	}
	n = base()
	n.Queue = "urgent"
	if err := n.Prepare(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown queue: %v", err)
	}
	m := Mutation{TenantID: "t", WorkID: "w", ActorType: "system", ActorID: "orchestrator"}
	q := QueueCritical
	m.Queue = &q
	if err := CheckMutation(&m); !errors.Is(err, ErrInvalid) {
		t.Fatalf("the orchestrator cannot escalate: %v", err)
	}
}

// Requests without a queue keep the idempotency hash they had before
// queues existed, so replays of old requests still match.
func TestRequestHashStableWithoutQueue(t *testing.T) {
	n := NewWork{Kind: KindIncident, Title: "x", Goal: GoalSpec{Intent: "mitigate"}, Template: Template{ID: "t", Version: 2}, Priority: 3}
	legacy := cpkit.MustHash(struct {
		Kind     Kind
		Title    string
		Goal     GoalSpec
		Template string
		Version  int
		Priority int
	}{n.Kind, n.Title, n.Goal, n.Template.ID, n.Template.Version, n.Priority})
	if n.RequestHash() != legacy {
		t.Fatal("hash changed for requests without a queue")
	}
	n.Queue = QueueRoutine
	if n.RequestHash() == legacy {
		t.Fatal("a requested queue is part of the request")
	}
}
