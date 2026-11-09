package work

import "strings"

// Queue is the scheduling class of a Work (RFC §20.2). Each queue has its
// own claim loop, concurrency and model-run budget, so a flood of routine or
// background Works never delays an incident mitigation. A queue only orders
// and isolates work: it never relaxes policy, approval or risk rules.
type Queue string

const (
	// QueueCritical is for mitigations; only an authenticated event source
	// (a correlation rule) or a human operator may put a Work here.
	QueueCritical   Queue = "critical-mitigation"
	QueueIncident   Queue = "incident-investigate"
	QueueRoutine    Queue = "routine-work"
	QueueBatch      Queue = "batch-evaluation"
	QueueBackground Queue = "background-improvement"
)

// Queues lists the queues from the most to the least urgent.
var Queues = []Queue{QueueCritical, QueueIncident, QueueRoutine, QueueBatch, QueueBackground}

// ValidQueue reports whether q is a known queue.
func ValidQueue(q Queue) bool { return q.Rank() >= 0 }

// Rank is the queue's urgency (0 = most urgent, -1 = unknown).
func (q Queue) Rank() int {
	for i, x := range Queues {
		if x == q {
			return i
		}
	}
	return -1
}

// DefaultQueue is the queue of a Work that did not ask for one: the
// template's queue, else by kind (incidents investigate, the rest is
// routine work).
func DefaultQueue(kind Kind, tpl Template) Queue {
	if tpl.Queue != "" {
		return tpl.Queue
	}
	switch kind {
	case KindIncident, KindAIInfra:
		return QueueIncident
	}
	return QueueRoutine
}

// MayUseQueue reports whether an actor may place a Work in q. The critical
// queue is reserved for authenticated event sources (actor id "event:...")
// and humans (actor type "user", already authorized as operators by the
// API); Works derived by the system itself (follow-ups, playbook sweeps,
// model proposals) cannot claim it.
func MayUseQueue(q Queue, actorType, actorID string) bool {
	if q != QueueCritical {
		return true
	}
	return actorType == "user" || strings.HasPrefix(actorID, "event:")
}
