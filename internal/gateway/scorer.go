package gateway

// Scorer ranks the endpoints that passed the hard routing filters (§30.2);
// the highest score is tried first. It is the route_policy plugin kind.
// Scores must be deterministic for the same input so routing decisions are
// reproducible from the recorded route_reason.
type Scorer interface {
	Name() string
	Score(in ScoreInput) float64
}

// ScoreInput is everything known about one candidate endpoint.
type ScoreInput struct {
	Request  Request
	Scope    Scope
	Endpoint Endpoint
	// Rank is the endpoint's position in the alias list (0 = preferred).
	Rank          int
	TokenEstimate int64
	// RecentFailures is the circuit breaker's consecutive failure count.
	RecentFailures int
}

// PriorityScorer is the default: endpoint priority, then alias order, then
// weight, with a penalty for recent failures.
type PriorityScorer struct{}

func (PriorityScorer) Name() string { return "priority" }

func (PriorityScorer) Score(in ScoreInput) float64 {
	return float64(1000-in.Endpoint.Priority*100-in.Rank*10) + in.Endpoint.Weight - float64(in.RecentFailures)*5
}

// WeightedScorer combines priority, alias order, expected cost, recent
// failures and context-window headroom with configurable weights. Every
// term is "higher is better" after its sign.
type WeightedScorer struct {
	Priority float64 `json:"priority"` // per priority level (lower priority value wins)
	Rank     float64 `json:"rank"`     // per position in the alias list
	CostUSD  float64 `json:"cost_usd"` // per expected USD of the request
	Failures float64 `json:"failures"` // per recent consecutive failure
	Headroom float64 `json:"headroom"` // × free fraction of the context window
	Weight   float64 `json:"weight"`   // × endpoint.Weight
}

// DefaultWeightedScorer reproduces PriorityScorer's ordering and adds a cost term.
func DefaultWeightedScorer() WeightedScorer {
	return WeightedScorer{Priority: 100, Rank: 10, CostUSD: 50, Failures: 5, Headroom: 0, Weight: 1}
}

func (WeightedScorer) Name() string { return "weighted" }

func (w WeightedScorer) Score(in ScoreInput) float64 {
	e := in.Endpoint
	cost := float64(e.Pricing.CostMicros(Usage{InputTokens: in.TokenEstimate, OutputTokens: in.Request.MaxOutputTokens})) / 1e6
	headroom := 0.0
	if e.ContextWindow > 0 {
		headroom = 1 - float64(in.TokenEstimate+in.Request.MaxOutputTokens)/float64(e.ContextWindow)
	}
	return 1000 - w.Priority*float64(e.Priority) - w.Rank*float64(in.Rank) - w.CostUSD*cost -
		w.Failures*float64(in.RecentFailures) + w.Headroom*headroom + w.Weight*e.Weight
}
