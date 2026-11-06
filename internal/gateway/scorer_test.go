package gateway_test

import (
	"testing"
	"time"

	gw "go-sre-harness/internal/gateway"
)

type reverse struct{}

func (reverse) Name() string                   { return "reverse" }
func (reverse) Score(in gw.ScoreInput) float64 { return float64(in.Rank) }

func TestScorerIsPluggableButHardFiltersStay(t *testing.T) {
	p := policy()
	cands, err := p.Route(req(), sc(), 100, nil, nil, time.Now())
	if err != nil || cands[0].Endpoint.ID != "a" || cands[0].Reason["scorer"] != "priority" {
		t.Fatalf("default: %v %+v", err, cands)
	}
	cands, _ = p.Route(req(), sc(), 100, nil, reverse{}, time.Now())
	if cands[0].Endpoint.ID != "b" || cands[0].Reason["scorer"] != "reverse" {
		t.Fatalf("custom scorer ignored: %+v", cands)
	}
	// A scorer cannot bring back an endpoint a hard filter removed.
	p.Endpoints[1].Tenants = []string{"other"}
	cands, _ = p.Route(req(), sc(), 100, nil, reverse{}, time.Now())
	if len(cands) != 1 || cands[0].Endpoint.ID != "a" {
		t.Fatalf("hard filter bypassed: %+v", cands)
	}
}

func TestWeightedScorerPrefersCheaperEndpoint(t *testing.T) {
	p := policy()
	p.Endpoints[0].Pricing.OutputPerMTok = 200 // a: same priority class, much more expensive
	p.Endpoints[1].Priority = 0
	w := gw.DefaultWeightedScorer()
	w.CostUSD = 1000
	cands, err := p.Route(req(), sc(), 1000, nil, w, time.Now())
	if err != nil || cands[0].Endpoint.ID != "b" {
		t.Fatalf("weighted: %v %+v", err, cands)
	}
	// The gateway uses the configured scorer for resolution.
	g := gw.New(p, gw.NewMemLedger(), gw.NewMemArtifacts(), gw.Admission{}, nil)
	g.SetScorer(w)
	if ep, err := g.Resolve(req(), sc()); err != nil || ep.ID != "b" {
		t.Fatalf("resolve: %v %s", err, ep.ID)
	}
}
