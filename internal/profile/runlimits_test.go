package profile

import (
	"encoding/json"
	"testing"
)

func TestRunLimits(t *testing.T) {
	s, err := Parse(json.RawMessage(`{"strategy":"llm","tools":["*"],"run_limits":{"max_cost_usd":0.5,"max_tokens":200000,"max_repeats":0,"stall_steps":3,"max_parallel_tools":2}}`))
	if err != nil {
		t.Fatal(err)
	}
	r, err := Compile("p@1", s)
	if err != nil {
		t.Fatal(err)
	}
	if l := r.RunLimits; l == nil || l.MaxCostUSD != 0.5 || l.MaxTokens != 200000 || l.MaxRepeats == nil || *l.MaxRepeats != 0 ||
		l.StallSteps != 3 || l.MaxParallelTools != 2 {
		t.Fatalf("limits: %+v", r.RunLimits)
	}
	for _, bad := range []string{`{"max_cost_usd":-1}`, `{"max_repeats":11}`, `{"max_parallel_tools":17}`, `{"stall_steps":51}`, `{"max_tokens":-5}`} {
		s, err := Parse(json.RawMessage(`{"strategy":"llm","tools":["*"],"run_limits":` + bad + `}`))
		if err == nil {
			_, err = Compile("p@1", s)
		}
		if err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}
