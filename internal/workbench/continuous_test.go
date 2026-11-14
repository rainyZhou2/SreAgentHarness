package workbench

import (
	"context"
	"testing"
)

func TestSGLangContinuousWork(t *testing.T) {
	cases, err := LoadCases("testdata/sglang-continuous.json")
	if err != nil {
		t.Fatal(err)
	}
	res, err := (&Runner{}).Run(context.Background(), cases[0])
	if err != nil {
		t.Fatal(err)
	}
	if !res.Passed {
		t.Fatalf("failures=%v result=%+v", res.Failures, res)
	}
	if len(res.Unused) != 0 {
		t.Fatalf("scenario did not consume all inputs: %v", res.Unused)
	}
	bad := cases[0]
	bad.Expect.Dispatches = map[string]int{"serving.scale_prefill": 1}
	check(bad, &res)
	if res.Passed {
		t.Fatal("exact dispatch assertion accepted a duplicate effect")
	}
}
