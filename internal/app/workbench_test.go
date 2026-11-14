package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go-sre-harness/internal/workbench"
)

func TestWorkBenchCLI(t *testing.T) {
	if _, err := os.Stat("../../bench/cases"); err != nil {
		t.Skip("bench/cases is not present (kept local, not in the repository)")
	}
	ctx := context.Background()
	env := func(string) string { return "" }
	dir := t.TempDir()
	cases, err := workbench.LoadCases("../../bench/cases")
	if err != nil {
		t.Fatal(err)
	}
	out, base := filepath.Join(dir, "out.json"), filepath.Join(dir, "base.json")
	var so, se bytes.Buffer
	if code := WorkBench(ctx, []string{"run", "../../bench/cases", "-out", out, "-write-baseline", base}, env, &so, &se); code != 0 {
		t.Fatalf("run: %d\n%s", code, se.String())
	}
	if !strings.Contains(se.String(), fmt.Sprintf("%d passed, 0 failed", len(cases))) {
		t.Fatalf("stderr: %s", se.String())
	}
	var suite struct {
		Passed int `json:"cases_passed"`
	}
	b, _ := os.ReadFile(out)
	if json.Unmarshal(b, &suite) != nil || suite.Passed != len(cases) {
		t.Fatalf("output: %s", b)
	}
	if code := WorkBench(ctx, []string{"run", "../../bench/cases", "-baseline", base, "-out", filepath.Join(dir, "o2.json")}, env, &so, &se); code != 0 {
		t.Fatalf("baseline gate against itself: %d", code)
	}
	// A failing case exits 1.
	bad := filepath.Join(dir, "bad.json")
	c, _ := os.ReadFile("../../bench/cases/01-rollback-verified.json")
	_ = os.WriteFile(bad, bytes.Replace(c, []byte(`"status": "SUCCEEDED"`), []byte(`"status": "FAILED"`), 1), 0o600)
	se.Reset()
	if code := WorkBench(ctx, []string{"run", bad, "-out", filepath.Join(dir, "o3.json")}, env, &so, &se); code != 1 || !strings.Contains(se.String(), "FAIL") {
		t.Fatalf("failing case: %d %s", code, se.String())
	}
	for _, args := range [][]string{nil, {"run"}, {"record", "-tenant", "t"}, {"bogus"}} {
		if code := WorkBench(ctx, args, env, &so, &se); code != 2 {
			t.Errorf("%v: %d", args, code)
		}
	}
	if code := WorkBench(ctx, []string{"replay", "-tenant", "t", "-work", "w"}, env, &so, &se); code != 2 {
		t.Errorf("replay without a database: %d", code)
	}
}
