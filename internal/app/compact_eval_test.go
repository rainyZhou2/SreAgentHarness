package app

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestCompactEvalWithoutModelSaysSo(t *testing.T) {
	for _, k := range []string{"ANTHROPIC_API_KEY", "GEMINI_API_KEY", "HARNESS_OPENAI_BASE_URL"} {
		t.Setenv(k, "")
	}
	var out, errb bytes.Buffer
	if code := RunCompactEval(context.Background(), []string{"-data", t.TempDir()}, &out, &errb); code != 0 {
		t.Fatalf("exit %d: %s %s", code, out.String(), errb.String())
	}
	s := out.String()
	for _, want := range []string{"deterministic", "NOT run", "n/a", "model input"} {
		if !strings.Contains(s, want) {
			t.Errorf("output missing %q:\n%s", want, s)
		}
	}
}

func TestCompactEvalJSON(t *testing.T) {
	for _, k := range []string{"ANTHROPIC_API_KEY", "GEMINI_API_KEY", "HARNESS_OPENAI_BASE_URL"} {
		t.Setenv(k, "")
	}
	var out bytes.Buffer
	if code := RunCompactEval(context.Background(), []string{"-json", "-data", t.TempDir()}, &out, &bytes.Buffer{}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(out.String(), `"model_configured": false`) {
		t.Fatalf("%s", out.String())
	}
}
