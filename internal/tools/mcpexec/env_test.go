package mcpexec

import (
	"strings"
	"testing"
)

func TestStdioEnv(t *testing.T) {
	harness := []string{"PATH=/bin", "HOME=/h", "HARNESS_DATABASE_URL=postgres://secret", "OPENAI_API_KEY=sk-x", "GITHUB_TOKEN=gh"}
	env := strings.Join(StdioEnv(harness, []string{"GITHUB_TOKEN", "MODE=ro", "MISSING", "PATH=/opt/bin"}), "\n")
	for _, want := range []string{"PATH=/opt/bin", "HOME=/h", "GITHUB_TOKEN=gh", "MODE=ro"} {
		if !strings.Contains(env, want) {
			t.Fatalf("missing %s in %q", want, env)
		}
	}
	for _, bad := range []string{"HARNESS_DATABASE_URL", "OPENAI_API_KEY", "MISSING"} {
		if strings.Contains(env, bad) {
			t.Fatalf("%s leaked: %q", bad, env)
		}
	}
}
