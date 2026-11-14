package orchestration

import (
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Work orchestration owns the Runs port; the composition layer owns adapters
// to the transport API. Keeping this boundary explicit prevents a second
// execution API from becoming part of Work's domain implementation.
func TestOrchestrationDoesNotImportRunTransport(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".go") || strings.HasSuffix(file.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), file.Name(), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range f.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if path == "go-sre-harness/internal/api" {
				t.Errorf("%s imports run transport; adapters belong in workengine", file.Name())
			}
		}
	}
}
