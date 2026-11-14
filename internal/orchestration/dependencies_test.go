package orchestration

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Check production source recursively, including build-tagged files. Tests may
// import adapters to exercise integration contracts without reversing a runtime
// dependency. Rules live in docs/architecture/PACKAGE_DEPENDENCY_RULES.md.
func TestPackageDependencyBoundaries(t *testing.T) {
	for _, pkg := range []string{"domain", "work", "evidence", "outcome", "orchestration", "kernel"} {
		t.Run(pkg, func(t *testing.T) {
			err := filepath.WalkDir(filepath.Join("..", pkg), func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
					return nil
				}
				source, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
				if err != nil {
					return err
				}
				for _, spec := range source.Imports {
					imported, err := strconv.Unquote(spec.Path.Value)
					if err != nil {
						return err
					}
					if forbiddenDependency(pkg, imported) {
						t.Errorf("%s imports forbidden dependency %q", path, imported)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDependencyRules(t *testing.T) {
	for _, tc := range []struct {
		pkg, imported string
		forbidden     bool
	}{
		{"orchestration", "go-sre-harness/internal/api", true},
		{"orchestration", "go-sre-harness/internal/kernel", true},
		{"orchestration", "go-sre-harness/internal/workapi", true},
		{"orchestration", "go-sre-harness/internal/workengine/adapters", true},
		{"orchestration", "go-sre-harness/internal/store/cpsql", false}, // documented debt
		{"domain", "database/sql/driver", true},
		{"domain", "net/http", true},
		{"work", "go-sre-harness/internal/api", true},
		{"evidence", "go-sre-harness/internal/store/pg", true},
		{"outcome", "go-sre-harness/internal/localstore", true},
		{"work", "go-sre-harness/internal/app", true},
		{"domain", "go-sre-harness/internal/localapp", true},
		{"evidence", "go-sre-harness/internal/workengine", true},
		{"outcome", "github.com/aws/aws-sdk-go-v2/service/s3", true},
		{"domain", "google.golang.org/grpc", true},
		{"work", "go-sre-harness/plugins/srework", true},
		{"domain", "go-sre-harness/pkg/harness", true},
		{"work", "go-sre-harness/internal/continuous", false},
		{"work", "go-sre-harness/internal/eventing", false},
		{"evidence", "go-sre-harness/internal/cpkit", false},
		{"domain", "encoding/json", false},
		{"kernel", "go-sre-harness/internal/work", true},
		{"kernel", "go-sre-harness/internal/orchestration", true},
		{"kernel", "go-sre-harness/internal/store/pg", true},
		{"kernel", "go-sre-harness/internal/api", true},
		{"kernel", "go-sre-harness/internal/workengine", true},
		{"kernel", "go-sre-harness/plugins/sre", true},
		{"kernel", "go-sre-harness/plugins/srework", true},
		{"kernel", "go-sre-harness/plugins/sretools", true},
		{"kernel", "go-sre-harness/internal/tools/artifactq", false},
		{"kernel", "go.opentelemetry.io/otel/trace", false},
		{"orchestration", "go-sre-harness/internal/apimachinery", false},
	} {
		t.Run(tc.pkg+"/"+tc.imported, func(t *testing.T) {
			if got := forbiddenDependency(tc.pkg, tc.imported); got != tc.forbidden {
				t.Errorf("forbidden=%v want %v", got, tc.forbidden)
			}
		})
	}
}

func forbiddenDependency(pkg, imported string) bool {
	var forbidden []string
	switch pkg {
	case "orchestration":
		forbidden = []string{"internal/kernel", "internal/api", "internal/workapi", "internal/workengine"}
	case "domain", "work", "evidence", "outcome", "kernel":
		forbidden = []string{
			"internal/api", "internal/workapi", "internal/store", "internal/localstore",
			"internal/app", "internal/localapp", "internal/workengine", "internal/orchestration",
			"plugins", "pkg/harness", "pkg/workclient", "pkg/sdk",
		}
		if pkg == "kernel" {
			forbidden = append(forbidden, "internal/work")
		} else {
			forbidden = append(forbidden, "internal/kernel")
			// Contracts currently use only standard-library and repository
			// contract imports. Keep external SDKs behind consumer-owned ports.
			root, _, _ := strings.Cut(imported, "/")
			if strings.Contains(root, ".") {
				return true
			}
		}
		if imported == "database/sql" || strings.HasPrefix(imported, "database/sql/") ||
			imported == "net/http" || strings.HasPrefix(imported, "net/http/") {
			return true
		}
	}
	for _, path := range forbidden {
		path = "go-sre-harness/" + path
		if imported == path || strings.HasPrefix(imported, path+"/") {
			return true
		}
	}
	return false
}
