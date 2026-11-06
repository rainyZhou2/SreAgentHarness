// Command custom-harness shows how to build a harness binary with extra
// plugins: import the built-ins (or a subset) plus your own packages.
//
//	HARNESS_CONFIG=examples/custom-harness/harness.yaml go run ./examples/custom-harness all
package main

import (
	"go-sre-harness/pkg/harness"
	_ "go-sre-harness/plugins/all"

	_ "go-sre-harness/examples/plugins/hello"
)

func main() { harness.Main() }
