// Package harness is the public entry point for building a harness binary.
// A custom binary imports the plugin packages it wants and calls Main:
//
//	import (
//		"go-sre-harness/pkg/harness"
//		_ "go-sre-harness/plugins/all"      // built-ins
//		_ "example.com/myorg/harness-slack" // your plugins
//	)
//
//	func main() { harness.Main() }
package harness

import "go-sre-harness/internal/app"

// Main parses the mode from os.Args and runs the harness.
func Main() { app.Main() }
