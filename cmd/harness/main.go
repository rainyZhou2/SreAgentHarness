// Command harness runs the Go SRE Agent Harness with every built-in plugin
// compiled in. See internal/app for modes and README.md for configuration.
package main

import (
	"go-sre-harness/pkg/harness"
	_ "go-sre-harness/plugins/all"
)

func main() { harness.Main() }
