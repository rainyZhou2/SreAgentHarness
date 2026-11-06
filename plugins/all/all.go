// Package all compiles in every built-in plugin. A custom binary may import
// a subset instead, plus its own plugin packages.
package all

import (
	_ "go-sre-harness/plugins/apiext"
	_ "go-sre-harness/plugins/authn"
	_ "go-sre-harness/plugins/eventsinks"
	_ "go-sre-harness/plugins/governance"
	_ "go-sre-harness/plugins/memory"
	_ "go-sre-harness/plugins/messaging"
	_ "go-sre-harness/plugins/models"
	_ "go-sre-harness/plugins/profiles"
	_ "go-sre-harness/plugins/relay"
	_ "go-sre-harness/plugins/sretools"
	_ "go-sre-harness/plugins/storage"
	_ "go-sre-harness/plugins/strategies"
	_ "go-sre-harness/plugins/telemetry"
)
