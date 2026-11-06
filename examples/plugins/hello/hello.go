// Package hello is an example plugin written only against the public SDK.
// It contributes a read-only tool, a stricter policy, and an API route.
//
// Enable it in the harness config:
//
//	plugins:
//	  tools:  [demo, {name: hello, config: {greeting: "Hi"}}]
//	  policy: {name: freeze_window, config: {deny: ["demo.restart"]}}
//	  api:    [memory, eval, hello]
package hello

import (
	"context"
	"encoding/json"
	"net/http"

	"go-sre-harness/pkg/sdk"
)

type ToolConfig struct {
	Greeting string `json:"greeting,omitempty"`
}

type FreezeConfig struct {
	Deny []string `json:"deny"`
}

// freezeWindow denies listed tools and defers everything else to the
// built-in policy. The framework floor (R2 approval, R3 deny) still applies.
type freezeWindow struct {
	sdk.Gate
	deny map[string]bool
}

func (f freezeWindow) Evaluate(desc *sdk.ToolDescriptor, target string) sdk.PolicyDecision {
	d := f.Gate.Evaluate(desc, target)
	if desc != nil && f.deny[desc.Name] {
		d.Verdict, d.Reason = sdk.Deny, "change freeze: "+desc.Name+" is disabled"
	}
	return d
}

type helloAPI struct{ greeting string }

func (a helloAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/example/hello", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"message": a.greeting})
	})
}

// RouteRoles: GET routes default to viewer; writes must be declared.
func (helloAPI) RouteRoles() map[string]sdk.Role { return nil }

func init() {
	sdk.Register(sdk.Tools, "hello", "example.hello: greets a service (R0)",
		func(_ context.Context, _ *sdk.Host, c sdk.Config) (sdk.ToolSource, error) {
			cfg := ToolConfig{Greeting: "Hello"}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return sdk.StaticTools(&sdk.FuncTool{
				Desc: sdk.ToolDescriptor{Name: "example.hello", Version: "1", Risk: sdk.RiskR0, Trust: "T2", Source: "plugin:hello",
					Description: "Returns a greeting for a service.",
					InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"}},"required":["service"]}`)},
				Fn: func(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
					var in struct{ Service string }
					if err := json.Unmarshal(args, &in); err != nil {
						return nil, err
					}
					return json.Marshal(map[string]string{"message": cfg.Greeting + ", " + in.Service})
				},
			}), nil
		})
	sdk.Register(sdk.Policy, "freeze_window", "built-in policy plus a deny list (change freeze)",
		func(_ context.Context, _ *sdk.Host, c sdk.Config) (sdk.Gate, error) {
			var cfg FreezeConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			deny := map[string]bool{}
			for _, n := range cfg.Deny {
				deny[n] = true
			}
			return freezeWindow{Gate: sdk.DefaultPolicy(), deny: deny}, nil
		})
	sdk.Register(sdk.API, "hello", "GET /v1/example/hello",
		func(context.Context, *sdk.Host, sdk.Config) (sdk.APIExtension, error) {
			return helloAPI{greeting: "hello from a plugin"}, nil
		})
}
