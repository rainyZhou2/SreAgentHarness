// Package telemetry registers the built-in telemetry exporters.
package telemetry

import (
	"context"

	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/plugin"
)

type OTLPConfig struct {
	// ServiceName defaults to go-sre-harness-<mode>. The exporter itself is
	// configured with the standard OTEL_EXPORTER_OTLP_* variables; without
	// an endpoint tracing stays disabled.
	ServiceName string `json:"service_name,omitempty"`
}

type shutdown func(context.Context) error

func (s shutdown) Shutdown(ctx context.Context) error { return s(ctx) }

func init() {
	plugin.Register(plugin.Telemetry, "otlp", "OpenTelemetry traces over OTLP/HTTP",
		func(ctx context.Context, h *plugin.Host, c plugin.Config) (plugin.TelemetryExporter, error) {
			cfg := OTLPConfig{ServiceName: "go-sre-harness-" + h.Mode}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			stop, enabled, err := obs.SetupTracing(ctx, cfg.ServiceName)
			if err != nil {
				return nil, err
			}
			h.Log.Info("tracing", "enabled", enabled, "service", cfg.ServiceName)
			return shutdown(stop), nil
		})
}
