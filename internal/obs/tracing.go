package obs

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Tracer is the harness tracer. Without SetupTracing it is a no-op.
func Tracer() trace.Tracer { return otel.Tracer("go-sre-harness") }

// SetupTracing installs an OTLP/HTTP exporter when OTEL_EXPORTER_OTLP_ENDPOINT
// (or ..._TRACES_ENDPOINT) is set; the exporter reads the standard OTEL_* env.
// Prompts and tool outputs are never span attributes, only refs and hashes.
func SetupTracing(ctx context.Context, service string) (func(context.Context) error, bool, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, false, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, false, err
	}
	res, _ := resource.Merge(resource.Default(), resource.NewSchemaless(attribute.String("service.name", service)))
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, true, nil
}

// TraceID returns the current trace ID (for audit rows), or "".
func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}
