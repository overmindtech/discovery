package discovery

import (
	"log"
	"os"
	"testing"

	"github.com/overmindtech/discovery/tracing"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
)

func TestMain(m *testing.M) {
	key, _ := os.LookupEnv("HONEYCOMB_API_KEY")
	opts := make([]otlptracehttp.Option, 0)
	if key != "" {
		opts = []otlptracehttp.Option{
			otlptracehttp.WithEndpoint("api.honeycomb.io"),
			otlptracehttp.WithHeaders(map[string]string{"x-honeycomb-team": key}),
		}
	}

	if err := tracing.InitTracer(opts...); err != nil {
		log.Fatal(err)
	}

	exitCode := m.Run()

	tracing.ShutdownTracer()

	os.Exit(exitCode)
}
