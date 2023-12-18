package discovery

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"

	"github.com/getsentry/sentry-go"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName    = "github.com/overmindtech/discovery/discovery"
	instrumentationVersion = "0.0.1"
)

var (
	tracer = otel.GetTracerProvider().Tracer(
		instrumentationName,
		trace.WithInstrumentationVersion(instrumentationVersion),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
)

// LogRecoverToReturn Recovers from a panic, logs and forwards it sentry and otel, then returns
// Does nothing when there is no panic.
func LogRecoverToReturn(ctx context.Context, loc string) {
	err := recover()
	if err == nil {
		return
	}

	stack := string(debug.Stack())
	handleError(ctx, loc, err, stack)
}

// LogRecoverToExit Recovers from a panic, logs and forwards it sentry and otel, then exits
// Does nothing when there is no panic.
func LogRecoverToExit(ctx context.Context, loc string) {
	err := recover()
	if err == nil {
		return
	}

	stack := string(debug.Stack())
	handleError(ctx, loc, err, stack)

	os.Exit(1)
}

func handleError(ctx context.Context, loc string, err interface{}, stack string) {
	msg := fmt.Sprintf("unhandled panic in %v, exiting: %v", loc, err)

	sentry.CurrentHub().Recover(err)

	log.WithFields(log.Fields{"loc": loc, "stack": stack}).Error(msg)

	if ctx != nil {
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(attribute.String("ovm.panic.loc", loc))
		span.SetAttributes(attribute.String("ovm.panic.stack", stack))
	}
}
