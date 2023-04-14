module github.com/overmindtech/discovery

go 1.20

// Direct dependencies of my codebase
require (
	github.com/getsentry/sentry-go v0.20.0
	github.com/google/uuid v1.3.0
	github.com/goombaio/namegenerator v0.0.0-20181006234301-989e774b106e
	github.com/nats-io/nats-server/v2 v2.9.15
	github.com/nats-io/nats.go v1.25.0
	github.com/overmindtech/connect v0.9.0
	github.com/overmindtech/sdp-go v0.26.0
	github.com/overmindtech/sdpcache v1.3.0
	github.com/sirupsen/logrus v1.9.0
	github.com/sourcegraph/conc v0.3.0 // use latest `main` to avoid heavy dependency chain
	go.opentelemetry.io/otel v1.14.0
	go.opentelemetry.io/otel/trace v1.14.0
	google.golang.org/protobuf v1.30.0
)

// Transitive dependencies
require (
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/klauspost/compress v1.16.0 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/nats-io/jwt/v2 v2.4.1 // indirect
	github.com/nats-io/nkeys v0.4.4 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/overmindtech/api-client v0.14.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.40.0 // indirect
	go.opentelemetry.io/otel/metric v0.37.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.8.0 // indirect
	golang.org/x/net v0.9.0 // indirect
	golang.org/x/oauth2 v0.6.0 // indirect
	golang.org/x/sys v0.7.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
)

require (
	github.com/auth0/go-jwt-middleware/v2 v2.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	gopkg.in/square/go-jose.v2 v2.6.0 // indirect
)
