module github.com/overmindtech/discovery

go 1.22.1

// Direct dependencies of my codebase
require (
	github.com/MrAlias/otel-schema-utils v0.2.1-alpha
	github.com/getsentry/sentry-go v0.27.0
	github.com/google/uuid v1.6.0
	github.com/goombaio/namegenerator v0.0.0-20181006234301-989e774b106e
	github.com/nats-io/nats-server/v2 v2.10.12
	github.com/nats-io/nats.go v1.34.0
	github.com/overmindtech/sdp-go v0.68.0
	github.com/overmindtech/sdpcache v1.6.4
	github.com/sirupsen/logrus v1.9.3
	github.com/sourcegraph/conc v0.3.0
	github.com/spf13/viper v1.18.2
	go.opentelemetry.io/contrib/detectors/aws/ec2 v1.24.0
	go.opentelemetry.io/otel v1.24.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.24.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.24.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.24.0
	go.opentelemetry.io/otel/sdk v1.24.0
	go.opentelemetry.io/otel/trace v1.24.0
	golang.org/x/oauth2 v0.18.0
	google.golang.org/protobuf v1.33.0
)

// Transitive dependencies
require (
	connectrpc.com/connect v1.15.0 // indirect
	github.com/Masterminds/semver/v3 v3.2.1 // indirect
	github.com/auth0/go-jwt-middleware/v2 v2.2.1 // indirect
	github.com/aws/aws-sdk-go v1.50.15 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-jose/go-jose/v4 v4.0.1 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.19.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.17.7 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/nats-io/jwt/v2 v2.5.5 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/overmindtech/api-client v0.14.0 // indirect
	github.com/pelletier/go-toml/v2 v2.1.0 // indirect
	github.com/sagikazarmark/locafero v0.4.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/cast v1.6.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/schema v0.0.7 // indirect
	go.opentelemetry.io/proto/otlp v1.1.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.21.0 // indirect
	golang.org/x/exp v0.0.0-20231206192017-f3f8817b8deb // indirect
	golang.org/x/net v0.22.0 // indirect
	golang.org/x/sync v0.6.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240102182953-50ed04b92917 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240102182953-50ed04b92917 // indirect
	google.golang.org/grpc v1.61.1 // indirect
	gopkg.in/go-jose/go-jose.v2 v2.6.3 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
