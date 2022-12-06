module github.com/overmindtech/discovery

go 1.19

// Direct dependencies of my codebase
require (
	github.com/google/uuid v1.3.0
	github.com/goombaio/namegenerator v0.0.0-20181006234301-989e774b106e
	github.com/nats-io/nats-server/v2 v2.9.8
	github.com/nats-io/nats.go v1.21.0
	github.com/overmindtech/connect v0.5.0
	github.com/overmindtech/sdp-go v0.13.7
	github.com/overmindtech/sdpcache v0.3.2
	github.com/sirupsen/logrus v1.9.0
	google.golang.org/protobuf v1.28.1
)

// Transitive dependencies
require (
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/klauspost/compress v1.15.12 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/nats-io/jwt/v2 v2.3.0 // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/overmindtech/api-client v0.4.2 // indirect
	golang.org/x/crypto v0.3.0 // indirect
	golang.org/x/net v0.3.0 // indirect
	golang.org/x/oauth2 v0.3.0 // indirect
	golang.org/x/sys v0.3.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
