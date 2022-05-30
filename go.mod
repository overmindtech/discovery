module github.com/overmindtech/discovery

go 1.17

// Direct dependencies of my codebase
require (
	github.com/google/uuid v1.3.0
	github.com/goombaio/namegenerator v0.0.0-20181006234301-989e774b106e
	github.com/nats-io/nats-server/v2 v2.8.4
	github.com/nats-io/nats.go v1.16.0
	github.com/overmindtech/multiconn v0.3.1
	github.com/overmindtech/sdp-go v0.9.1
	github.com/overmindtech/sdpcache v0.3.1
	github.com/sirupsen/logrus v1.8.1
	google.golang.org/protobuf v1.28.0
)

// Transitive dependencies
require (
	github.com/dgraph-io/dgo/v210 v210.0.0-20220113041351-ba0e5dfc4c3e // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/klauspost/compress v1.15.3 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/nats-io/jwt/v2 v2.2.1-0.20220330180145-442af02fd36a // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/overmindtech/tokenx-client v0.1.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stretchr/testify v1.7.1 // indirect
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/net v0.0.0-20220526153639-5463443f8c37 // indirect
	golang.org/x/oauth2 v0.0.0-20220524215830-622c5d57e401 // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20220411224347-583f2d630306 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220527130721-00d5c0f3be58 // indirect
	google.golang.org/grpc v1.46.2 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)
