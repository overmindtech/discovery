module github.com/overmindtech/discovery

go 1.17

// Direct dependencies of my codebase
require (
	github.com/google/uuid v1.3.0
	github.com/goombaio/namegenerator v0.0.0-20181006234301-989e774b106e
	github.com/nats-io/jwt/v2 v2.2.1-0.20220113022732-58e87895b296
	github.com/nats-io/nats-server/v2 v2.7.4
	github.com/nats-io/nats.go v1.13.1-0.20220308171302-2f2f6968e98d
	github.com/nats-io/nkeys v0.3.0
	github.com/overmindtech/sdp-go v0.6.1
	github.com/overmindtech/sdpcache v0.3.1
	github.com/overmindtech/tokenx-client v0.1.2
	github.com/sirupsen/logrus v1.8.1
	google.golang.org/protobuf v1.28.0
)

// Transitive dependencies
require (
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.7 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/stretchr/testify v1.7.1 // indirect
	golang.org/x/crypto v0.0.0-20220331220935-ae2d96664a29 // indirect
	golang.org/x/net v0.0.0-20220403103023-749bd193bc2b // indirect
	golang.org/x/oauth2 v0.0.0-20220309155454-6242fa91716a
	golang.org/x/sys v0.0.0-20220403205710-6acee93ad0eb // indirect
	golang.org/x/time v0.0.0-20220224211638-0e9765cccd65 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)
