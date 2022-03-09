module github.com/overmindtech/discovery

go 1.17

// Direct dependencies of my codebase
require (
	github.com/google/uuid v1.3.0
	github.com/goombaio/namegenerator v0.0.0-20181006234301-989e774b106e
	github.com/nats-io/nats-server/v2 v2.7.3
	github.com/nats-io/nats.go v1.13.1-0.20220121202836-972a071d373d
	github.com/overmindtech/sdp-go v0.6.1
	github.com/overmindtech/sdpcache v0.3.1
	github.com/sirupsen/logrus v1.8.1
	google.golang.org/protobuf v1.27.1
)

// Transitive dependencies
require (
	github.com/google/go-cmp v0.5.7 // indirect
	github.com/klauspost/compress v1.15.0 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/nats-io/jwt/v2 v2.2.1-0.20220113022732-58e87895b296 // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	golang.org/x/crypto v0.0.0-20220307211146-efcb8507fb70 // indirect
	golang.org/x/sys v0.0.0-20220307203707-22a9840ba4d7 // indirect
	golang.org/x/time v0.0.0-20220224211638-0e9765cccd65 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)
