module github.com/overmindtech/discovery

go 1.17

// Direct dependencies of my codebase
require (
	github.com/goombaio/namegenerator v0.0.0-20181006234301-989e774b106e
	github.com/nats-io/nats.go v1.13.1-0.20211018182449-f2416a8b1483
	github.com/overmindtech/sdp-go v0.3.0
	github.com/overmindtech/sdpcache v0.1.4
	github.com/sirupsen/logrus v1.8.1
	google.golang.org/protobuf v1.27.1
)

// Transitive dependencies
require (
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/nats-io/nats-server/v2 v2.6.5 // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	golang.org/x/crypto v0.0.0-20211117183948-ae814b36b871 // indirect
	golang.org/x/sys v0.0.0-20211117180635-dee7805ff2e1 // indirect
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect

)
