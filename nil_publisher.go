package discovery

import (
	"fmt"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

// When testing this library, or running without a real NATS connection, it is
// necessary to create a fake publisher rather than pass in a nil pointer. This
// is due to the fact that the NATS libraries will panic if a method is called
// on a nil pointer
type NilConnection struct{}

// Publish Logs an error rather than publishing
func (n NilConnection) Publish(subject string, v interface{}) error {
	log.WithFields(log.Fields{
		"subject": subject,
		"message": fmt.Sprint(v),
	}).Error("Could not publish NATS message due to no connection")

	return nil
}

// Subscribe Does nothing
func (n NilConnection) Subscribe(subject string, cb nats.Handler) (*nats.Subscription, error) {
	log.WithFields(log.Fields{
		"subject": subject,
	}).Error("Could not subscribe to NAT subject due to no connection")

	return nil, nil
}
