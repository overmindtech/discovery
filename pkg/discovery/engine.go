package discovery

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/dylanratcliffe/sdp-go"
	"github.com/dylanratcliffe/source-go/pkg/sdpcache"

	"github.com/nats-io/nats.go"

	log "github.com/sirupsen/logrus"
)

type NATSOptions struct {
	// The list of URLs to use for connecting to NATS
	URLs []string

	// The name given to the connection, useful in logging
	ConnectionName string

	// How long to wait when trying to connect to each NATS server
	ConnectTimeout time.Duration

	// How many times to retry if there was an error when connecting
	NumRetries int

	// Path to the customer CA file to use when using TLS (if required)
	CAFile string

	// Path to the NKey seed file
	NkeyFile string

	// Path to the JWT
	JWTFile string

	// The name of the queue to join when subscribing to subjects
	QueueName string
}

// Engine is the main discovery engine. This is where all of the Sources and
// sources are stored and is responsible for calling out to the right sources to
// discover everything
//
// Note that an engine that does not have a connected NATS connection will
// simply not communicate over NATS
type Engine struct {
	// Descriptive name of this engine. Used as responder name in SDP responses
	Name string

	// Options for connecting to NATS
	NATSOptions *NATSOptions

	// The maximum number of queries that can be executing in parallel. Defaults
	// to the number of CPUs
	MaxParallelExecutions int

	// Internal throttle used to limit MaxParallelExecutions
	throttle Throttle

	// Cache that is used for storing SDP items in memory
	cache sdpcache.Cache

	// The NATS connection
	natsConnection *nats.EncodedConn

	// List of all current subscriptions
	subscriptions []*nats.Subscription

	// Map of types to all sources for that type
	sourceMap map[string][]Source

	// GetFindMutex used for locking
	gfm GetFindMutex
}

// AddSources Adds a source to this engine
func (e *Engine) AddSources(sources ...Source) {
	if e.sourceMap == nil {
		e.sourceMap = make(map[string][]Source)
	}

	for _, src := range sources {
		allSources := append(e.sourceMap[src.Type()], src)

		sort.Slice(allSources, func(i, j int) bool {
			iSource := allSources[i]
			jSource := allSources[j]

			// Sort by weight, highest first
			return iSource.Weight() > jSource.Weight()
		})

		e.sourceMap[src.Type()] = append(e.sourceMap[src.Type()], src)
	}
}

// Sources Returns a slice of all known sources
func (e *Engine) Sources() []Source {
	sources := make([]Source, 0)

	for _, typeSources := range e.sourceMap {
		sources = append(sources, typeSources...)
	}

	return sources
}

// Connect Connects to NATS
func (e *Engine) Connect() error {
	// Try to connect to NATS
	if no := e.NATSOptions; no != nil {
		var tries int
		var servers string

		// Register our custom encoder
		nats.RegisterEncoder("sdp", &sdp.ENCODER)

		// Create server list as comme separated
		servers = strings.Join(no.URLs, ",")

		// Configure options
		options := []nats.Option{
			nats.Name(no.ConnectionName),
			nats.Timeout(no.ConnectTimeout),
		}

		if no.CAFile != "" {
			options = append(options, nats.RootCAs(no.CAFile))
		}

		if no.NkeyFile != "" && no.JWTFile != "" {
			options = append(options, nats.UserCredentials(no.JWTFile, no.NkeyFile))
		}

		// Loop until we have a connection
		for tries <= no.NumRetries {
			log.WithFields(log.Fields{
				"servers": servers,
			}).Info("Connecting to NATS")

			// TODO: Make these options more configurable
			// https://docs.nats.io/developing-with-nats/connecting/pingpong
			nc, err := nats.Connect(
				servers,
				options...,
			)

			if err == nil {
				var enc *nats.EncodedConn

				enc, err = nats.NewEncodedConn(nc, "sdp")

				if err != nil {
					return err
				}

				e.natsConnection = enc

				log.WithFields(log.Fields{
					"Addr":     e.natsConnection.Conn.ConnectedAddr(),
					"ServerID": e.natsConnection.Conn.ConnectedServerId(),
					"URL:":     e.natsConnection.Conn.ConnectedUrl(),
				}).Info("NATS connected")

				return nil
			}

			// Increment tries
			tries++

			log.WithFields(log.Fields{
				"servers": servers,
				"err":     err,
			}).Info("Connection failed")

			// TODO: Add a configurable backoff here
			time.Sleep(5 * time.Second)
		}

		return fmt.Errorf("could not connect after %v tries", tries)
	}

	return errors.New("no NATSOptions struct provided")
}

// Start performs all of the initialisation steps required for the engine to
// work. Note that this creates NATS subscriptions for all available sources so
// modifying the Sources value after an engine has been started will not have
// any effect until the engine is restarted
func (e *Engine) Start() error {
	var subscription *nats.Subscription
	var err error

	e.setDefaultMaxParallelExecutions()
	e.throttle = Throttle{
		NumParallel: e.MaxParallelExecutions,
	}

	// Start purging cache
	e.cache.StartPurger()

	subject := "request.all"

	log.WithFields(log.Fields{
		"queueName":  e.NATSOptions.QueueName,
		"subject":    subject,
		"engineName": e.Name,
	}).Debug("creating NATS subscription")

	// Subscribe to "requests.all" since this is relevant for all sources
	if e.NATSOptions.QueueName == "" {
		subscription, err = e.natsConnection.Subscribe(subject, e.NewItemRequestHandler(e.Sources()))
	} else {
		subscription, err = e.natsConnection.QueueSubscribe(subject, e.NATSOptions.QueueName, e.NewItemRequestHandler(e.Sources()))
	}

	if err != nil {
		return err
	}

	e.subscriptions = append(e.subscriptions, subscription)

	// Now group sources by the subscriptions they require based on their
	// supported contexts. Each key represnets a subscription, and the value is
	// the list of sources that are capable of responding to requests on that
	// subscription
	subscriptionMap := make(map[string][]Source)

	for _, src := range e.Sources() {
		for _, itemContext := range src.Contexts() {
			var subject string

			if itemContext == AllContexts {
				subject = "request.context.>"
			} else {
				subject = fmt.Sprintf("request.context.%v", itemContext)
			}

			if sources, ok := subscriptionMap[subject]; ok {
				subscriptionMap[subject] = append(sources, src)
			} else {
				subscriptionMap[subject] = []Source{src}
			}
		}
	}

	// Now actually create the required subscriptions
	for subject, sources := range subscriptionMap {
		log.WithFields(log.Fields{
			"queueName":  e.NATSOptions.QueueName,
			"subject":    subject,
			"engineName": e.Name,
		}).Debug("creating NATS subscription")

		// Subscribe to "requests.all" since this is relevant for all sources
		if e.NATSOptions.QueueName == "" {
			subscription, err = e.natsConnection.Subscribe(subject, e.NewItemRequestHandler(sources))
		} else {
			subscription, err = e.natsConnection.QueueSubscribe(subject, e.NATSOptions.QueueName, e.NewItemRequestHandler(sources))
		}

		if err != nil {
			return err
		}

		e.subscriptions = append(e.subscriptions, subscription)
	}

	return nil
}

// Stop Stops the engine running, draining all connections
func (e *Engine) Stop() error {
	for _, c := range e.subscriptions {
		err := c.Drain()

		if err != nil {
			return err
		}
	}

	return nil
}

// IsNATSConnected returns whether the engine is connected to NATS
func (e *Engine) IsNATSConnected() bool {
	if enc := e.natsConnection; enc != nil {
		if conn := enc.Conn; conn != nil {
			return conn.IsConnected()
		}
		return false
	}
	return false
}

// setDefaultMaxParallelExecutions Sets MaxParallelExecutions to the number of
// CPUs if not already set
func (e *Engine) setDefaultMaxParallelExecutions() {
	if e.MaxParallelExecutions == 0 {
		e.MaxParallelExecutions = runtime.NumCPU()
	}
}

// IsWildcard checks if a string is the wildcard. Use this instead of
// implementing the wildcard check everwhere so that if we need to change the
// woldcard at a later date we can do so here
func IsWildcard(s string) bool {
	return s == "*"
}
