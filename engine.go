package discovery

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdpcache"

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

	// MaxReconnect sets the number of reconnect attempts that will be
	// tried before giving up. If negative, then it will never give up
	// trying to reconnect.
	MaxReconnect int

	// ReconnectWait sets the time to backoff after attempting a reconnect
	// to a server that we were already connected to previously.
	ReconnectWait time.Duration

	// ReconnectJitter sets the upper bound for a random delay added to
	// ReconnectWait during a reconnect when no TLS is used.
	// Note that any jitter is capped with ReconnectJitterMax.
	ReconnectJitter time.Duration

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

	// How often to check for closed connections and try to recover
	ConnectionWatchInterval time.Duration
	ConnectionWatcher       NATSWatcher

	// List of all current subscriptions
	subscriptions []*nats.Subscription

	// Map of types to all sources for that type
	sourceMap map[string][]Source

	// Storage for triggers
	triggers      []*Trigger
	triggersMutex sync.RWMutex

	// GetFindMutex used for locking
	gfm GetFindMutex

	// trackedRequests is used for storing requests that have a UUID so they can
	// be cancelled if required
	trackedRequests      map[uuid.UUID]*RequestTracker
	trackedRequestsMutex sync.RWMutex

	restartMutex sync.Mutex
}

// TrackRequest Stores a RequestTracker in the engine so that it can be looked
// up later and cancelled if required. The UUID should be supplied as part of
// the request itself
func (e *Engine) TrackRequest(uuid uuid.UUID, request *RequestTracker) {
	e.ensureTrackedRequests()
	e.trackedRequestsMutex.Lock()
	defer e.trackedRequestsMutex.Unlock()
	e.trackedRequests[uuid] = request
}

// GetTrackedRequest Returns the RequestTracked object for a given UUID. THis
// tracker can then be used to cancel the request
func (e *Engine) GetTrackedRequest(uuid uuid.UUID) (*RequestTracker, error) {
	e.ensureTrackedRequests()
	e.trackedRequestsMutex.RLock()
	defer e.trackedRequestsMutex.RUnlock()

	if tracker, ok := e.trackedRequests[uuid]; ok {
		return tracker, nil
	} else {
		return nil, fmt.Errorf("tracker with UUID %x not found", uuid)
	}
}

// DeleteTrackedRequest Deletes a request from tracking
func (e *Engine) DeleteTrackedRequest(uuid [16]byte) {
	e.ensureTrackedRequests()
	e.trackedRequestsMutex.Lock()
	defer e.trackedRequestsMutex.Unlock()
	delete(e.trackedRequests, uuid)
}

// ensureTrackedRequests Makes sure the trackedRequests map has been created
func (e *Engine) ensureTrackedRequests() {
	e.trackedRequestsMutex.Lock()
	defer e.trackedRequestsMutex.Unlock()
	if e.trackedRequests == nil {
		e.trackedRequests = make(map[uuid.UUID]*RequestTracker)
	}
}

// SetupThrottle Sets up the throttling based on MaxParallelExecutions,
// including ensuring that it's not set to zero
func (e *Engine) SetupThrottle() {
	if e.MaxParallelExecutions == 0 {
		e.MaxParallelExecutions = runtime.NumCPU()
	}

	e.throttle = Throttle{
		NumParallel: e.MaxParallelExecutions,
	}
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

		e.sourceMap[src.Type()] = allSources
	}
}

// AddTriggers Adds a trigger to this engine. Triggers cause the engine to
// listen for items from other contexts and will fire a custom ItemRequest if
// they match
func (e *Engine) AddTriggers(triggers ...Trigger) {
	e.triggersMutex.Lock()
	defer e.triggersMutex.Unlock()

	if e.triggers == nil {
		e.triggers = make([]*Trigger, 0)
	}

	for _, trigger := range triggers {
		e.triggers = append(e.triggers, &trigger)
	}
}

// ClearTriggers removes all triggers from the engine
func (e *Engine) ClearTriggers() {
	e.triggersMutex.Lock()
	defer e.triggersMutex.Unlock()

	e.triggers = make([]*Trigger, 0)
}

// ProcessTriggers Checks all triggers against a given item and fires them if
// required
func (e *Engine) ProcessTriggers(item *sdp.Item) {
	e.triggersMutex.RLock()
	defer e.triggersMutex.RUnlock()

	wg := sync.WaitGroup{}

	wg.Add(len(e.triggers))

	for _, trigger := range e.triggers {
		go func(t *Trigger) {
			defer wg.Done()

			// Check to see if the trigger should fire
			req, err := t.ProcessItem(item)

			if err != nil {
				return
			}

			// Fire the trigger and send the request to the engine
			e.ItemRequestHandler(req)
		}(trigger)
	}

	// Wait for all to complete so that we know what we have running
	wg.Wait()
}

// Sources Returns a slice of all known sources
func (e *Engine) Sources() []Source {
	sources := make([]Source, 0)

	for _, typeSources := range e.sourceMap {
		sources = append(sources, typeSources...)
	}

	return sources
}

// NonHiddenSources Returns a slice of all known sources excliding hidden ones
func (e *Engine) NonHiddenSources() []Source {
	allSources := e.Sources()
	nonHiddenSources := make([]Source, 0)

	// Add all sources unless they are hidden
	for _, source := range allSources {
		if hs, ok := source.(HiddenSource); ok {
			if hs.Hidden() {
				// If the source is hidden, continue without adding it
				continue
			}
		}

		nonHiddenSources = append(nonHiddenSources, source)
	}

	return nonHiddenSources
}

// Connect Connects to NATS
func (e *Engine) Connect() error {
	// Try to connect to NATS
	if no := e.NATSOptions; no != nil {
		var servers string

		// Register our custom encoder
		nats.RegisterEncoder("sdp", &sdp.ENCODER)

		// Create server list as comme separated
		servers = strings.Join(no.URLs, ",")

		// Configure options
		options := []nats.Option{
			nats.Name(no.ConnectionName),
			nats.Timeout(no.ConnectTimeout),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(no.MaxReconnect),
			nats.DisconnectErrHandler(func(c *nats.Conn, e error) {
				log.WithFields(log.Fields{
					"error":   e,
					"address": c.ConnectedAddr(),
				}).Error("NATS disconnected")
			}),
			nats.ReconnectHandler(func(c *nats.Conn) {
				log.WithFields(log.Fields{
					"reconnects": c.Reconnects,
					"ServerID":   c.ConnectedServerId(),
					"URL:":       c.ConnectedUrl(),
				}).Info("NATS reconnected")
			}),
			nats.ClosedHandler(func(c *nats.Conn) {
				log.WithFields(log.Fields{
					"error": c.LastError(),
				}).Info("NATS connection closed")
			}),
			nats.LameDuckModeHandler(func(c *nats.Conn) {
				log.WithFields(log.Fields{
					"address": c.ConnectedAddr(),
				}).Info("NATS server has entered lame duck mode")
			}),
			nats.ErrorHandler(func(c *nats.Conn, s *nats.Subscription, e error) {
				log.WithFields(log.Fields{
					"error":   e,
					"address": c.ConnectedAddr(),
					"subject": s.Subject,
					"queue":   s.Queue,
				}).Error("NATS error")
			}),
		}

		if no.ReconnectWait > 0 {
			options = append(options, nats.ReconnectWait(no.ReconnectWait))
		}

		if no.ReconnectJitter > 0 {
			options = append(options, nats.ReconnectJitter(no.ReconnectJitter, no.ReconnectJitter))
		}

		if no.CAFile != "" {
			options = append(options, nats.RootCAs(no.CAFile))
		}

		if no.NkeyFile != "" && no.JWTFile != "" {
			options = append(options, nats.UserCredentials(no.JWTFile, no.NkeyFile))
		}

		log.WithFields(log.Fields{
			"servers": servers,
		}).Info("NATS connecting")

		nc, err := nats.Connect(
			servers,
			options...,
		)

		if err != nil {
			return err
		}

		e.ConnectionWatcher = NATSWatcher{
			Connection: nc,
			FailureHandler: func() {
				// Restart the engine on a failure
				go e.Restart()
			},
		}
		e.ConnectionWatcher.Start(e.ConnectionWatchInterval)

		// Wait for the connection to be completed
		err = nc.FlushTimeout(10 * time.Minute)

		if err != nil {
			return err
		}

		var enc *nats.EncodedConn

		enc, err = nats.NewEncodedConn(nc, "sdp")

		if err != nil {
			return err
		}

		e.natsConnection = enc

		log.WithFields(log.Fields{
			"ServerID": e.natsConnection.Conn.ConnectedServerId(),
			"URL:":     e.natsConnection.Conn.ConnectedUrl(),
		}).Info("NATS connected")

		return nil
	}

	return errors.New("no NATSOptions struct provided")
}

// Start performs all of the initialisation steps required for the engine to
// work. Note that this creates NATS subscriptions for all available sources so
// modifying the Sources value after an engine has been started will not have
// any effect until the engine is restarted
func (e *Engine) Start() error {
	e.SetupThrottle()

	// Start purging cache
	e.cache.StartPurger()

	var err error

	err = e.Subscribe("request.all", e.ItemRequestHandler)

	if err != nil {
		return err
	}

	err = e.Subscribe("cancel.all", e.CancelItemRequestHandler)

	if err != nil {
		return err
	}

	if len(e.triggers) > 0 {
		err = e.Subscribe("return.item.>", e.ProcessTriggers)

		if err != nil {
			return err
		}
	}

	// Loop over all sources and work out what subscriptions we need to make
	// depending on what contexts they support. These context names are then
	// stored in a map for de-duplication before being subscribed to
	subscriptionMap := make(map[string]bool)

	// We need to track if we are making a wildcard subscription. If we are then
	// there isn't any point making 10 subscriptions since they will be covered
	// by the wildcard anyway and will end up being duplicates. In that case we
	// should just be making the one
	var wildcardExists bool

	for _, src := range e.Sources() {
		for _, itemContext := range src.Contexts() {
			if itemContext == sdp.WILDCARD {
				wildcardExists = true
			} else {
				subscriptionMap[itemContext] = true
			}
		}
	}

	// Now actually create the required subscriptions
	if wildcardExists {
		e.Subscribe("request.context.>", e.ItemRequestHandler)
		e.Subscribe("cancel.context.>", e.CancelItemRequestHandler)
	} else {
		for suffix := range subscriptionMap {
			e.Subscribe(fmt.Sprintf("request.context.%v", suffix), e.ItemRequestHandler)
			e.Subscribe(fmt.Sprintf("cancel.context.%v", suffix), e.CancelItemRequestHandler)
		}
	}

	return nil
}

func (e *Engine) Subscribe(subject string, handler nats.Handler) error {
	var subscription *nats.Subscription
	var err error

	if e.natsConnection == nil {
		return errors.New("cannot subscribe. NATS connection is nil")
	}

	log.WithFields(log.Fields{
		"queueName":  e.NATSOptions.QueueName,
		"subject":    subject,
		"engineName": e.Name,
	}).Debug("creating NATS subscription")

	if e.NATSOptions.QueueName == "" {
		subscription, err = e.natsConnection.Subscribe(subject, handler)
	} else {
		subscription, err = e.natsConnection.QueueSubscribe(subject, e.NATSOptions.QueueName, handler)
	}

	if err != nil {
		return err
	}

	e.subscriptions = append(e.subscriptions, subscription)

	return nil
}

// Stop Stops the engine running and disconnects from NATS
func (e *Engine) Stop() error {
	for _, c := range e.subscriptions {
		err := c.Drain()

		if err != nil {
			return err
		}
	}

	// Clear the cache
	e.cache.Clear()
	e.cache.StopPurger()

	e.ConnectionWatcher.Stop()
	if e.natsConnection != nil {
		e.natsConnection.Close()
	}

	return nil
}

// Restart Restarts the engine. If called in parallel, subsequent calls are
// ignored until the restart is completed
func (e *Engine) Restart() error {
	e.restartMutex.Lock()
	defer e.restartMutex.Unlock()

	err := e.Stop()

	if err != nil {
		return err
	}

	err = e.Connect()

	if err != nil {
		return err
	}

	err = e.Start()

	return err
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

// CancelItemRequestHandler Takes a CancelItemRequest and cancels that request if it exists
func (e *Engine) CancelItemRequestHandler(cancelRequest *sdp.CancelItemRequest) {
	u, err := uuid.FromBytes(cancelRequest.UUID)

	if err != nil {
		log.Errorf("Error parsing UUID for cancel request: %v", err)
		return
	}

	var rt *RequestTracker

	rt, err = e.GetTrackedRequest(u)

	if err != nil {
		log.Debugf("Could not find tracked request %v. Possibly is has already finished", u.String())
		return
	}

	if rt != nil {
		log.WithFields(log.Fields{
			"UUID": u.String(),
		}).Debug("Cancelling request")
		rt.Cancel()
	}
}

// ClearCache Completely clears the cache
func (e *Engine) ClearCache() {
	e.cache.Clear()
}

// IsWildcard checks if a string is the wildcard. Use this instead of
// implementing the wildcard check everwhere so that if we need to change the
// woldcard at a later date we can do so here
func IsWildcard(s string) bool {
	return s == sdp.WILDCARD
}
