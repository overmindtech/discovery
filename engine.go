package discovery

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overmindtech/multiconn"
	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdpcache"

	"github.com/nats-io/nats.go"

	log "github.com/sirupsen/logrus"
)

// Engine is the main discovery engine. This is where all of the Sources and
// sources are stored and is responsible for calling out to the right sources to
// discover everything
//
// Note that an engine that does not have a connected NATS connection will
// simply not communicate over NATS
type Engine struct {
	// Descriptive name of this engine. Used as responder name in SDP responses
	Name string

	NATSOptions   *multiconn.NATSConnectionOptions // Options for connecting to NATS
	NATSQueueName string                           // The name of the queue to use when subscribing

	// The maximum number of queries that can be executing in parallel. Defaults
	// to the number of CPUs
	MaxParallelExecutions int

	// How often to check for closed connections and try to recover
	ConnectionWatchInterval time.Duration
	ConnectionWatcher       NATSWatcher

	// Internal throttle used to limit MaxParallelExecutions
	throttle Throttle

	// Cache that is used for storing SDP items in memory
	cache sdpcache.Cache

	// The NATS connection
	natsConnection      *nats.EncodedConn
	natsConnectionMutex sync.Mutex

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

// ManagedConnection Returns the connection that the engine is using. Note that
// the lifecycle of this connection is managed by the engine, causing it to
// disconnect will cause issues with the engine. Use Engine.Stop() instead
func (e *Engine) ManagedConnection() *nats.EncodedConn {
	e.natsConnectionMutex.Lock()
	defer e.natsConnectionMutex.Unlock()
	return e.natsConnection
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
func (e *Engine) connect() error {
	// Try to connect to NATS
	if no := e.NATSOptions; no != nil {
		enc, err := e.NATSOptions.Connect()

		if err != nil {
			return err
		}

		e.natsConnectionMutex.Lock()
		e.natsConnection = enc
		e.natsConnectionMutex.Unlock()

		e.ConnectionWatcher = NATSWatcher{
			Connection: e.natsConnection.Conn,
			FailureHandler: func() {
				go func() {
					if err := e.disconnect(); err != nil {
						log.Error(err)
					}

					if err := e.connect(); err != nil {
						log.Error(err)
					}
				}()
			},
		}
		e.ConnectionWatcher.Start(e.ConnectionWatchInterval)

		// Wait for the connection to be completed
		err = e.natsConnection.FlushTimeout(10 * time.Minute)

		if err != nil {
			return err
		}

		log.WithFields(log.Fields{
			"ServerID": e.natsConnection.Conn.ConnectedServerId(),
			"URL:":     e.natsConnection.Conn.ConnectedUrl(),
		}).Info("NATS connected")

		err = e.subscribe("request.all", e.ItemRequestHandler)

		if err != nil {
			return err
		}

		err = e.subscribe("cancel.all", e.CancelHandler)

		if err != nil {
			return err
		}

		if len(e.triggers) > 0 {
			err = e.subscribe("return.item.>", e.ProcessTriggers)

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
			e.subscribe("request.context.>", e.ItemRequestHandler)
			e.subscribe("cancel.context.>", e.CancelHandler)
		} else {
			for suffix := range subscriptionMap {
				e.subscribe(fmt.Sprintf("request.context.%v", suffix), e.ItemRequestHandler)
				e.subscribe(fmt.Sprintf("cancel.context.%v", suffix), e.CancelHandler)
			}
		}

		return nil
	}

	return errors.New("no NATSOptions struct provided")
}

// disconnect Disconnects the engine from the NATS network
func (e *Engine) disconnect() error {
	e.ConnectionWatcher.Stop()

	e.natsConnectionMutex.Lock()
	defer e.natsConnectionMutex.Unlock()

	if e.natsConnection != nil {
		// Only unsubscribe if the connection is not closed. If it's closed
		// there is no point
		for _, c := range e.subscriptions {
			if e.natsConnection.Conn.Status() != nats.CONNECTED {
				// If the connection is not connected we can't unsubscribe
				continue
			}

			err := c.Drain()

			if err != nil {
				return err
			}

			err = c.Unsubscribe()

			if err != nil {
				return err
			}
		}

		e.subscriptions = nil

		// Finally close the connection
		e.natsConnection.Close()
	}

	e.natsConnection = nil

	return nil
}

// Start performs all of the initialisation steps required for the engine to
// work. Note that this creates NATS subscriptions for all available sources so
// modifying the Sources value after an engine has been started will not have
// any effect until the engine is restarted
func (e *Engine) Start() error {
	e.SetupThrottle()

	// Start purging cache
	e.cache.StartPurger()

	return e.connect()
}

// subscribe Subscribes to a subject using the current NATS connection
func (e *Engine) subscribe(subject string, handler nats.Handler) error {
	var subscription *nats.Subscription
	var err error

	e.natsConnectionMutex.Lock()
	defer e.natsConnectionMutex.Unlock()

	if e.natsConnection == nil {
		return errors.New("cannot subscribe. NATS connection is nil")
	}

	log.WithFields(log.Fields{
		"queueName":  e.NATSQueueName,
		"subject":    subject,
		"engineName": e.Name,
	}).Debug("creating NATS subscription")

	if e.NATSQueueName == "" {
		subscription, err = e.natsConnection.Subscribe(subject, handler)
	} else {
		subscription, err = e.natsConnection.QueueSubscribe(subject, e.NATSQueueName, handler)
	}

	if err != nil {
		return err
	}

	e.subscriptions = append(e.subscriptions, subscription)

	return nil
}

// Stop Stops the engine running and disconnects from NATS
func (e *Engine) Stop() error {
	err := e.disconnect()

	if err != nil {
		return err
	}

	// Clear the cache
	e.cache.Clear()
	e.cache.StopPurger()

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

	err = e.Start()

	return err
}

// IsNATSConnected returns whether the engine is connected to NATS
func (e *Engine) IsNATSConnected() bool {
	e.natsConnectionMutex.Lock()
	defer e.natsConnectionMutex.Unlock()

	if enc := e.natsConnection; enc != nil {
		if conn := enc.Conn; conn != nil {
			return conn.IsConnected()
		}
		return false
	}
	return false
}

// CancelHandler calls HandleCancelItemRequest in a goroutine
func (e *Engine) CancelHandler(cancelRequest *sdp.CancelItemRequest) {
	go e.HandleCancelItemRequest(cancelRequest)
}

// HandleCancelItemRequest Takes a CancelItemRequest and cancels that request if it exists
func (e *Engine) HandleCancelItemRequest(cancelRequest *sdp.CancelItemRequest) {
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
