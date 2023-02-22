package discovery

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overmindtech/connect"
	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdpcache"

	"github.com/nats-io/nats.go"

	log "github.com/sirupsen/logrus"
)

const DefaultMaxRequestTimeout = 1 * time.Minute
const DefaultConnectionWatchInterval = 3 * time.Second

// Engine is the main discovery engine. This is where all of the Sources and
// sources are stored and is responsible for calling out to the right sources to
// discover everything
//
// Note that an engine that does not have a connected NATS connection will
// simply not communicate over NATS
type Engine struct {
	// Descriptive name of this engine. Used as responder name in SDP responses
	Name string

	NATSOptions   *connect.NATSOptions // Options for connecting to NATS
	NATSQueueName string               // The name of the queue to use when subscribing

	// The maximum number of queries that can be executing in parallel. Defaults
	// to the number of CPUs
	MaxParallelExecutions int

	// The maximum request timeout. Defaults to `DefaultMaxRequestTimeout` if
	// set to zero. If a client does not send a timeout, it will default to this
	// value. Requests with timeouts larger than this value will have their
	// timeouts overridden
	MaxRequestTimeout time.Duration

	// How often to check for closed connections and try to recover
	ConnectionWatchInterval time.Duration
	connectionWatcher       NATSWatcher

	// Internal throttle used to limit MaxParallelExecutions. This reads
	// MaxParallelExecutions and is populated when the engine is started
	throttle *Throttle

	// Cache that is used for storing SDP items in memory
	cache *sdpcache.Cache

	// The NATS connection
	natsConnection      sdp.EncodedConnection
	natsConnectionMutex sync.Mutex

	// List of all current subscriptions
	subscriptions []*nats.Subscription

	// All Sources managed by this Engine
	sh *SourceHost

	// GetListMutex used for locking out Get requests when there's a List happening
	gfm GetListMutex

	// trackedRequests is used for storing requests that have a UUID so they can
	// be cancelled if required
	trackedRequests      map[uuid.UUID]*RequestTracker
	trackedRequestsMutex sync.RWMutex

	// Prevents the engine being restarted many times in parallel
	restartMutex sync.Mutex

	// Context to control cache purging. Purging will stop when the cache is cancelled
	cacheContext context.Context
	// Func that cancels cache purging
	cacheCancel context.CancelFunc
}

func NewEngine() (*Engine, error) {
	sh, err := NewSourceHost()
	if err != nil {
		return nil, err
	}
	return &Engine{
		MaxParallelExecutions:   runtime.NumCPU(),
		MaxRequestTimeout:       DefaultMaxRequestTimeout,
		ConnectionWatchInterval: DefaultConnectionWatchInterval,
		cache:                   sdpcache.NewCache(),
		sh:                      sh,
		trackedRequests:         make(map[uuid.UUID]*RequestTracker),
	}, nil
}

// TrackRequest Stores a RequestTracker in the engine so that it can be looked
// up later and cancelled if required. The UUID should be supplied as part of
// the request itself
func (e *Engine) TrackRequest(uuid uuid.UUID, request *RequestTracker) {
	e.trackedRequestsMutex.Lock()
	defer e.trackedRequestsMutex.Unlock()
	e.trackedRequests[uuid] = request
}

// GetTrackedRequest Returns the RequestTracked object for a given UUID. This
// tracker can then be used to cancel the request
func (e *Engine) GetTrackedRequest(uuid uuid.UUID) (*RequestTracker, error) {
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
	e.trackedRequestsMutex.Lock()
	defer e.trackedRequestsMutex.Unlock()
	delete(e.trackedRequests, uuid)
}

// AddSources Adds a source to this engine
func (e *Engine) AddSources(sources ...Source) {
	e.sh.AddSources(sources...)
}

// Connect Connects to NATS
func (e *Engine) connect() error {
	// Try to connect to NATS
	if no := e.NATSOptions; no != nil {
		ec, err := e.NATSOptions.Connect()

		if err != nil {
			return err
		}

		e.natsConnectionMutex.Lock()
		e.natsConnection = ec
		e.natsConnectionMutex.Unlock()

		e.connectionWatcher = NATSWatcher{
			Connection: e.natsConnection,
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
		e.connectionWatcher.Start(e.ConnectionWatchInterval)

		// Wait for the connection to be completed
		err = e.natsConnection.Underlying().FlushTimeout(10 * time.Minute)

		if err != nil {
			return err
		}

		log.WithFields(log.Fields{
			"ServerID": e.natsConnection.Underlying().ConnectedServerId(),
			"URL:":     e.natsConnection.Underlying().ConnectedUrl(),
		}).Info("NATS connected")

		err = e.subscribe("request.all", sdp.NewAsyncRawItemRequestHandler("ItemRequestHandler", func(ctx context.Context, _ *nats.Msg, i *sdp.ItemRequest) {
			e.HandleItemRequest(ctx, i)
		}))

		if err != nil {
			return err
		}

		err = e.subscribe("cancel.all", sdp.NewAsyncRawCancelItemRequestHandler("CancelHandler", func(ctx context.Context, m *nats.Msg, i *sdp.CancelItemRequest) {
			e.HandleCancelItemRequest(ctx, i)
		}))

		if err != nil {
			return err
		}

		// Loop over all sources and work out what subscriptions we need to make
		// depending on what scopes they support. These scope names are then
		// stored in a map for de-duplication before being subscribed to
		subscriptionMap := make(map[string]bool)

		// We need to track if we are making a wildcard subscription. If we are then
		// there isn't any point making 10 subscriptions since they will be covered
		// by the wildcard anyway and will end up being duplicates. In that case we
		// should just be making the one
		var wildcardExists bool

		for _, src := range e.sh.Sources() {
			for _, itemScope := range src.Scopes() {
				if itemScope == sdp.WILDCARD {
					wildcardExists = true
				} else {
					subscriptionMap[itemScope] = true
				}
			}
		}

		// Now actually create the required subscriptions
		if wildcardExists {
			e.subscribe("request.scope.>", sdp.NewAsyncRawItemRequestHandler("WildcardItemRequestHandler", func(ctx context.Context, m *nats.Msg, i *sdp.ItemRequest) {
				e.HandleItemRequest(ctx, i)
			}))
			e.subscribe("cancel.scope.>", sdp.NewAsyncRawCancelItemRequestHandler("WildcardCancelHandler", func(ctx context.Context, m *nats.Msg, i *sdp.CancelItemRequest) {
				e.HandleCancelItemRequest(ctx, i)
			}))
		} else {
			for suffix := range subscriptionMap {
				e.subscribe(fmt.Sprintf("request.scope.%v", suffix), sdp.NewAsyncRawItemRequestHandler("WildcardItemRequestHandler", func(ctx context.Context, m *nats.Msg, i *sdp.ItemRequest) {
					e.HandleItemRequest(ctx, i)
				}))
				e.subscribe(fmt.Sprintf("cancel.scope.%v", suffix), sdp.NewAsyncRawCancelItemRequestHandler("WildcardCancelHandler", func(ctx context.Context, m *nats.Msg, i *sdp.CancelItemRequest) {
					e.HandleCancelItemRequest(ctx, i)
				}))
			}
		}

		return nil
	}

	return errors.New("no NATSOptions struct provided")
}

// disconnect Disconnects the engine from the NATS network
func (e *Engine) disconnect() error {
	e.connectionWatcher.Stop()

	e.natsConnectionMutex.Lock()
	defer e.natsConnectionMutex.Unlock()

	if e.natsConnection == nil {
		return nil
	}

	if e.natsConnection.Underlying() != nil {
		// Only unsubscribe if the connection is not closed. If it's closed
		// there is no point
		for _, c := range e.subscriptions {
			if e.natsConnection.Status() != nats.CONNECTED {
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

	e.natsConnection.Drop()

	return nil
}

// Start performs all of the initialisation steps required for the engine to
// work. Note that this creates NATS subscriptions for all available sources so
// modifying the Sources value after an engine has been started will not have
// any effect until the engine is restarted
func (e *Engine) Start() error {
	e.throttle = NewThrottle(e.MaxParallelExecutions)

	e.cacheContext, e.cacheCancel = context.WithCancel(context.Background())

	// Start purging cache
	e.cache.StartPurger(e.cacheContext)

	return e.connect()
}

// subscribe Subscribes to a subject using the current NATS connection.
// Remember to use sdp.NewMsgHandler to get a nats.MsgHandler with otel propagation and protobuf marshaling
func (e *Engine) subscribe(subject string, handler nats.MsgHandler) error {
	var subscription *nats.Subscription
	var err error

	e.natsConnectionMutex.Lock()
	defer e.natsConnectionMutex.Unlock()

	if e.natsConnection.Underlying() == nil {
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

	// Stop purging and clear the cache
	e.cacheCancel()
	e.cache.Clear()

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

	if e.natsConnection == nil {
		return false
	}

	if conn := e.natsConnection.Underlying(); conn != nil {
		return conn.IsConnected()
	}

	return false
}

// HandleCancelItemRequest Takes a CancelItemRequest and cancels that request if it exists
func (e *Engine) HandleCancelItemRequest(ctx context.Context, cancelRequest *sdp.CancelItemRequest) {
	u, err := uuid.FromBytes(cancelRequest.UUID)

	if err != nil {
		log.Errorf("Error parsing UUID for cancel request: %v", err)
		return
	}

	rt, err := e.GetTrackedRequest(u)

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
// implementing the wildcard check everywhere so that if we need to change the
// wildcard at a later date we can do so here
func IsWildcard(s string) bool {
	return s == sdp.WILDCARD
}
