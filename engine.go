package discovery

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdp-go/auth"
	log "github.com/sirupsen/logrus"
	"github.com/sourcegraph/conc/pool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const DefaultMaxRequestTimeout = 1 * time.Minute
const DefaultConnectionWatchInterval = 3 * time.Second

// The clint that will be used to send heartbeats. This will usually be an
// `sdpconnect.ManagementServiceClient`
type HeartbeatClient interface {
	SubmitSourceHeartbeat(context.Context, *connect.Request[sdp.SubmitSourceHeartbeatRequest]) (*connect.Response[sdp.SubmitSourceHeartbeatResponse], error)
}

type HeartbeatOptions struct {
	// The client that will be used to send heartbeats
	ManagementClient HeartbeatClient

	// The function that should be run to check if the adapter is healthy. It
	// will be executed each time a heartbeat is sent and should return an error
	// if the adapter is unhealthy.
	HealthCheck func() error

	// How frequently to send a heartbeat
	Frequency time.Duration
}

// Engine is the main discovery engine. This is where all of the Adapters and
// adapters are stored and is responsible for calling out to the right adapters to
// discover everything
//
// Note that an engine that does not have a connected NATS connection will
// simply not communicate over NATS
type Engine struct {
	// Descriptive name of this engine. Used as responder name in SDP responses
	Name string
	// UUID iof this engine. This will be used to identify it for heartbeats. If
	// this is empty, a random UUID will be generated when the engine is started
	UUID uuid.UUID
	// The version of the adapter that should be reported in the heartbeat
	Version string
	// The of adapter, this will be reported to Overmind as part of the
	// heartbeat. e.g. "aws" or "kubernetes"
	Type string
	// Whether this adapter is managed by Overmind. This is inly used for
	// reporting so that you can tell the difference between managed adapters and
	// ones you're running locally
	Managed sdp.SourceManaged

	NATSOptions   *auth.NATSOptions // Options for connecting to NATS
	NATSQueueName string            // The name of the queue to use when subscribing

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

	// The configuration for the heartbeat for this engine. If this is nil the
	// engine won't send heartbeats when started
	HeartbeatOptions *HeartbeatOptions

	// Internal throttle used to limit MaxParallelExecutions. This reads
	// MaxParallelExecutions and is populated when the engine is started. This
	// pool is only used for LIST requests. Since GET requests can be blocked by
	// LIST requests, they need to be handled in a different pool to avoid
	// deadlocking.
	listExecutionPool *pool.Pool

	// Internal throttle used to limit MaxParallelExecutions. This reads
	// MaxParallelExecutions and is populated when the engine is started. This
	// pool is only used for GET and SEARCH requests. Since GET requests can be
	// blocked by LIST requests, they need to be handled in a different pool to
	// avoid deadlocking.
	getExecutionPool *pool.Pool

	// The NATS connection
	natsConnection      sdp.EncodedConnection
	natsConnectionMutex sync.Mutex

	// List of all current subscriptions
	subscriptions []*nats.Subscription

	// All Adapters managed by this Engine
	sh *AdapterHost

	// GetListMutex used for locking out Get queries when there's a List happening
	gfm GetListMutex

	// trackedQueries is used for storing queries that have a UUID so they can
	// be cancelled if required
	trackedQueries      map[uuid.UUID]*QueryTracker
	trackedQueriesMutex sync.RWMutex

	// Prevents the engine being restarted many times in parallel
	restartMutex sync.Mutex

	// Context to background jobs like cache purging and heartbeats. These will
	// stop when the context is cancelled
	backgroundJobContext context.Context
	backgroundJobCancel  context.CancelFunc
	heartbeatCancel      context.CancelFunc
}

func NewEngine() (*Engine, error) {
	sh := NewAdapterHost()
	return &Engine{
		MaxParallelExecutions:   runtime.NumCPU(),
		MaxRequestTimeout:       DefaultMaxRequestTimeout,
		ConnectionWatchInterval: DefaultConnectionWatchInterval,
		sh:                      sh,
		trackedQueries:          make(map[uuid.UUID]*QueryTracker),
	}, nil
}

// TrackQuery Stores a QueryTracker in the engine so that it can be looked
// up later and cancelled if required. The UUID should be supplied as part of
// the query itself
func (e *Engine) TrackQuery(uuid uuid.UUID, qt *QueryTracker) {
	e.trackedQueriesMutex.Lock()
	defer e.trackedQueriesMutex.Unlock()
	e.trackedQueries[uuid] = qt
}

// GetTrackedQuery Returns the QueryTracker object for a given UUID. This
// tracker can then be used to cancel the query
func (e *Engine) GetTrackedQuery(uuid uuid.UUID) (*QueryTracker, error) {
	e.trackedQueriesMutex.RLock()
	defer e.trackedQueriesMutex.RUnlock()

	if qt, ok := e.trackedQueries[uuid]; ok {
		return qt, nil
	} else {
		return nil, fmt.Errorf("tracker with UUID %x not found", uuid)
	}
}

// DeleteTrackedQuery Deletes a query from tracking
func (e *Engine) DeleteTrackedQuery(uuid [16]byte) {
	e.trackedQueriesMutex.Lock()
	defer e.trackedQueriesMutex.Unlock()
	delete(e.trackedQueries, uuid)
}

// AddAdapters Adds an adapter to this engine
func (e *Engine) AddAdapters(adapters ...Adapter) {
	e.sh.AddAdapters(adapters...)
}

// Connect Connects to NATS
func (e *Engine) connect() error {
	// Try to connect to NATS
	if e.NATSOptions != nil {
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

		err = e.subscribe("request.all", sdp.NewAsyncRawQueryHandler("QueryHandler", func(ctx context.Context, _ *nats.Msg, i *sdp.Query) {
			e.HandleQuery(ctx, i)
		}))
		if err != nil {
			return err
		}

		err = e.subscribe("cancel.all", sdp.NewAsyncRawCancelQueryHandler("CancelQueryHandler", func(ctx context.Context, m *nats.Msg, i *sdp.CancelQuery) {
			e.HandleCancelQuery(ctx, i)
		}))
		if err != nil {
			return err
		}

		err = e.subscribe("request.scope.>", sdp.NewAsyncRawQueryHandler("WildcardQueryHandler", func(ctx context.Context, m *nats.Msg, i *sdp.Query) {
			e.HandleQuery(ctx, i)
		}))
		if err != nil {
			return err
		}

		err = e.subscribe("cancel.scope.>", sdp.NewAsyncRawCancelQueryHandler("WildcardCancelQueryHandler", func(ctx context.Context, m *nats.Msg, i *sdp.CancelQuery) {
			e.HandleCancelQuery(ctx, i)
		}))
		if err != nil {
			return err
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
// work. Note that this creates NATS subscriptions for all available adapters so
// modifying the Adapters value after an engine has been started will not have
// any effect until the engine is restarted
func (e *Engine) Start() error {
	e.listExecutionPool = pool.New().WithMaxGoroutines(e.MaxParallelExecutions)
	e.getExecutionPool = pool.New().WithMaxGoroutines(e.MaxParallelExecutions)

	e.backgroundJobContext, e.backgroundJobCancel = context.WithCancel(context.Background())

	// Decide your own UUID if not provided
	if e.UUID == uuid.Nil {
		e.UUID = uuid.New()
	}

	// Start background jobs
	e.sh.StartPurger(e.backgroundJobContext)
	e.StartSendingHeartbeats(e.backgroundJobContext)

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
	if e.backgroundJobCancel != nil {
		e.backgroundJobCancel()
	}
	if e.heartbeatCancel != nil {
		e.heartbeatCancel()
	}

	e.sh.ClearCaches()

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

// HealthCheck returns an error if the Engine is not healthy. Call this inside
// an opentelemetry span to capture default metrics from the engine.
func (e *Engine) HealthCheck(ctx context.Context) error {
	span := trace.SpanFromContext(ctx)

	natsConnected := e.IsNATSConnected()

	span.SetAttributes(
		attribute.String("ovm.engine.name", e.Name),
		attribute.Bool("ovm.nats.connected", natsConnected),
		attribute.Int("ovm.discovery.listExecutionPoolCount", int(listExecutionPoolCount.Load())),
		attribute.Int("ovm.discovery.getExecutionPoolCount", int(getExecutionPoolCount.Load())),
	)

	if !natsConnected {
		return errors.New("NATS connection is not connected")
	}

	return nil
}

// HandleCancelQuery Takes a CancelQuery and cancels that query if it exists
func (e *Engine) HandleCancelQuery(ctx context.Context, cancelQuery *sdp.CancelQuery) {
	span := trace.SpanFromContext(ctx)
	span.SetName("HandleCancelQuery")

	u, err := uuid.FromBytes(cancelQuery.GetUUID())

	if err != nil {
		log.Errorf("Error parsing UUID for cancel query: %v", err)
		return
	}

	rt, err := e.GetTrackedQuery(u)

	if err != nil {
		log.Debugf("Could not find tracked query %v. Possibly it has already finished", u.String())
		return
	}

	if rt != nil && rt.Cancel != nil {
		log.WithFields(log.Fields{
			"UUID": u.String(),
		}).Debug("Cancelling query")
		rt.Cancel()
	}
}

// ClearCache Completely clears the cache
func (e *Engine) ClearCache() {
	e.sh.ClearCaches()
}

// ClearAdapters Deletes all adapters from the engine, allowing new adapters to be
// added using `AddAdapter()`. Note that this requires a restart using
// `Restart()` in order to take effect
func (e *Engine) ClearAdapters() {
	e.sh.ClearAllAdapters()
}

// IsWildcard checks if a string is the wildcard. Use this instead of
// implementing the wildcard check everywhere so that if we need to change the
// wildcard at a later date we can do so here
func IsWildcard(s string) bool {
	return s == sdp.WILDCARD
}
