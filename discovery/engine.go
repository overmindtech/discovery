package discovery

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/dylanratcliffe/sdp-go"

	"github.com/dylanratcliffe/deviant_cli/sources"
	"github.com/spf13/viper"

	"github.com/nats-io/nats.go"

	log "github.com/sirupsen/logrus"
)

// DefaultBackendPriority The default priority value for all backends that do
// not define their own default and are not overridden in config
const DefaultBackendPriority = 50

// DefaultBackendCacheDurationSeconds The number of seconds to cache each item
// unless it is overridden in the backend of the config
const DefaultBackendCacheDurationSeconds = 1800

// Engine is the main discovery engine. This is where all of the Sources and
// sources are stored and is responsible for calling out to the right sources to
// discover everything
//
// Note that an engine that does not have a connected NATS connection will
// simply not communicate over NATS
type Engine struct {
	NATSConnection *nats.EncodedConn
	Assistants     map[string]*Assistant

	BackendPackages []func() ([]sources.Backend, error)

	// This callback is used to check if the engine has a working NATS
	// connection
	connectionCallback func() bool

	// These are called when the local linker is executed
	linkedItemCallbacks []func(*sdp.Item)
}

// Start performs all of the initialisation steps required for the engine to
// work
func (e *Engine) Start() {
	// Create slices and maps
	e.Assistants = make(map[string]*Assistant)
	e.linkedItemCallbacks = make([]func(*sdp.Item), 0)

	// Check if we have a NATS connection and set up handling if we don't
	if e.NATSConnection == nil {
		e.connectionCallback = func() bool {
			// If we don't have a pointer to a NATS connection object then we
			// will never have a connection so always return false
			return false
		}
	} else {
		// If we do have a connection then link this callback to the actual NATS
		// IsConnected method so that it's always up to date instead of just
		// setting it once when we start the engine
		e.connectionCallback = e.NATSConnection.Conn.IsConnected
	}

	if e.IsNATSConnected() {
		log.WithFields(log.Fields{
			"Addr":     e.NATSConnection.Conn.ConnectedAddr(),
			"ServerID": e.NATSConnection.Conn.ConnectedServerId(),
			"URL:":     e.NATSConnection.Conn.ConnectedUrl(),
		}).Info("Starting engine with NATS connection")
	} else {
		log.Info("Starting engine without NATS connection")
	}

	contextBackends := make(map[string][]*sources.BackendInfo)

	// Loop over all backend packages
	for _, backendFunction := range e.BackendPackages {
		// Load backends from the package
		packageBackends, err := backendFunction()

		if err == nil {
			// If it didn't fail then create a backend info object for each
			// discovered backed
			for _, be := range packageBackends {
				bi := sources.BackendInfo{
					Backend:       be,
					Priority:      GetBackendPriority(be),
					CacheDuration: GetBackendCacheDuration(be),
					Context:       GetBackendContext(be),
				}

				// Save this to the backend group for each context
				contextBackends[bi.Context] = append(contextBackends[bi.Context], &bi)
			}
		} else {
			// TODO: This doesn't log the backend package that it was sourced from
			log.WithFields(log.Fields{
				"error":           err,
				"backendFunction": backendFunction,
			}).Info("Failed to load some backends")
		}
	}

	// Worker settings
	viper.SetDefault("workers", runtime.NumCPU())
	workers := viper.GetInt("workers")

	// Create a permissions pool for all workers
	pool := NewPermissionPool(workers)

	// Generate assistants and register backends
	for context, backends := range contextBackends {
		log.WithFields(log.Fields{
			"context":     context,
			"numBackends": len(backends),
		}).Debug("Creating assistant")

		// Create the assistant
		assistant := NewAssistant()

		// Set the context
		assistant.Context = context

		// Register backends
		for _, backend := range backends {
			assistant.RegisterBackend(backend)
		}

		// Add permissions
		assistant.StartWithPermissions(pool)

		// Save the assistant
		e.Assistants[context] = &assistant
	}

	if e.IsNATSConnected() {
		// Create listeners for each assistant
		for context, assistant := range e.Assistants {
			// Next step is to create listeners on all of the topics that we care about
			// There won't need to be any communication between these threads as all
			// they will do is get a command from the thing they are listening to and
			// respond
			// Subscribe to requests on `request.all`
			if _, err := e.NATSConnection.QueueSubscribe("request.all", fmt.Sprintf("primary.daemon.%v", context), assistant.NewItemRequestServer(e.NATSConnection)); err != nil {
				log.Fatal(err)
			}

			// Subscribe to context specific requests on request.context.{context}
			subject := "request.context." + context

			if _, err := e.NATSConnection.QueueSubscribe(subject, fmt.Sprintf("primary.daemon.%v", context), assistant.NewItemRequestServer(e.NATSConnection)); err != nil {
				log.Fatal(err)
			}
		}
	}
}

// FindAll will do the following:
//
//   * Send a `FIND` request to every backend that the engine has
//   * Process all results and return the items
//
// Linking can be toggled based on the `Link` attribute of the enging itself
//
func (e *Engine) FindAll() []*sdp.Item {
	var wg sync.WaitGroup
	itemsFound := make([]*sdp.Item, 0)
	itemsChan := make(chan *sdp.Item)
	itemsDone := make(chan bool)

	// Create a thread to constantly read from the channel
	go func() {
		for item := range itemsChan {
			itemsFound = append(itemsFound, item)
		}
		itemsDone <- true
	}()

	for _, assistant := range e.Assistants {
		wg.Add(1)

		rh := RequestHandlerV2{
			Assistant: assistant,
		}

		for typ := range assistant.Sources {
			// Get all items that we can find
			r := sdp.ItemRequest{
				Type:      typ,
				Method:    sdp.RequestMethod_FIND,
				Context:   assistant.Context,
				LinkDepth: 65535,
			}

			rh.Requests = append(rh.Requests, &r)
		}

		go func(handler *RequestHandlerV2, i chan *sdp.Item) {
			defer wg.Done()

			// Execute the find request
			foundItems, _ := rh.Run()

			// Place all found items onto the channel for collection
			for _, foundItem := range foundItems {
				i <- foundItem
			}
		}(&rh, itemsChan)
	}

	wg.Wait()

	close(itemsChan)

	// Wait for things to be added to memory
	<-itemsDone
	close(itemsDone)

	return itemsFound
}

// IsNATSConnected returns whether the engine is connected to NATS
func (e *Engine) IsNATSConnected() bool {
	return e.connectionCallback()
}

// RegisterLinkedItemCallback Allows users to add callback functions that will
// be called whenever an item has finished being linked. This will be called
// regardless if whether the item had any LinkedItemRequests or not
func (e *Engine) RegisterLinkedItemCallback(cb func(*sdp.Item)) {
	e.linkedItemCallbacks = append(e.linkedItemCallbacks, cb)
}

// GetBackendPriority Gets the priority of a backend from config
// (backends.package_name.priority) or defaults to the default set by the backend, or
// DefaultBackendPriority
func GetBackendPriority(backend sources.Backend) int {
	var priority int

	// Get the priority from the config
	p, ok := backend.(sources.PriorityDefiner)

	if ok {
		priority = p.DefaultPriority()
	} else {
		priority = DefaultBackendPriority
	}

	viper.SetDefault(fmt.Sprintf("backends.%v.priority", backend.BackendPackage()), priority)
	return viper.GetInt(fmt.Sprintf("backends.%v.priority", backend.BackendPackage()))
}

// GetBackendCacheDuration Gets the duration of the caching for items produced
// by a backend from config (backends.package_name.cache_duration) or defaults to the
// default set by the backend, or DefaultBackendCacheDurationSeconds
func GetBackendCacheDuration(backend sources.Backend) time.Duration {
	var durationSeconds int
	var duration time.Duration

	// Check if the backend has a default duration
	c, ok := backend.(sources.CacheDefiner)

	if ok {
		durationSeconds = int(c.DefaultCacheDuration().Seconds())
	} else {
		durationSeconds = DefaultBackendCacheDurationSeconds
	}

	viper.SetDefault(fmt.Sprintf("backends.%v.cache_duration", backend.BackendPackage()), durationSeconds)
	durationSeconds = viper.GetInt(fmt.Sprintf("backends.%v.cache_duration", backend.BackendPackage()))
	duration = time.Duration(durationSeconds) * time.Second
	return duration
}

// GetBackendContext returns the context of a given backend. If the backend
// doesn't have the ability to determine its own context then it returns
// sources.DefaultContext()
func GetBackendContext(backend sources.Backend) string {
	// Calculate the context that the item was found in. This is the
	// default context or whatever the backend says if the backend is
	// capable of determining contexts
	if i, hasContext := backend.(sources.Contextual); hasContext {
		return i.Context()
	}

	return sources.LocalContext()
}

// deleteItemRequest Deletes an item request from a slice
func deleteItemRequest(requests []*sdp.ItemRequest, remove *sdp.ItemRequest) []*sdp.ItemRequest {
	finalRequests := make([]*sdp.ItemRequest, 0)
	for _, request := range requests {
		if request != remove {
			finalRequests = append(finalRequests, request)
		}
	}
	return finalRequests
}
