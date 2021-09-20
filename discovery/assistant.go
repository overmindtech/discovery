package discovery

import (
	"fmt"

	"github.com/dylanratcliffe/deviant_cli/sources"
	"github.com/dylanratcliffe/sdp-go"
)

// Assistant Underlying struct used to store the information that assistants
// require
type Assistant struct {
	Sources sources.SourceMap
	Context string

	// Pool of permissions to execute backends, exposed as a pseudo worker pool
	backendPermissions chan bool
}

// AssistantStats Statistics about the assistant
type AssistantStats struct {
	NumWorkers int
	Workers    []string
}

// SingleRequest A request that we know will return a single item. It can also
// be passed a unique callback which will be called since the request has been
// completed
type SingleRequest struct {
	Request  sdp.ItemRequest
	Callback func(*sdp.Item, error)
}

// MultiRequest A request that we know will return multiple items. It can also
// be passed a unique callback which will be called since the request has been
// completed
type MultiRequest struct {
	Request  sdp.ItemRequest
	Callback func([]*sdp.Item, error)
}

// RequestProcessor Request processors have the ability to process an incoming
// sdp.ItemRequest and respond appropriately
type RequestProcessor interface {
	ProcessGetRequest(r sdp.ItemRequest) (*sdp.Item, error)
	ProcessFindRequest(r sdp.ItemRequest) []*sdp.Item
	ProcessAsyncRequest(r sdp.ItemRequest)
}

// Get runs a GET on all backends known to the assistant for a paricular type
func (a *Assistant) Get(typ string, query string) (*sdp.Item, error) {
	// Do as much prep as we can before we ask permission
	var src *sources.Source
	var exists bool

	// Get the source ready
	src, exists = a.Sources[typ]

	if exists {
		// Execute the Get
		return src.Get(query)
	}

	// This means we don't have a source that can respond to the request
	return &sdp.Item{}, &sdp.ItemRequestError{
		ErrorType:   sdp.ItemRequestError_NOCONTEXT,
		Context:     a.Context,
		ErrorString: fmt.Sprintf("No matching source for %v", typ),
	}
}

// Find runs a FIND on all backends known to the assistant for a paricular type
func (a *Assistant) Find(typ string) ([]*sdp.Item, error) {
	var src *sources.Source
	var results []*sdp.Item
	var exists bool

	// Get the source ready
	src, exists = a.Sources[typ]

	if !exists {
		// If we don't have a source for the given type then just return nothing
		return make([]*sdp.Item, 0), &sdp.ItemRequestError{
			Context:     a.Context,
			ErrorString: "Requested context not available",
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
		}
	}

	// Execute the find
	results = src.Find()

	if len(results) == 0 {
		return results, &sdp.ItemRequestError{
			Context:     a.Context,
			ErrorString: "No results found",
			ErrorType:   sdp.ItemRequestError_NOTFOUND,
		}
	}

	return results, nil
}

// Search searches for items in all backends known by the assistant
func (a *Assistant) Search(typ string, query string) ([]*sdp.Item, error) {
	var src *sources.Source
	var results []*sdp.Item
	var exists bool

	// Get the source ready
	src, exists = a.Sources[typ]

	if !exists {
		// If we don't have a source for the given type then just return nothing
		return make([]*sdp.Item, 0), &sdp.ItemRequestError{
			Context:     a.Context,
			ErrorString: "Requested context not available",
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
		}
	}

	// Execute the search
	results = src.Search(query)

	if len(results) == 0 {
		return results, &sdp.ItemRequestError{
			Context:     a.Context,
			ErrorString: "No results found",
			ErrorType:   sdp.ItemRequestError_NOTFOUND,
		}
	}

	return results, nil

}

// StartWithWorkers Starts an assistant with a given number of workers
func (a *Assistant) StartWithWorkers(workers int) {
	p := NewPermissionPool(workers)
	a.StartWithPermissions(p)
}

// StartWithPermissions starts a worker with an existing permissions pool
func (a *Assistant) StartWithPermissions(permissions chan bool) {
	a.backendPermissions = permissions

	// Share this permissions channel among all sources
	for _, source := range a.Sources {
		source.BackendExecutionPermissions = a.backendPermissions
	}
}

// NewPermissionPool creates a chan of booleans to be Used by an assistant as
// worker execution permissions
func NewPermissionPool(size int) chan bool {
	// Create the permissions channel
	permissions := make(chan bool, size)

	// Populate the permission channel with the correct number of permissions
	for i := 0; i < size; i++ {
		permissions <- true
	}

	return permissions
}

// RegisterBackend registers a backend, if the source does not exist it will be
// created
//
// All backends must have matching contexts as assistants are not meant to
// process requests for more than once context. Once a backend has been added
// the assistant will reject any other backends that don't match the context of
// the first
func (a *Assistant) RegisterBackend(bi *sources.BackendInfo) error {
	// Check the context of this backend.
	if a.Context == "" {
		a.Context = bi.Context
	}

	if a.Context != bi.Context {
		return fmt.Errorf("backend context %v does not match assistant context %v", bi.Context, a.Context)
	}

	// Check if the type has already been registered
	if source, ok := a.Sources[bi.Backend.Type()]; ok {
		// If it is already there, register the backend with the existing
		// source
		source.RegisterBackend(bi)

		// Name the cache so we get good logging
		bi.Cache.Name = source.GetType() + "." + bi.Backend.BackendPackage()

		// Start the purge process for this backend
		bi.Cache.StartPurger()
	} else {
		// If this is a new type then create a source for the backend to
		// live in
		source = &sources.Source{
			Type:         bi.Backend.Type(),
			GetFindMutex: &sources.GFM{},
		}

		a.Sources[bi.Backend.Type()] = source

		a.RegisterBackend(bi)
	}

	return nil
}

// NewAssistant initialises an assistant and returns it
func NewAssistant() Assistant {
	return Assistant{
		Sources: make(sources.SourceMap),
	}
}

// IsWildcard checks if a string is the wildcard. Use this instead of
// implementing the wildcard check everwhere so that if we need to change the
// woldcard at a later date we can do so here
func IsWildcard(s string) bool {
	return s == "*"
}
