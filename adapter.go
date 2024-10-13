package discovery

import (
	"context"
	"encoding/json"
	"os"

	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdpcache"
)

// Adapter is capable of finding information about items
//
// Adapters must implement all of the methods to satisfy this interface in order
// to be able to used as an SDP adapter. Note that the `context.Context` value
// that is passed to the Get(), List() and Search() (optional) methods needs to
// handled by each adapter individually. Adapter authors should make an effort
// ensure that expensive operations that the adapter undertakes can be cancelled
// if the context `ctx` is cancelled
type Adapter interface {
	// Type The type of items that this adapter is capable of finding
	Type() string

	// Descriptive name for the adapter, used in logging and metadata
	Name() string

	// List of scopes that this adapter is capable of find items for. If the
	// adapter supports all scopes the special value "*"
	// should be used
	Scopes() []string

	// Get Get a single item with a given scope and query. The item returned
	// should have a UniqueAttributeValue that matches the `query` parameter.
	Get(ctx context.Context, scope string, query string, ignoreCache bool) (*sdp.Item, error)

	// List Lists all items in a given scope
	List(ctx context.Context, scope string, ignoreCache bool) ([]*sdp.Item, error)

	// Weight Returns the priority weighting of items returned by this adapter.
	// This is used to resolve conflicts where two adapters of the same type
	// return an item for a GET query. In this instance only one item can be
	// sen on, so the one with the higher weight value will win.
	Weight() int

	// A struct that contains information about the adapter, it is used by the api-server to determine the capabilities of the adapter
	// It is mandatory for all adapters to implement this method
	Metadata() *sdp.AdapterMetadata
}

func AdapterMetadataToJSONFile(components []*sdp.AdapterMetadata, targetLocation string) error {
	// create the target location folder if it doesn't exist
	err := os.MkdirAll(targetLocation, os.ModePerm)
	if err != nil {
		return err
	}
	for i := range components {
		component := components[i]
		// convert the component to JSON
		bytes, err := json.Marshal(component)
		if err != nil {
			return err
		}
		// write the JSON to a file
		err = os.WriteFile(targetLocation+"/"+component.GetType()+".json", bytes, 0600)
		if err != nil {
			return err
		}
	}
	return nil
}

// CachingAdapter Is an adapter of items that supports caching
type CachingAdapter interface {
	Adapter
	Cache() *sdpcache.Cache
}

// SearchableAdapter Is an adapter of items that supports searching
type SearchableAdapter interface {
	Adapter
	// Search executes a specific search and returns zero or many items as a
	// result (and optionally an error). The specific format of the query that
	// needs to be provided to Search is dependant on the adapter itself as each
	// adapter will respond to searches differently
	Search(ctx context.Context, scope string, query string, ignoreCache bool) ([]*sdp.Item, error)
}

// HiddenAdapter adapters that define a `Hidden()` method are able to tell whether
// or not the items they produce should be marked as hidden within the metadata.
// Hidden items will not be shown in GUIs or stored in databases and are used
// for gathering data as part of other processes such as remotely executed
// secondary adapters
type HiddenAdapter interface {
	Hidden() bool
}
