package discovery

import (
	"context"
	"time"

	"github.com/overmindtech/sdp-go"
)

// Source is capable of finding information about items
//
// Sources must implement all of the methods to satisfy this interface in order
// to be able to used as an SDP source. Note that the `context.Context` value
// that is passed to the Get(), List() and Search() (optional) methods needs to
// handled by each source individually. Source authors should make an effort
// ensure that expensive operations that the source undertakes can be cancelled
// if the context `ctx` is cancelled
type Source interface {
	// Type The type of items that this source is capable of finding
	Type() string

	// Descriptive name for the source, used in logging and metadata
	Name() string

	// List of scopes that this source is capable of find items for. If the
	// source supports all scopes the special value "*"
	// should be used
	Scopes() []string

	// Get Get a single item with a given scope and query. The item returned
	// should have a UniqueAttributeValue that matches the `query` parameter.
	Get(ctx context.Context, scope string, query string) (*sdp.Item, error)

	// List Lists all items in a given scope
	List(ctx context.Context, scope string) ([]*sdp.Item, error)

	// Weight Returns the priority weighting of items returned by this source.
	// This is used to resolve conflicts where two sources of the same type
	// return an item for a GET query. In this instance only one item can be
	// sen on, so the one with the higher weight value will win.
	Weight() int
}

// SearchableItemSource Is a source of items that supports searching
type SearchableSource interface {
	Source
	// Search executes a specific search and returns zero or many items as a
	// result (and optionally an error). The specific format of the query that
	// needs to be provided to Search is dependant on the source itself as each
	// source will respond to searches differently
	Search(ctx context.Context, scope string, query string) ([]*sdp.Item, error)
}

// CacheDefiner Some backends may implement the CacheDefiner interface which
// means that they specify a custom default cache interval. The config will
// always take precedence however
type CacheDefiner interface {
	DefaultCacheDuration() time.Duration
}

// HiddenSource Sources that define a `Hidden()` method are able to tell whether
// or not the items they produce should be marked as hidden within the metadata.
// Hidden items will not be shown in GUIs or stored in databases and are used
// for gathering data as part of other processes such as remotely executed
// secondary sources
type HiddenSource interface {
	Hidden() bool
}

// GetCacheDuration Gets the cache duration for a specific source, or a default
// value
func GetCacheDuration(s Source) time.Duration {
	if cd, ok := s.(CacheDefiner); ok {
		return cd.DefaultCacheDuration()
	}

	return (10 * time.Minute)
}

type SourceMethod int64

const (
	Get SourceMethod = iota
	List
	Search
)

func (s SourceMethod) String() string {
	switch s {
	case Get:
		return "Get"
	case List:
		return "List"
	case Search:
		return "Search"
	default:
		return "Unknown"
	}
}
