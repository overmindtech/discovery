package discovery

import (
	"sync"

	"github.com/dylanratcliffe/sdp-go"
)

const AllContexts = "all"

// Source is capable of finding information about items
type Source interface {
	// Type The type of items that this source is capable of finding
	Type() string

	// Descriptive name for the source, used in logging and metadata
	Name() string

	// List of contexts that this source is capable of find items for. If the
	// source supports all contexts the special value `AllContexts` ("all")
	// should be used
	Contexts() []string

	// Get Get a single item with a given context and query. The item returned
	// should have a UniqueAttributeValue that matches the `query` parameter
	Get(itemContext string, query string) (*sdp.Item, error)

	// Find Finds all items in a given context
	Find(itemContext string) ([]*sdp.Item, error)

	// Weight Returns the priority weighting of items returned by this source.
	// This is used to resolve conflicts where two sources of the same type
	// return an item for a GET request. In this instance only one item can be
	// sen on, so the one with the higher weight value will win.
	Weight() int
}

// SearchableItemSource Is a source of items that supports searching
type SearchableSource interface {
	Source
	Search(query string) ([]*sdp.Item, error)
}

// Get Runs a get query against known sources in priority order. If nothing was
// found, returns the first error
func (e *Engine) Get(typ string, context string, query string) (*sdp.Item, error) {
	// TODO: Throttling
	// TODO: Logging
	errors := make([]error, 0)

	for _, src := range e.SourcesForType(typ) {
		item, err := src.Get(context, query)

		if err == nil {
			return item, nil
		}

		errors = append(errors, err)
	}

	return nil, errors[0]
}

// Find executes Find() on all sources for a given type, returning the merged
// results. Only returns an error if all sources fail, in which case returns the
// first error
func (e *Engine) Find(typ string, context string) ([]*sdp.Item, error) {
	var storageMutex sync.Mutex
	var workingSources sync.WaitGroup

	relevantSources := e.SourcesForType(typ)
	items := make([]*sdp.Item, 0)
	errors := make([]error, 0)

	// TODO: Throttling
	// TODO: Logging
	for _, src := range relevantSources {
		workingSources.Add(1)
		go func(source Source) {
			defer workingSources.Done()

			foundItems, err := source.Find(context)

			storageMutex.Lock()
			items = append(items, foundItems...)
			errors = append(errors, err)
			storageMutex.Unlock()
		}(src)
	}

	workingSources.Wait()
	storageMutex.Lock()
	defer storageMutex.Unlock()

	// Check if there were any successful runs and if so return the items
	for _, e := range errors {
		if e == nil {
			return items, nil
		}
	}

	return items, errors[0]
}

// Search executes Search() on all sources for a given type, returning the merged
// results. Only returns an error if all sources fail, in which case returns the
// first error
func (e *Engine) Search(typ string, context string) ([]*sdp.Item, error) {
	var storageMutex sync.Mutex
	var workingSources sync.WaitGroup

	relevantSources := e.SourcesForType(typ)
	items := make([]*sdp.Item, 0)
	errors := make([]error, 0)

	// TODO: Throttling
	// TODO: Logging
	for _, src := range relevantSources {
		if searchable, ok := src.(SearchableSource); ok {
			workingSources.Add(1)
			go func(source Source) {
				defer workingSources.Done()

				foundItems, err := searchable.Search(context)

				storageMutex.Lock()
				items = append(items, foundItems...)
				errors = append(errors, err)
				storageMutex.Unlock()
			}(src)
		}
	}

	workingSources.Wait()
	storageMutex.Lock()
	defer storageMutex.Unlock()

	// Check if there were any successful runs and if so return the items
	for _, e := range errors {
		if e == nil {
			return items, nil
		}
	}

	return items, errors[0]
}
