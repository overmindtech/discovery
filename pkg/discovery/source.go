package discovery

import (
	"fmt"
	"sync"
	"time"

	"github.com/dylanratcliffe/sdp-go"
	"github.com/dylanratcliffe/source-go/pkg/sdpcache"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TODO: Implement GetFindMutex

const AllContexts = "*"

// Source is capable of finding information about items
type Source interface {
	// Type The type of items that this source is capable of finding
	Type() string

	// Descriptive name for the source, used in logging and metadata
	Name() string

	// List of contexts that this source is capable of find items for. If the
	// source supports all contexts the special value `AllContexts` ("*")
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
	Search(itemContext string, query string) ([]*sdp.Item, error)
}

// CacheDefiner Some backends may implement the CacheDefiner interface which
// means that they specify a custom default cache interval. The config will
// always take precedence however
type CacheDefiner interface {
	DefaultCacheDuration() time.Duration
}

// GetCacheDuration Gets the cache duration for a specific source, or a default
// value
func GetCacheDuration(s Source) time.Duration {
	if cd, ok := s.(CacheDefiner); ok {
		return cd.DefaultCacheDuration()
	}

	return (10 * time.Minute)
}

// FilterSources returns the set of sources that match the supplied type and
// context
func (e *Engine) FilterSources(typ string, context string) []Source {
	sources := make([]Source, 0)

	// Get all sources that match the supplied type
	for _, source := range e.sourceMap[typ] {
		// Filter by matching context
		for _, sourceContext := range source.Contexts() {
			if sourceContext == context || IsWildcard(sourceContext) {
				sources = append(sources, source)
			}
		}
	}

	return sources
}

// Get Runs a get query against known sources in priority order. If nothing was
// found, returns the first error
func (e *Engine) Get(typ string, context string, query string) (*sdp.Item, error) {
	relevantSources := e.FilterSources(typ, context)

	if len(relevantSources) == 0 {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: fmt.Sprintf("no sources found for type %v and context %v", typ, context),
			Context:     context,
		}
	}

	// TODO: Throttling
	for _, src := range relevantSources {
		tags := sdpcache.Tags{
			"sourceName":           src.Name(),
			"uniqueAttributeValue": query,
			"type":                 typ,
			"context":              context,
		}

		logFields := log.Fields{
			"sourceName":   src.Name(),
			"sourceWeight": src.Weight(),
			"type":         typ,
			"context":      context,
			"query":        query,
		}

		cached, cacheErr := e.cache.Search(tags)

		switch err := cacheErr.(type) {
		case sdpcache.CacheNotFoundError:
			// If the item/error wasn't found in the cache then just continue on
		case *sdp.ItemRequestError:
			if err.ErrorType == sdp.ItemRequestError_NOTFOUND {
				// If the item wasn't found, but we've already looked then don't look
				// again and just return a blank result
				log.WithFields(logFields).Debug("Was not found previously, skipping")

				// TODO: Re-implement the GetFindMutex
				// s.GetFindMutex.GetUnlock()

				continue
			}
		case nil:
			if len(cached) == 1 {
				// If the cache found something then just return that
				log.WithFields(logFields).Debug("Found item from cache")

				// s.GetFindMutex.GetUnlock()

				return cached[0], nil
			}

			// If we got a weird number of stuff from the cache then
			// something is wrong
			log.WithFields(logFields).Error("Cache returned >1 value, purging and continuing")

			e.cache.Delete(tags)
		}

		log.WithFields(logFields).Debug("Executing get for backend")

		var getDuration time.Duration
		var item *sdp.Item
		var err error

		getDuration = timeOperation(func() {
			item, err = src.Get(context, query)
		})

		logFields["itemFound"] = (err == nil)
		logFields["error"] = err

		// A good backend should be careful to raise ItemNotFoundError if the
		// query was able to execute successfully, but the item wasn't found. If
		// however there was some failure in checking and therefore we aren't
		// sure if the item is actually there is not another type of error
		// should be raised and this will be logged
		if ire, sdpErr := err.(*sdp.ItemRequestError); (sdpErr && ire.ErrorType == sdp.ItemRequestError_NOTFOUND) || err == nil {
			log.WithFields(logFields).Debug("Get complete")

			if ire != nil {
				// Cache the error since the types was ItemRequestError_NOTFOUND
				// and therefore the item doesn't exist
				e.cache.StoreError(err, GetCacheDuration(src), tags)
			}
		} else {
			log.WithFields(logFields).Error("Get Failed")
		}

		if err == nil {
			// Handle the case where we are given a nil pointer
			if item == nil {
				return &sdp.Item{}, &sdp.ItemRequestError{
					ErrorType:   sdp.ItemRequestError_OTHER,
					ErrorString: "Backend returned a nil pointer as an item",
				}
			}

			// Set the metadata
			item.Metadata = &sdp.Metadata{
				Timestamp:             timestamppb.New(time.Now()),
				SourceDuration:        durationpb.New(getDuration),
				SourceDurationPerItem: durationpb.New(getDuration),
				SourceName:            src.Name(),
			}

			// Store the new item in the cache
			e.cache.StoreItem(item, GetCacheDuration(src), tags)

			// s.GetFindMutex.GetUnlock()

			return item, nil
		}

		// s.GetFindMutex.GetUnlock()
	}

	// If we don't find anything then we should raise an error
	return &sdp.Item{}, &sdp.ItemRequestError{
		ErrorType:   sdp.ItemRequestError_NOTFOUND,
		ErrorString: fmt.Sprintf("No item found in %v sources", len(relevantSources)),
	}
}

// Find executes Find() on all sources for a given type, returning the merged
// results. Only returns an error if all sources fail, in which case returns the
// first error
func (e *Engine) Find(typ string, context string) ([]*sdp.Item, error) {
	var storageMutex sync.Mutex
	var workingSources sync.WaitGroup

	relevantSources := e.FilterSources(typ, context)

	if len(relevantSources) == 0 {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: fmt.Sprintf("no sources found for type %v and context %v", typ, context),
			Context:     context,
		}
	}

	items := make([]*sdp.Item, 0)
	errors := make([]error, 0)

	// TODO: Throttling
	for _, src := range relevantSources {
		workingSources.Add(1)
		go func(source Source) {
			defer workingSources.Done()

			tags := sdpcache.Tags{
				"method":     "find",
				"sourceName": source.Name(),
				"context":    context,
			}

			logFields := log.Fields{
				"sourceName": source.Name(),
				"type":       typ,
				"context":    context,
			}

			cachedItems, err := e.cache.Search(tags)

			switch err := err.(type) {
			case sdpcache.CacheNotFoundError:
				// If the item/error wasn't found in the cache then just
				// continue on
			case *sdp.ItemRequestError:
				if err.ErrorType == sdp.ItemRequestError_NOTFOUND {
					log.WithFields(logFields).Debug("Found cached empty FIND, not executing")

					return
				}
			default:
				// If we get a result from the cache then return that
				if len(cachedItems) > 0 {
					logFields["items"] = len(cachedItems)

					log.WithFields(logFields).Debug("Found items from cache")

					storageMutex.Lock()
					items = append(items, cachedItems...)
					errors = append(errors, err)
					storageMutex.Unlock()

					return
				}
			}

			log.WithFields(logFields).Debug("Executing find")

			finds := make([]*sdp.Item, 0)
			err = nil

			findDuration := timeOperation(func() {
				finds, err = source.Find(context)
			})

			logFields["items"] = len(finds)
			logFields["error"] = err

			if err == nil {
				log.WithFields(logFields).Debug("Find complete")

				// Check too see if nothing was found, make sure we cache the
				// nothing
				if len(finds) == 0 {
					e.cache.StoreError(&sdp.ItemRequestError{
						ErrorType: sdp.ItemRequestError_NOTFOUND,
					}, GetCacheDuration(source), tags)
				}
			} else {
				log.WithFields(logFields).Error("Error during find")

				e.cache.StoreError(err, GetCacheDuration(source), tags)
			}

			// For each found item, add more details
			//
			// Use the index here to ensure that we're actually editing the
			// right thing
			for i := range finds {
				// Get a pointer to the item we're dealing with
				item := finds[i]

				// Handle the case where we are given a nil pointer
				if item == nil {
					continue
				}

				// Store metadata
				item.Metadata = &sdp.Metadata{
					Timestamp:             timestamppb.New(time.Now()),
					SourceDuration:        durationpb.New(findDuration),
					SourceDurationPerItem: durationpb.New(time.Duration(findDuration.Nanoseconds() / int64(len(finds)))),
					SourceName:            source.Name(),
				}

				// Cache the item
				e.cache.StoreItem(item, GetCacheDuration(source), tags)
			}

			// Unlock the  mutex to allow other operations to resume while we
			// continue with the housekeeping
			// s.GetFindMutex.FindUnlock()

			storageMutex.Lock()
			items = append(items, finds...)
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

	if len(errors) > 0 {
		return items, errors[0]
	}

	return items, nil
}

// Search executes Search() on all sources for a given type, returning the merged
// results. Only returns an error if all sources fail, in which case returns the
// first error
func (e *Engine) Search(typ string, context string, query string) ([]*sdp.Item, error) {
	var storageMutex sync.Mutex
	var workingSources sync.WaitGroup

	relevantSources := e.FilterSources(typ, context)
	searchableSources := make([]SearchableSource, 0)

	// Filter further by searchability
	for _, source := range relevantSources {
		if searchable, ok := source.(SearchableSource); ok {
			searchableSources = append(searchableSources, searchable)
		}
	}

	if len(searchableSources) == 0 {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: fmt.Sprintf("no sources found for type %v and context %v that support searching", typ, context),
			Context:     context,
		}
	}

	items := make([]*sdp.Item, 0)
	errors := make([]error, 0)

	// TODO: Throttling
	for _, src := range searchableSources {
		workingSources.Add(1)
		go func(source SearchableSource) {
			defer workingSources.Done()

			tags := sdpcache.Tags{
				"method":     "find",
				"sourceName": source.Name(),
				"query":      query,
				"context":    context,
			}

			logFields := log.Fields{
				"sourceName": source.Name(),
				"type":       typ,
				"context":    context,
			}

			cachedItems, err := e.cache.Search(tags)

			switch err := err.(type) {
			case sdpcache.CacheNotFoundError:
				// If the item/error wasn't found in the cache then just
				// continue on
			case *sdp.ItemRequestError:
				if err.ErrorType == sdp.ItemRequestError_NOTFOUND {
					log.WithFields(logFields).Debug("Found cached empty result, not executing")

					return
				}
			default:
				// If we get a result from the cache then return that
				if len(cachedItems) > 0 {
					logFields["items"] = len(cachedItems)

					log.WithFields(logFields).Debug("Found items from cache")

					storageMutex.Lock()
					items = append(items, cachedItems...)
					errors = append(errors, err)
					storageMutex.Unlock()

					return
				}
			}

			log.WithFields(logFields).Debug("Executing search")

			var searchItems []*sdp.Item
			err = nil

			searchDuration := timeOperation(func() {
				searchItems, err = source.Search(context, query)
			})

			logFields["items"] = len(searchItems)
			logFields["error"] = err

			if err == nil {
				log.WithFields(logFields).Debug("Search completed")

				// Check too see if nothing was found, make sure we cache the
				// nothing
				if len(searchItems) == 0 {
					e.cache.StoreError(&sdp.ItemRequestError{
						ErrorType: sdp.ItemRequestError_NOTFOUND,
					}, GetCacheDuration(source), tags)
				}
			} else {
				log.WithFields(logFields).Error("Error during search")

				e.cache.StoreError(err, GetCacheDuration(source), tags)
			}

			// For each found item, add more details
			//
			// Use the index here to ensure that we're actually editing the
			// right thing
			for i := range searchItems {
				// Get a pointer to the item we're dealing with
				item := searchItems[i]

				// Handle the case where we are given a nil pointer
				if item == nil {
					continue
				}

				// Store metadata
				item.Metadata = &sdp.Metadata{
					Timestamp:             timestamppb.New(time.Now()),
					SourceDuration:        durationpb.New(searchDuration),
					SourceDurationPerItem: durationpb.New(time.Duration(searchDuration.Nanoseconds() / int64(len(searchItems)))),
					SourceName:            source.Name(),
				}

				// Cache the item
				e.cache.StoreItem(item, GetCacheDuration(source), tags)
			}

			storageMutex.Lock()
			items = append(items, searchItems...)
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

	if len(errors) > 0 {
		return items, errors[0]
	}

	return items, nil
}

// timeOperation Times how lon an operation takes and stores it in the first
// parameter. The second parameter is the function to execute
func timeOperation(f func()) time.Duration {
	start := time.Now()

	f()

	return time.Since(start)
}
