package discovery

import (
	"context"
	"time"

	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdpcache"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Source is capable of finding information about items
//
// Sources must implement all of the methods to satisfy this interface in order
// to be able to used as an SDP source. Note that the `context.Context` value
// that is passed to the Get(), Find() and Search() (optional) methods needs to
// handled by each source individually. Source authors should make an effort
// ensure that expensive operations that the source undertakes can be cancelled
// if the context `ctx` is cancelled
type Source interface {
	// Type The type of items that this source is capable of finding
	Type() string

	// Descriptive name for the source, used in logging and metadata
	Name() string

	// List of contexts that this source is capable of find items for. If the
	// source supports all contexts the special value "*"
	// should be used
	Contexts() []string

	// Get Get a single item with a given context and query. The item returned
	// should have a UniqueAttributeValue that matches the `query` parameter.
	//
	// Note that the itemContext parameter represents the context of the item
	// from the perspective of State Description Protocol (SDP), whereas the
	// `context.Context` value is a golang context which is used for
	// cancellations and timeouts
	Get(ctx context.Context, itemContext string, query string) (*sdp.Item, error)

	// Find Finds all items in a given context
	//
	// Note that the itemContext parameter represents the context of the item
	// from the perspective of State Description Protocol (SDP), whereas the
	// `context.Context` value is a golang context which is used for
	// cancellations and timeouts
	Find(ctx context.Context, itemContext string) ([]*sdp.Item, error)

	// Weight Returns the priority weighting of items returned by this source.
	// This is used to resolve conflicts where two sources of the same type
	// return an item for a GET request. In this instance only one item can be
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
	//
	// Note that the itemContext parameter represents the context of the item
	// from the perspective of State Description Protocol (SDP), whereas the
	// `context.Context` value is a golang context which is used for
	// cancellations and timeouts
	Search(ctx context.Context, itemContext string, query string) ([]*sdp.Item, error)
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
// for gathering data as part of other proccesses such as remotely executed
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

// Get Runs a get query against known sources in priority order. If nothing was
// found, returns the first error. This returns a slice if items for
// convenience, but this should always be of length 1 or 0
func (e *Engine) Get(ctx context.Context, r *sdp.ItemRequest, relevantSources []Source) ([]*sdp.Item, []*sdp.ItemRequestError) {
	return e.callSources(ctx, r, relevantSources, Get)
}

// Find executes Find() on all sources for a given type, returning the merged
// results. Only returns an error if all sources fail, in which case returns the
// first error
func (e *Engine) Find(ctx context.Context, r *sdp.ItemRequest, relevantSources []Source) ([]*sdp.Item, []*sdp.ItemRequestError) {
	return e.callSources(ctx, r, relevantSources, Find)
}

// Search executes Search() on all sources for a given type, returning the merged
// results. Only returns an error if all sources fail, in which case returns the
// first error
func (e *Engine) Search(ctx context.Context, r *sdp.ItemRequest, relevantSources []Source) ([]*sdp.Item, []*sdp.ItemRequestError) {
	searchableSources := make([]Source, 0)

	// Filter further by searchability
	for _, source := range relevantSources {
		if searchable, ok := source.(SearchableSource); ok {
			searchableSources = append(searchableSources, searchable)
		}
	}

	return e.callSources(ctx, r, searchableSources, Search)
}

type SourceMethod int64

const (
	Get SourceMethod = iota
	Find
	Search
)

func (s SourceMethod) String() string {
	switch s {
	case Get:
		return "Get"
	case Find:
		return "Find"
	case Search:
		return "Search"
	default:
		return "Unknown"
	}
}

func (e *Engine) callSources(ctx context.Context, r *sdp.ItemRequest, relevantSources []Source, method SourceMethod) ([]*sdp.Item, []*sdp.ItemRequestError) {
	errs := make([]*sdp.ItemRequestError, 0)
	items := make([]*sdp.Item, 0)

	// We want to avid having a Get and a Find running at the same time, we'd
	// rather run the Find first, populate the cache, then have the Get just
	// grab the value from the cache. To this end we use a GetFindMutex to allow
	// a Find to block all subsequent Get requests until it is done
	switch method {
	case Get:
		e.gfm.GetLock(r.Context, r.Type)
		defer e.gfm.GetUnlock(r.Context, r.Type)
	case Find:
		e.gfm.FindLock(r.Context, r.Type)
		defer e.gfm.FindUnlock(r.Context, r.Type)
	case Search:
		// We don't need to lock for a search since they are independant and
		// will only ever have a cache hit if the request is identical
	}

	for _, src := range relevantSources {
		tags := sdpcache.Tags{
			"sourceName": src.Name(),
			"context":    r.Context,
			"type":       r.Type,
		}

		switch method {
		case Get:
			// With a Get request we need just the one specific item, so also
			// filter on uniqueAttributeValue
			tags["uniqueAttributeValue"] = r.Query
		case Find:
			// In the case of a find, we just want everything that was found in
			// the last find, so we only care about the method
			tags["method"] = method.String()
		case Search:
			// For a search, we only want to get from the cache items that were
			// found using search, and with the exact same query
			tags["method"] = method.String()
			tags["query"] = r.Query
		}

		logFields := log.Fields{
			"sourceName": src.Name(),
			"type":       r.Type,
			"context":    r.Context,
		}

		if !r.IgnoreCache {
			// TODO: This is where I'm up to. Rethink the way this works. Does
			// the logic make sense here?
			cachedItems, err := e.cache.Search(tags)

			if err != nil {
				switch err := err.(type) {
				case sdpcache.CacheNotFoundError:
					// If nothing was found then continue with execution
				case *sdp.ItemRequestError:
					// Add relevant info
					err.Context = r.Context
					err.ItemRequestUUID = r.UUID
					err.SourceName = src.Name()
					err.ItemType = r.Type

					errs = append(errs, err)

					logFields["error"] = err.Error()

					if err.ErrorType == sdp.ItemRequestError_NOTFOUND {
						log.WithFields(logFields).Debug("Found cached empty result, not executing")
					} else {
						log.WithFields(logFields).Debug("Found cached error")
					}

					continue
				default:
					// If it's an unknown error, conver it to SDP and skip this source
					errs = append(errs, &sdp.ItemRequestError{
						ItemRequestUUID: r.UUID,
						ErrorType:       sdp.ItemRequestError_OTHER,
						ErrorString:     err.Error(),
						Context:         r.Context,
						SourceName:      src.Name(),
						ItemType:        r.Type,
					})

					logFields["error"] = err.Error()
					log.WithFields(logFields).Debug("Found cached error")

					continue
				}
			} else {
				logFields["numItems"] = len(cachedItems)
				log.WithFields(logFields).Debug("Found items from cache")

				if method == Get {
					// If the method was Get we should validate that we have
					// only pulled one thing from the cache

					if len(cachedItems) < 2 {
						items = append(items, cachedItems...)
						continue
					} else {
						// If we got a weird number of stuff from the cache then
						// something is wrong
						log.WithFields(logFields).Error("Cache returned >1 value, purging and continuing")

						e.cache.Delete(tags)
					}
				} else {
					items = append(items, cachedItems...)
					continue
				}
			}
		}

		log.WithFields(logFields).Debugf("Executing %v", method.String())

		var resultItems []*sdp.Item
		var err error

		searchDuration := timeOperation(func() {
			switch method {
			case Get:
				var newItem *sdp.Item

				newItem, err = src.Get(ctx, r.Context, r.Query)

				if err == nil {
					resultItems = []*sdp.Item{newItem}
				}
			case Find:
				resultItems, err = src.Find(ctx, r.Context)
			case Search:
				if searchableSrc, ok := src.(SearchableSource); ok {
					resultItems, err = searchableSrc.Search(ctx, r.Context, r.Query)
				} else {
					err = &sdp.ItemRequestError{
						ItemRequestUUID: r.UUID,
						ErrorType:       sdp.ItemRequestError_NOTFOUND,
						ErrorString:     "source is not searchable",
						Context:         r.Context,
						SourceName:      src.Name(),
						ItemType:        r.Type,
					}
				}
			}
		})

		logFields["items"] = len(resultItems)
		logFields["error"] = err

		if considerFailed(err) {
			log.WithFields(logFields).Error("Error during search")
		} else {
			log.WithFields(logFields).Debugf("%v completed", method.String())

			if err != nil {
				// In this case the error must be a NOTFOUND error, which we
				// want to cache
				e.cache.StoreError(err, GetCacheDuration(src), tags)
			}
		}

		if err != nil {
			if sdpErr, ok := err.(*sdp.ItemRequestError); ok {
				errs = append(errs, sdpErr)
			} else {
				errs = append(errs, &sdp.ItemRequestError{
					ItemRequestUUID: r.UUID,
					ErrorType:       sdp.ItemRequestError_OTHER,
					ErrorString:     err.Error(),
					Context:         r.Context,
					SourceName:      src.Name(),
					ItemType:        r.Type,
				})
			}
		}

		// For each found item, add more details
		//
		// Use the index here to ensure that we're actually editing the
		// right thing
		for i := range resultItems {
			// Get a pointer to the item we're dealing with
			item := resultItems[i]

			// Handle the case where we are given a nil pointer
			if item == nil {
				continue
			}

			// Store metadata
			item.Metadata = &sdp.Metadata{
				Timestamp:             timestamppb.New(time.Now()),
				SourceDuration:        durationpb.New(searchDuration),
				SourceDurationPerItem: durationpb.New(time.Duration(searchDuration.Nanoseconds() / int64(len(resultItems)))),
				SourceName:            src.Name(),
			}

			// Mark the item as hidden if the source is a hidden source
			if hs, ok := src.(HiddenSource); ok {
				item.Metadata.Hidden = hs.Hidden()
			}

			// Cache the item
			e.cache.StoreItem(item, GetCacheDuration(src), tags)
		}

		items = append(items, resultItems...)

		if method == Get {
			// If it's a get, we just return the first thing that works
			if len(resultItems) > 0 {
				break
			}
		}
	}

	return items, errs
}

// considerFailed Returns whether or not a given error should be considered as a
// failure or not. The only error that isn't consider a failure is a
// *sdp.ItemRequestError with a Type of NOTFOUND, this means that it was queried
// successfully but it simply doesn't exist
func considerFailed(err error) bool {
	if err == nil {
		return false
	} else {
		if sdperr, ok := err.(*sdp.ItemRequestError); ok {
			if sdperr.ErrorType == sdp.ItemRequestError_NOTFOUND {
				return false
			} else {
				return true
			}
		} else {
			return true
		}
	}
}

// timeOperation Times how lon an operation takes and stores it in the first
// parameter. The second parameter is the function to execute
func timeOperation(f func()) time.Duration {
	start := time.Now()

	f()

	return time.Since(start)
}
