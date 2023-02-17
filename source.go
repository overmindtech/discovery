package discovery

import (
	"context"
	"errors"
	"time"

	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdpcache"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// Get Runs a get query against known sources in priority order. If nothing was
// found, returns the first error. This returns a slice if items for
// convenience, but this should always be of length 1 or 0
func (e *Engine) Get(ctx context.Context, r *sdp.ItemRequest, relevantSources []Source) ([]*sdp.Item, []*sdp.ItemRequestError) {
	return e.callSources(ctx, r, relevantSources, Get)
}

// List executes List() on all sources for a given type, returning the merged
// results. Only returns an error if all sources fail, in which case returns the
// first error
func (e *Engine) List(ctx context.Context, r *sdp.ItemRequest, relevantSources []Source) ([]*sdp.Item, []*sdp.ItemRequestError) {
	return e.callSources(ctx, r, relevantSources, List)
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

func (e *Engine) callSources(ctx context.Context, r *sdp.ItemRequest, relevantSources []Source, method SourceMethod) ([]*sdp.Item, []*sdp.ItemRequestError) {
	ctx, span := tracer.Start(ctx, "CallSources")
	defer span.End()

	errs := make([]*sdp.ItemRequestError, 0)
	items := make([]*sdp.Item, 0)

	// We want to avoid having a Get and a List running at the same time, we'd
	// rather run the List first, populate the cache, then have the Get just
	// grab the value from the cache. To this end we use a GetListMutex to allow
	// a List to block all subsequent Get requests until it is done
	switch method {
	case Get:
		e.gfm.GetLock(r.Scope, r.Type)
		defer e.gfm.GetUnlock(r.Scope, r.Type)
	case List:
		e.gfm.ListLock(r.Scope, r.Type)
		defer e.gfm.ListUnlock(r.Scope, r.Type)
	case Search:
		// We don't need to lock for a search since they are independent and
		// will only ever have a cache hit if the request is identical
	}

	for _, src := range relevantSources {
		if func() bool {
			ctx, span := tracer.Start(ctx, src.Name())
			defer span.End()

			query := sdpcache.CacheQuery{
				SST: sdpcache.SST{
					SourceName: src.Name(),
					Scope:      r.Scope,
					Type:       r.Type,
				},
			}

			switch method {
			case Get:
				// With a Get request we need just the one specific item, so also
				// filter on uniqueAttributeValue
				query.UniqueAttributeValue = &r.Query
			case List:
				// In the case of a find, we just want everything that was found in
				// the last find, so we only care about the method
				query.Method = &r.Method
			case Search:
				// For a search, we only want to get from the cache items that were
				// found using search, and with the exact same query
				query.Method = &r.Method
				query.Query = &r.Query
			}

			span.SetAttributes(
				attribute.String("om.source.sourceName", src.Name()),
				attribute.String("om.source.requestType", r.Type),
				attribute.String("om.source.requestScope", r.Scope),
			)

			if !r.IgnoreCache {
				cachedItems, err := e.cache.Search(query)

				if err != nil {
					var ire *sdp.ItemRequestError
					if errors.Is(err, sdpcache.ErrCacheNotFound) {
						// If nothing was found then continue with execution
					} else if errors.As(err, &ire) {
						// Add relevant info
						ire.Scope = r.Scope
						ire.ItemRequestUUID = r.UUID
						ire.SourceName = src.Name()
						ire.ItemType = r.Type

						errs = append(errs, ire)

						span.SetAttributes(attribute.String("om.cache.error", err.Error()))

						if ire.ErrorType == sdp.ItemRequestError_NOTFOUND {
							span.SetStatus(codes.Ok, "cache hit: item not found")
						} else {
							span.SetStatus(codes.Ok, "cache hit: ItemRequestError")
						}

						return false
					} else {
						// If it's an unknown error, convert it to SDP and skip this source
						errs = append(errs, &sdp.ItemRequestError{
							ItemRequestUUID: r.UUID,
							ErrorType:       sdp.ItemRequestError_OTHER,
							ErrorString:     err.Error(),
							Scope:           r.Scope,
							SourceName:      src.Name(),
							ItemType:        r.Type,
						})

						span.SetAttributes(attribute.String("om.cache.error", err.Error()))

						span.SetStatus(codes.Ok, "cache hit: ItemRequestError")

						return false
					}
				} else {
					span.SetAttributes(attribute.Int("om.cache.numItems", len(cachedItems)))

					if method == Get {
						// If the method was Get we should validate that we have
						// only pulled one thing from the cache

						if len(cachedItems) < 2 {
							span.SetStatus(codes.Ok, "cache hit: 1 item")

							items = append(items, cachedItems...)
							return false
						} else {
							span.AddEvent("cache returned >1 value, purging and continuing")

							e.cache.Delete(query)
						}
					} else {
						span.SetStatus(codes.Ok, "cache hit: multiple items")

						items = append(items, cachedItems...)
						return false
					}
				}
			}

			var resultItems []*sdp.Item
			var err error
			var sourceDuration time.Duration

			func(ctx context.Context) {
				ctx, span := tracer.Start(ctx, method.String())
				start := time.Now()
				defer span.End()

				switch method {
				case Get:
					var newItem *sdp.Item

					newItem, err = src.Get(ctx, r.Scope, r.Query)

					if err == nil {
						resultItems = []*sdp.Item{newItem}
					}
				case List:
					resultItems, err = src.List(ctx, r.Scope)
				case Search:
					if searchableSrc, ok := src.(SearchableSource); ok {
						resultItems, err = searchableSrc.Search(ctx, r.Scope, r.Query)
					} else {
						err = &sdp.ItemRequestError{
							ErrorType:   sdp.ItemRequestError_NOTFOUND,
							ErrorString: "source is not searchable",
						}
					}
				}

				sourceDuration = time.Since(start)
			}(ctx)

			span.SetAttributes(attribute.Int("om.source.numItems", len(resultItems)))

			if considerFailed(err) {
				span.SetStatus(codes.Error, err.Error())
			} else {
				if err != nil {
					// In this case the error must be a NOTFOUND error, which we
					// want to cache
					e.cache.StoreError(err, GetCacheDuration(src), query)
				}
			}

			if err != nil {
				span.SetAttributes(attribute.String("om.source.error", err.Error()))

				if sdpErr, ok := err.(*sdp.ItemRequestError); ok {
					// Add details if they aren't populated
					if sdpErr.Scope == "" {
						sdpErr.Scope = r.Scope
					}
					sdpErr.ItemRequestUUID = r.UUID
					sdpErr.ItemType = src.Type()
					sdpErr.ResponderName = e.Name
					sdpErr.SourceName = src.Name()

					errs = append(errs, sdpErr)
				} else {
					errs = append(errs, &sdp.ItemRequestError{
						ItemRequestUUID: r.UUID,
						ErrorType:       sdp.ItemRequestError_OTHER,
						ErrorString:     err.Error(),
						Scope:           r.Scope,
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
					SourceDuration:        durationpb.New(sourceDuration),
					SourceDurationPerItem: durationpb.New(time.Duration(sourceDuration.Nanoseconds() / int64(len(resultItems)))),
					SourceName:            src.Name(),
					SourceRequest:         r,
				}

				// Mark the item as hidden if the source is a hidden source
				if hs, ok := src.(HiddenSource); ok {
					item.Metadata.Hidden = hs.Hidden()
				}

				// Cache the item
				e.cache.StoreItem(item, GetCacheDuration(src))
			}

			items = append(items, resultItems...)

			if method == Get {
				// If it's a get, we just return the first thing that works
				if len(resultItems) > 0 {
					return true
				}
			}

			return false
		}() {
			// `get` queries only return the first source results
			break
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
