package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdpcache"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AllSourcesFailedError Will be returned when all sources have failed
type AllSourcesFailedError struct {
	NumSources int
}

func (e AllSourcesFailedError) Error() string {
	return fmt.Sprintf("all sources (%v) failed", e.NumSources)
}

// NewItemSubject Generates a random subject name for returning items e.g.
// return.item._INBOX.712ab421
func NewItemSubject() string {
	return fmt.Sprintf("return.item.%v", nats.NewInbox())
}

// NewResponseSubject Generates a random subject name for returning responses
// e.g. return.response._INBOX.978af6de
func NewResponseSubject() string {
	return fmt.Sprintf("return.response.%v", nats.NewInbox())
}

// HandleItemRequest Handles a single request. This includes responses, linking
// etc.
func (e *Engine) HandleItemRequest(ctx context.Context, itemRequest *sdp.ItemRequest) {
	ctx, span := tracer.Start(ctx, "HandleItemRequest")
	defer span.End()

	numExpandedRequests := len(e.sh.ExpandRequest(itemRequest))
	span.SetAttributes(attribute.Int("om.discovery.numExpandedRequests", numExpandedRequests))

	if numExpandedRequests == 0 {
		// If we don't have any relevant sources, exit
		return
	}

	var timeoutOverride bool

	// If the timeout is infinite OR greater than the max, set it to the max
	if itemRequest.Timeout.AsDuration() == 0 || itemRequest.Timeout.AsDuration() > e.MaxRequestTimeout {
		itemRequest.Timeout = durationpb.New(e.MaxRequestTimeout)
		timeoutOverride = true
	}

	// Respond saying we've got it
	responder := sdp.ResponseSender{
		ResponseSubject: itemRequest.ResponseSubject,
	}

	var pub sdp.EncodedConnection

	if e.IsNATSConnected() {
		pub = e.natsConnection
	} else {
		pub = NilConnection{}
	}

	responder.Start(
		ctx,
		pub,
		e.Name,
	)

	// Extract and parse the UUID
	reqUUID, uuidErr := uuid.FromBytes(itemRequest.UUID)

	span.SetAttributes(
		attribute.String("om.sdp.requestType", itemRequest.Type),
		attribute.String("om.sdp.requestMethod", itemRequest.Method.String()),
		attribute.String("om.sdp.requestQuery", itemRequest.Query),
		attribute.Int("om.sdp.requestLinkDepth", int(itemRequest.LinkDepth)),
		attribute.String("om.sdp.requestScope", itemRequest.Scope),
		attribute.String("om.sdp.requestTimeout", itemRequest.Timeout.AsDuration().String()),
		attribute.Bool("om.sdp.requestTimeoutOverridden", timeoutOverride),
		attribute.String("om.sdp.requestUUID", reqUUID.String()),
		attribute.Bool("om.sdp.requestIgnoreCache", itemRequest.IgnoreCache),
	)

	requestTracker := RequestTracker{
		Request: itemRequest,
		Engine:  e,
	}

	if uuidErr == nil {
		e.TrackRequest(reqUUID, &requestTracker)
	}

	_, _, err := requestTracker.Execute(ctx)

	// If all failed then return an error
	if err != nil {
		if err == context.Canceled {
			responder.Cancel()
		} else {
			responder.Error()
		}

		span.SetAttributes(
			attribute.String("om.sdp.errorType", "OTHER"),
			attribute.String("om.sdp.errorString", err.Error()),
		)
	} else {
		responder.Done()
	}
}

// ExecuteRequestSync Executes a request, waiting for all results, then returns
// them along with the error, rather than passing the results back along channels
func (e *Engine) ExecuteRequestSync(ctx context.Context, req *sdp.ItemRequest) ([]*sdp.Item, []*sdp.ItemRequestError, error) {
	itemsChan := make(chan *sdp.Item, 100_000)
	errsChan := make(chan *sdp.ItemRequestError, 100_000)
	items := make([]*sdp.Item, 0)
	errs := make([]*sdp.ItemRequestError, 0)

	err := e.ExecuteRequest(ctx, req, itemsChan, errsChan)

	for i := range itemsChan {
		items = append(items, i)
	}

	for e := range errsChan {
		errs = append(errs, e)
	}

	return items, errs, err
}

// ExecuteRequest Executes a single request and returns the results without any
// linking. Will return an error if all sources fail, or the request couldn't be
// run.
//
// Items and errors will be sent to the supplied channels as they are found.
// Note that if these channels are not buffered, something will need to be
// receiving the results or this method will never finish. If results are not
// required the channels can be nil
func (e *Engine) ExecuteRequest(ctx context.Context, req *sdp.ItemRequest, items chan<- *sdp.Item, errs chan<- *sdp.ItemRequestError) error {
	// Make sure we close channels once we're done
	if items != nil {
		defer close(items)
	}
	if errs != nil {
		defer close(errs)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	wg := sync.WaitGroup{}

	expanded := e.sh.ExpandRequest(req)

	if len(expanded) == 0 {
		errs <- &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOSCOPE,
			ErrorString: "no matching sources found",
			Scope:       req.Scope,
		}

		return errors.New("no matching sources found")
	}

	// These are used to calculate whether all sources have failed or not
	var numSources atomic.Int32
	var numErrs int

	for request, sources := range expanded {
		wg.Add(1)

		e.throttle.Lock()

		go func(ctx context.Context, r *sdp.ItemRequest, sources []Source) {
			defer wg.Done()
			defer e.throttle.Unlock()
			var requestItems []*sdp.Item
			var requestErrors []*sdp.ItemRequestError
			numSources.Add(1)

			// Make the request of all sources
			switch req.GetMethod() {
			case sdp.RequestMethod_GET:
				requestItems, requestErrors = e.Get(ctx, r, sources)
			case sdp.RequestMethod_LIST:
				requestItems, requestErrors = e.List(ctx, r, sources)
			case sdp.RequestMethod_SEARCH:
				requestItems, requestErrors = e.Search(ctx, r, sources)
			}

			for _, i := range requestItems {
				// If the main request had a linkDepth of greater than zero it means we
				// need to keep linking, this means that we need to pass down all of the
				// subject info along with the number of remaining links. If the link
				// depth is zero then we just pass then back in their normal form as we
				// won't be executing them
				if req.GetLinkDepth() > 0 {
					for _, lir := range i.LinkedItemRequests {
						lir.LinkDepth = req.LinkDepth - 1
						lir.ItemSubject = req.ItemSubject
						lir.ResponseSubject = req.ResponseSubject
						lir.IgnoreCache = req.IgnoreCache
						lir.Timeout = req.Timeout
						lir.UUID = req.UUID
					}
				}

				// Assign the item request
				if i.Metadata != nil {
					i.Metadata.SourceRequest = req
				}

				if items != nil {
					items <- i
				}
			}

			for _, e := range requestErrors {
				if r != nil {
					numErrs++

					if errs != nil {
						errs <- e
					}
				}
			}
		}(ctx, request, sources)
	}

	// Wait for all requests to complete
	wg.Wait()

	// If all failed then return first error
	if numSourcesInt := numSources.Load(); numErrs == int(numSourcesInt) {
		return AllSourcesFailedError{
			NumSources: int(numSourcesInt),
		}
	}

	return nil
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

func (e *Engine) callSources(ctx context.Context, r *sdp.ItemRequest, relevantSources []Source, method SourceMethod) ([]*sdp.Item, []*sdp.ItemRequestError) {
	ctx, span := tracer.Start(ctx, "CallSources", trace.WithAttributes(
		attribute.String("om.engine.method", method.String()),
	))
	defer span.End()

	items := make([]*sdp.Item, 0)
	errs := make([]*sdp.ItemRequestError, 0)

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
						// If nothing was found then execute the search against the sources
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
				start := time.Now()

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
