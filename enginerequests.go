package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getsentry/sentry-go"
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

// HandleQuery Handles a single query. This includes responses, linking
// etc.
func (e *Engine) HandleQuery(ctx context.Context, query *sdp.Query) {
	span := trace.SpanFromContext(ctx)
	span.SetName("HandleQuery")

	numExpandedQueries := len(e.sh.ExpandQuery(query))
	span.SetAttributes(attribute.Int("om.discovery.numExpandedQueries", numExpandedQueries))

	if numExpandedQueries == 0 {
		// If we don't have any relevant sources, exit
		return
	}

	var timeoutOverride bool

	// If the timeout is infinite OR greater than the max, set it to the max
	if query.Timeout.AsDuration() == 0 || query.Timeout.AsDuration() > e.MaxRequestTimeout {
		query.Timeout = durationpb.New(e.MaxRequestTimeout)
		timeoutOverride = true
	}

	// Respond saying we've got it
	responder := sdp.ResponseSender{
		ResponseSubject: query.Subject(),
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
	u, uuidErr := uuid.FromBytes(query.UUID)

	span.SetAttributes(
		attribute.String("om.sdp.query", query.Query),
		attribute.String("om.sdp.queryType", query.Type),
		attribute.String("om.sdp.queryMethod", query.Method.String()),
		attribute.String("om.sdp.queryScope", query.Scope),
		attribute.String("om.sdp.queryTimeout", query.Timeout.AsDuration().String()),
		attribute.Bool("om.sdp.queryTimeoutOverridden", timeoutOverride),
		attribute.String("om.sdp.queryUUID", u.String()),
		attribute.Bool("om.sdp.queryIgnoreCache", query.IgnoreCache),
	)

	if query.RecursionBehaviour != nil {
		span.SetAttributes(
			attribute.Int("om.sdp.queryLinkDepth", int(query.RecursionBehaviour.LinkDepth)),
			attribute.Bool("om.sdp.queryFollowOnlyBlastPropagation", query.RecursionBehaviour.FollowOnlyBlastPropagation),
		)
	}

	qt := QueryTracker{
		Query:  query,
		Engine: e,
	}

	if uuidErr == nil {
		e.TrackQuery(u, &qt)
	}

	_, _, err := qt.Execute(ctx)

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

// ExecuteQuerySync Executes a Query, waiting for all results, then returns
// them along with the error, rather than passing the results back along channels
func (e *Engine) ExecuteQuerySync(ctx context.Context, q *sdp.Query) ([]*sdp.Item, []*sdp.QueryError, error) {
	itemsChan := make(chan *sdp.Item, 100_000)
	errsChan := make(chan *sdp.QueryError, 100_000)
	items := make([]*sdp.Item, 0)
	errs := make([]*sdp.QueryError, 0)

	err := e.ExecuteQuery(ctx, q, itemsChan, errsChan)

	for i := range itemsChan {
		items = append(items, i)
	}

	for e := range errsChan {
		errs = append(errs, e)
	}

	return items, errs, err
}

// ExecuteQuery Executes a single Query and returns the results without any
// linking. Will return an error if all sources fail, or the Query couldn't be
// run.
//
// Items and errors will be sent to the supplied channels as they are found.
// Note that if these channels are not buffered, something will need to be
// receiving the results or this method will never finish. If results are not
// required the channels can be nil
func (e *Engine) ExecuteQuery(ctx context.Context, query *sdp.Query, items chan<- *sdp.Item, errs chan<- *sdp.QueryError) error {
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

	expanded := e.sh.ExpandQuery(query)

	if len(expanded) == 0 {
		errs <- &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOSCOPE,
			ErrorString: "no matching sources found",
			Scope:       query.Scope,
		}

		return errors.New("no matching sources found")
	}

	// These are used to calculate whether all sources have failed or not
	var numSources atomic.Int32
	var numErrs int

	// Since we need to wait for only the processing of this query's executions, we need a separate WaitGroup here
	// Overall MaxParallelExecutions evaluation is handled by e.executionPool
	wg := sync.WaitGroup{}
	for q, sources := range expanded {
		wg.Add(1)
		// localize values for the closure below
		q, sources := q, sources

		// push all queued items through a goroutine to avoid blocking `ExecuteQuery` from progressing
		// as `executionPool.Go()` will block once the max parallelism is hit
		go func() {
			// queue everything into the execution pool
			defer sentry.Recover()
			e.executionPool.Go(func() {
				defer sentry.Recover()
				defer wg.Done()
				var queryItems []*sdp.Item
				var queryErrors []*sdp.QueryError
				numSources.Add(1)

				// query all sources
				switch query.GetMethod() {
				case sdp.QueryMethod_GET:
					queryItems, queryErrors = e.Get(ctx, q, sources)
				case sdp.QueryMethod_LIST:
					queryItems, queryErrors = e.List(ctx, q, sources)
				case sdp.QueryMethod_SEARCH:
					queryItems, queryErrors = e.Search(ctx, q, sources)
				}

				for _, i := range queryItems {
					// If the main query had a linkDepth of greater than zero it means we
					// need to keep linking, this means that we need to pass down all of the
					// subject info along with the number of remaining links. If the link
					// depth is zero then we just pass then back in their normal form as we
					// won't be executing them
					if query.RecursionBehaviour.GetLinkDepth() > 0 {
						for _, lir := range i.LinkedItemQueries {
							lir.Query.RecursionBehaviour = &sdp.Query_RecursionBehaviour{
								LinkDepth:                  query.RecursionBehaviour.LinkDepth - 1,
								FollowOnlyBlastPropagation: query.RecursionBehaviour.FollowOnlyBlastPropagation,
							}
							if query.RecursionBehaviour.FollowOnlyBlastPropagation && !lir.BlastPropagation.Out {
								// we're only following blast propagation, so do not link this item further
								// TODO: we might want to drop the link completely if this returns too much
								// information, but that could risk missing revlinks
								lir.Query.RecursionBehaviour.LinkDepth = 0
							}
							lir.Query.IgnoreCache = query.IgnoreCache
							lir.Query.Timeout = query.Timeout
							lir.Query.UUID = query.UUID
						}
					}

					// Assign the source query
					if i.Metadata != nil {
						i.Metadata.SourceQuery = query
					}

					if items != nil {
						items <- i
					}
				}

				for _, e := range queryErrors {
					if q != nil {
						numErrs++

						if errs != nil {
							errs <- e
						}
					}
				}
			})
		}()
	}

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
func (e *Engine) Get(ctx context.Context, r *sdp.Query, relevantSources []Source) ([]*sdp.Item, []*sdp.QueryError) {
	return e.callSources(ctx, r, relevantSources, Get)
}

// List executes List() on all sources for a given type, returning the merged
// results. Only returns an error if all sources fail, in which case returns the
// first error
func (e *Engine) List(ctx context.Context, r *sdp.Query, relevantSources []Source) ([]*sdp.Item, []*sdp.QueryError) {
	return e.callSources(ctx, r, relevantSources, List)
}

// Search executes Search() on all sources for a given type, returning the merged
// results. Only returns an error if all sources fail, in which case returns the
// first error
func (e *Engine) Search(ctx context.Context, r *sdp.Query, relevantSources []Source) ([]*sdp.Item, []*sdp.QueryError) {
	searchableSources := make([]Source, 0)

	// Filter further by searchability
	for _, source := range relevantSources {
		if searchable, ok := source.(SearchableSource); ok {
			searchableSources = append(searchableSources, searchable)
		}
	}

	return e.callSources(ctx, r, searchableSources, Search)
}

func (e *Engine) callSources(ctx context.Context, r *sdp.Query, relevantSources []Source, method SourceMethod) ([]*sdp.Item, []*sdp.QueryError) {
	ctx, span := tracer.Start(ctx, "CallSources", trace.WithAttributes(
		attribute.String("om.engine.method", method.String()),
	))
	defer span.End()

	items := make([]*sdp.Item, 0)
	errs := make([]*sdp.QueryError, 0)

	// We want to avoid having a Get and a List running at the same time, we'd
	// rather run the List first, populate the cache, then have the Get just
	// grab the value from the cache. To this end we use a GetListMutex to allow
	// a List to block all subsequent Get querys until it is done
	switch method {
	case Get:
		e.gfm.GetLock(r.Scope, r.Type)
		defer e.gfm.GetUnlock(r.Scope, r.Type)
	case List:
		e.gfm.ListLock(r.Scope, r.Type)
		defer e.gfm.ListUnlock(r.Scope, r.Type)
	case Search:
		// We don't need to lock for a search since they are independent and
		// will only ever have a cache hit if the query is identical
	}

	for _, src := range relevantSources {
		if func() bool {
			if len(relevantSources) > 1 {
				ctx, span = tracer.Start(ctx, src.Name())
				defer span.End()
			} else {
				span.SetName(fmt.Sprintf("CallSources: %v", src.Name()))
			}

			query := sdpcache.CacheQuery{
				SST: sdpcache.SST{
					SourceName: src.Name(),
					Scope:      r.Scope,
					Type:       r.Type,
				},
			}

			switch method {
			case Get:
				// With a Get query we need just the one specific item, so also
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
				attribute.String("om.source.method", method.String()),
				attribute.String("om.source.reqmethod", r.Method.String()),
				attribute.String("om.source.sourceName", src.Name()),
				attribute.String("om.source.queryType", r.Type),
				attribute.String("om.source.queryScope", r.Scope),
			)

			if !r.IgnoreCache {
				cachedItems, err := e.cache.Search(query)

				if err != nil {
					var ire *sdp.QueryError
					if errors.Is(err, sdpcache.ErrCacheNotFound) {
						// If nothing was found then execute the search against the sources
					} else if errors.As(err, &ire) {
						// Add relevant info
						ire.Scope = r.Scope
						ire.UUID = r.UUID
						ire.SourceName = src.Name()
						ire.ItemType = r.Type

						errs = append(errs, ire)

						span.SetAttributes(attribute.String("om.cache.error", err.Error()))

						if ire.ErrorType == sdp.QueryError_NOTFOUND {
							span.SetStatus(codes.Ok, "cache hit: item not found")
						} else {
							span.SetStatus(codes.Ok, "cache hit: QueryError")
						}

						return false
					} else {
						// If it's an unknown error, convert it to SDP and skip this source
						errs = append(errs, &sdp.QueryError{
							UUID:        r.UUID,
							ErrorType:   sdp.QueryError_OTHER,
							ErrorString: err.Error(),
							Scope:       r.Scope,
							SourceName:  src.Name(),
							ItemType:    r.Type,
						})

						span.SetAttributes(attribute.String("om.cache.error", err.Error()))

						span.SetStatus(codes.Ok, "cache hit: QueryError")

						return false
					}
				} else {
					span.SetAttributes(
						attribute.Int("om.source.numItems", len(cachedItems)),
						attribute.Bool("om.source.cache", true),
					)

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
						err = &sdp.QueryError{
							ErrorType:   sdp.QueryError_NOTFOUND,
							ErrorString: "source is not searchable",
						}
					}
				}

				sourceDuration = time.Since(start)
			}(ctx)

			span.SetAttributes(
				attribute.Int("om.source.numItems", len(resultItems)),
				attribute.Bool("om.source.cache", false),
			)

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

				if sdpErr, ok := err.(*sdp.QueryError); ok {
					// Add details if they aren't populated
					if sdpErr.Scope == "" {
						sdpErr.Scope = r.Scope
					}
					sdpErr.UUID = r.UUID
					sdpErr.ItemType = src.Type()
					sdpErr.ResponderName = e.Name
					sdpErr.SourceName = src.Name()

					errs = append(errs, sdpErr)
				} else {
					errs = append(errs, &sdp.QueryError{
						UUID:        r.UUID,
						ErrorType:   sdp.QueryError_OTHER,
						ErrorString: err.Error(),
						Scope:       r.Scope,
						SourceName:  src.Name(),
						ItemType:    r.Type,
					})
				}
			}

			// For each found item, add more details
			//
			// Use the index here to ensure that we're actually editing the
			// right thing
			for _, item := range resultItems {
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
					SourceQuery:           r,
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
// *sdp.QueryError with a Type of NOTFOUND, this means that it was queried
// successfully but it simply doesn't exist
func considerFailed(err error) bool {
	if err == nil {
		return false
	} else {
		if sdperr, ok := err.(*sdp.QueryError); ok {
			if sdperr.ErrorType == sdp.QueryError_NOTFOUND {
				return false
			} else {
				return true
			}
		} else {
			return true
		}
	}
}
