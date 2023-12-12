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
	log "github.com/sirupsen/logrus"
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

	var deadlineOverride bool

	// If there is no deadline OR further in the future than MaxRequestTimeout, clamp the deadline to MaxRequestTimeout
	maxRequestDeadline := time.Now().Add(e.MaxRequestTimeout)
	if query.Deadline == nil || query.Deadline.AsTime().After(maxRequestDeadline) {
		query.Deadline = timestamppb.New(maxRequestDeadline)
		deadlineOverride = true
		log.WithContext(ctx).WithField("ovm.deadline", query.Deadline.AsTime()).Debug("capping deadline to MaxRequestTimeout")
	}

	// Add the query timeout to the context stack
	ctx, cancel := query.TimeoutContext(ctx)
	defer cancel()

	numExpandedQueries := len(e.sh.ExpandQuery(query))

	// Extract and parse the UUID
	u, uuidErr := uuid.FromBytes(query.UUID)

	span.SetAttributes(
		attribute.Int("ovm.discovery.numExpandedQueries", numExpandedQueries),
		attribute.String("ovm.sdp.uuid", u.String()),
		attribute.String("ovm.sdp.type", query.Type),
		attribute.String("ovm.sdp.method", query.Method.String()),
		attribute.String("ovm.sdp.query", query.Query),
		attribute.String("ovm.sdp.scope", query.Scope),
		attribute.String("ovm.sdp.deadline", query.Deadline.AsTime().String()),
		attribute.Bool("ovm.sdp.deadlineOverridden", deadlineOverride),
		attribute.Bool("ovm.sdp.queryIgnoreCache", query.IgnoreCache),
	)

	if query.RecursionBehaviour != nil {
		span.SetAttributes(
			attribute.Int("ovm.sdp.linkDepth", int(query.RecursionBehaviour.LinkDepth)),
			attribute.Bool("ovm.sdp.followOnlyBlastPropagation", query.RecursionBehaviour.FollowOnlyBlastPropagation),
		)
	}

	if numExpandedQueries == 0 {
		// If we don't have any relevant sources, exit
		return
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

	qt := QueryTracker{
		Query:   query,
		Engine:  e,
		Context: ctx,
		Cancel:  cancel,
	}

	if uuidErr == nil {
		e.TrackQuery(u, &qt)
		defer e.DeleteTrackedQuery(u)
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
			attribute.String("ovm.sdp.errorType", "OTHER"),
			attribute.String("ovm.sdp.errorString", err.Error()),
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
			defer LogRecoverToReturn(ctx, "ExecuteQuery outer")
			e.executionPool.Go(func() {
				defer LogRecoverToReturn(ctx, "ExecuteQuery inner")
				defer wg.Done()
				var queryItems []*sdp.Item
				var queryErrors []*sdp.QueryError
				numSources.Add(1)

				// query all sources
				queryItems, queryErrors = e.Execute(ctx, q, sources)

				for _, i := range queryItems {
					// If the main query had a linkDepth of greater than zero it means we
					// need to keep linking, this means that we need to pass down all of the
					// subject info along with the number of remaining links. If the link
					// depth is zero then we just pass then back in their normal form as we
					// won't be executing them
					if query.RecursionBehaviour.GetLinkDepth() > 0 {
						for _, liq := range i.LinkedItemQueries {
							liq.Query.RecursionBehaviour = &sdp.Query_RecursionBehaviour{
								LinkDepth:                  query.RecursionBehaviour.GetLinkDepth() - 1,
								FollowOnlyBlastPropagation: query.RecursionBehaviour.GetFollowOnlyBlastPropagation(),
							}
							if query.RecursionBehaviour.GetFollowOnlyBlastPropagation() && !liq.BlastPropagation.GetOut() {
								// we're only following blast propagation, so do not link this item further
								// TODO: we might want to drop the link completely if this returns too much
								// information, but that could risk missing revlinks
								liq.Query.RecursionBehaviour.LinkDepth = 0
							}
							liq.Query.IgnoreCache = query.IgnoreCache
							liq.Query.Deadline = query.Deadline
							liq.Query.UUID = query.UUID
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

// Execute Runs the query against known sources in priority order. If nothing was
// found, returns the first error. This returns a slice if items for
// convenience, but this should always be of length 1 or 0
func (e *Engine) Execute(ctx context.Context, q *sdp.Query, relevantSources []Source) ([]*sdp.Item, []*sdp.QueryError) {
	sources := relevantSources
	if q.Method == sdp.QueryMethod_SEARCH {
		sources = make([]Source, 0)

		// Filter further by searchability
		for _, source := range relevantSources {
			if searchable, ok := source.(SearchableSource); ok {
				sources = append(sources, searchable)
			}
		}
	}

	return e.callSources(ctx, q, sources)
}

func (e *Engine) callSources(ctx context.Context, q *sdp.Query, relevantSources []Source) ([]*sdp.Item, []*sdp.QueryError) {
	ctx, span := tracer.Start(ctx, "CallSources", trace.WithAttributes(
		attribute.String("ovm.source.queryMethod", q.Method.String()),
	))
	defer span.End()

	// Check that our context is okay before doing anything expensive
	if ctx.Err() != nil {
		return nil, []*sdp.QueryError{
			{
				UUID:          q.UUID,
				ErrorType:     sdp.QueryError_OTHER,
				ErrorString:   ctx.Err().Error(),
				Scope:         q.Scope,
				ResponderName: e.Name,
				ItemType:      q.Type,
			},
		}
	}

	items := make([]*sdp.Item, 0)
	errs := make([]*sdp.QueryError, 0)

	// We want to avoid having a Get and a List running at the same time, we'd
	// rather run the List first, populate the cache, then have the Get just
	// grab the value from the cache. To this end we use a GetListMutex to allow
	// a List to block all subsequent Get queries until it is done
	switch q.Method {
	case sdp.QueryMethod_GET:
		e.gfm.GetLock(q.Scope, q.Type)
		defer e.gfm.GetUnlock(q.Scope, q.Type)
	case sdp.QueryMethod_LIST:
		e.gfm.ListLock(q.Scope, q.Type)
		defer e.gfm.ListUnlock(q.Scope, q.Type)
	case sdp.QueryMethod_SEARCH:
		// We don't need to lock for a search since they are independent and
		// will only ever have a cache hit if the query is identical
	}

	span.SetAttributes(
		attribute.String("ovm.source.queryType", q.Type),
		attribute.String("ovm.source.queryScope", q.Scope),
	)

	for _, src := range relevantSources {
		if func() bool {
			// start querying the source after a cache miss
			if len(relevantSources) > 1 {
				ctx, span = tracer.Start(ctx, src.Name(), trace.WithAttributes(
					attribute.String("ovm.source.method", q.Method.String()),
					attribute.String("ovm.source.queryMethod", q.Method.String()),
					attribute.String("ovm.source.queryType", q.Type),
					attribute.String("ovm.source.queryScope", q.Scope)),
				)
				defer span.End()
			} else {
				span.SetName(fmt.Sprintf("CallSources: %v", src.Name()))
				span.SetAttributes(
					attribute.String("ovm.source.name", src.Name()),
				)
			}

			var resultItems []*sdp.Item
			var err error
			var sourceDuration time.Duration

			start := time.Now()

			switch q.Method {
			case sdp.QueryMethod_GET:
				var newItem *sdp.Item

				newItem, err = src.Get(ctx, q.Scope, q.Query, q.IgnoreCache)

				if err == nil {
					resultItems = []*sdp.Item{newItem}
				}
			case sdp.QueryMethod_LIST:
				resultItems, err = src.List(ctx, q.Scope, q.IgnoreCache)
			case sdp.QueryMethod_SEARCH:
				if searchableSrc, ok := src.(SearchableSource); ok {
					resultItems, err = searchableSrc.Search(ctx, q.Scope, q.Query, q.IgnoreCache)
				} else {
					err = &sdp.QueryError{
						ErrorType:   sdp.QueryError_NOTFOUND,
						ErrorString: "source is not searchable",
					}
				}
			}

			sourceDuration = time.Since(start)

			span.SetAttributes(
				attribute.Int("ovm.source.numItems", len(resultItems)),
				attribute.Bool("ovm.source.cache", false),
			)

			if considerFailed(err) {
				span.SetStatus(codes.Error, err.Error())
			}

			if err != nil {
				span.SetAttributes(attribute.String("ovm.source.error", err.Error()))

				if sdpErr, ok := err.(*sdp.QueryError); ok {
					// Add details if they aren't populated
					scope := sdpErr.Scope
					if scope == "" {
						scope = q.Scope
					}
					errs = append(errs, &sdp.QueryError{
						UUID:          q.UUID,
						ErrorType:     sdpErr.ErrorType,
						ErrorString:   sdpErr.ErrorString,
						Scope:         scope,
						SourceName:    src.Name(),
						ItemType:      src.Type(),
						ResponderName: e.Name,
					})
				} else {
					errs = append(errs, &sdp.QueryError{
						UUID:          q.UUID,
						ErrorType:     sdp.QueryError_OTHER,
						ErrorString:   err.Error(),
						Scope:         q.Scope,
						SourceName:    src.Name(),
						ItemType:      q.Type,
						ResponderName: e.Name,
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
					SourceQuery:           q,
				}

				// Mark the item as hidden if the source is a hidden source
				if hs, ok := src.(HiddenSource); ok {
					item.Metadata.Hidden = hs.Hidden()
				}
			}

			items = append(items, resultItems...)

			if q.Method == sdp.QueryMethod_GET {
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
