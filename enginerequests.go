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
	"github.com/sourcegraph/conc/pool"
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
	if query.GetDeadline() == nil || query.GetDeadline().AsTime().After(maxRequestDeadline) {
		query.Deadline = timestamppb.New(maxRequestDeadline)
		deadlineOverride = true
		log.WithContext(ctx).WithField("ovm.deadline", query.GetDeadline().AsTime()).Debug("capping deadline to MaxRequestTimeout")
	}

	// Add the query timeout to the context stack
	ctx, cancel := query.TimeoutContext(ctx)
	defer cancel()

	numExpandedQueries := len(e.sh.ExpandQuery(query))

	// Extract and parse the UUID
	u, uuidErr := uuid.FromBytes(query.GetUUID())

	span.SetAttributes(
		attribute.Int("ovm.discovery.numExpandedQueries", numExpandedQueries),
		attribute.String("ovm.sdp.uuid", u.String()),
		attribute.String("ovm.sdp.type", query.GetType()),
		attribute.String("ovm.sdp.method", query.GetMethod().String()),
		attribute.String("ovm.sdp.query", query.GetQuery()),
		attribute.String("ovm.sdp.scope", query.GetScope()),
		attribute.String("ovm.sdp.deadline", query.GetDeadline().AsTime().String()),
		attribute.Bool("ovm.sdp.deadlineOverridden", deadlineOverride),
		attribute.Bool("ovm.sdp.queryIgnoreCache", query.GetIgnoreCache()),
	)

	if query.GetRecursionBehaviour() != nil {
		span.SetAttributes(
			attribute.Int("ovm.sdp.linkDepth", int(query.GetRecursionBehaviour().GetLinkDepth())),
			attribute.Bool("ovm.sdp.followOnlyBlastPropagation", query.GetRecursionBehaviour().GetFollowOnlyBlastPropagation()),
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
		span.SetAttributes(attribute.Bool("ovm.nats.connected", true))
		pub = e.natsConnection
	} else {
		span.SetAttributes(attribute.Bool("ovm.nats.connected", false))
		pub = NilConnection{}
	}

	ru := uuid.New()
	responder.Start(
		ctx,
		pub,
		e.Name,
		ru,
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
		if errors.Is(err, context.Canceled) {
			responder.CancelWithContext(ctx)
		} else {
			responder.ErrorWithContext(ctx)
		}

		span.SetAttributes(
			attribute.String("ovm.sdp.errorType", "OTHER"),
			attribute.String("ovm.sdp.errorString", err.Error()),
		)
	} else {
		responder.DoneWithContext(ctx)
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

var listExecutionPoolCount atomic.Int32
var getExecutionPoolCount atomic.Int32

// ExecuteQuery Executes a single Query and returns the results without any
// linking. Will return an error if all sources fail, or the Query couldn't be
// run.
//
// Items and errors will be sent to the supplied channels as they are found.
// Note that if these channels are not buffered, something will need to be
// receiving the results or this method will never finish. If results are not
// required the channels can be nil
func (e *Engine) ExecuteQuery(ctx context.Context, query *sdp.Query, items chan<- *sdp.Item, errs chan<- *sdp.QueryError) error {
	span := trace.SpanFromContext(ctx)

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

	span.SetAttributes(
		attribute.Int("ovm.source.numExpandedQueries", len(expanded)),
	)

	if len(expanded) == 0 {
		errs <- &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOSCOPE,
			ErrorString: "no matching sources found",
			Scope:       query.GetScope(),
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

		var p *pool.Pool
		if q.GetMethod() == sdp.QueryMethod_LIST {
			p = e.listExecutionPool
			listExecutionPoolCount.Add(1)
		} else {
			p = e.getExecutionPool
			getExecutionPoolCount.Add(1)
		}

		// push all queued items through a goroutine to avoid blocking `ExecuteQuery` from progressing
		// as `executionPool.Go()` will block once the max parallelism is hit
		go func() {
			// queue everything into the execution pool
			defer LogRecoverToReturn(ctx, "ExecuteQuery outer")
			span.SetAttributes(
				attribute.Int("ovm.discovery.listExecutionPoolCount", int(listExecutionPoolCount.Load())),
				attribute.Int("ovm.discovery.getExecutionPoolCount", int(getExecutionPoolCount.Load())),
			)
			p.Go(func() {
				defer LogRecoverToReturn(ctx, "ExecuteQuery inner")
				defer wg.Done()
				defer func() {
					if q.GetMethod() == sdp.QueryMethod_LIST {
						listExecutionPoolCount.Add(-1)
					} else {
						getExecutionPoolCount.Add(-1)
					}
				}()
				var queryItems []*sdp.Item
				var queryErrors []*sdp.QueryError
				numSources.Add(1)

				// query all sources
				queryItems, queryErrors = e.Execute(ctx, q, sources)

				for _, i := range queryItems {
					// Assign the source query
					if i.GetMetadata() != nil {
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
	if q.GetMethod() == sdp.QueryMethod_SEARCH {
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
		attribute.String("ovm.source.queryMethod", q.GetMethod().String()),
	))
	defer span.End()

	// Check that our context is okay before doing anything expensive
	if ctx.Err() != nil {
		span.RecordError(ctx.Err())

		return nil, []*sdp.QueryError{
			{
				UUID:          q.GetUUID(),
				ErrorType:     sdp.QueryError_OTHER,
				ErrorString:   ctx.Err().Error(),
				Scope:         q.GetScope(),
				ResponderName: e.Name,
				ItemType:      q.GetType(),
			},
		}
	}

	items := make([]*sdp.Item, 0)
	errs := make([]*sdp.QueryError, 0)

	// We want to avoid having a Get and a List running at the same time, we'd
	// rather run the List first, populate the cache, then have the Get just
	// grab the value from the cache. To this end we use a GetListMutex to allow
	// a List to block all subsequent Get queries until it is done
	switch q.GetMethod() {
	case sdp.QueryMethod_GET:
		e.gfm.GetLock(q.GetScope(), q.GetType())
		defer e.gfm.GetUnlock(q.GetScope(), q.GetType())
	case sdp.QueryMethod_LIST:
		e.gfm.ListLock(q.GetScope(), q.GetType())
		defer e.gfm.ListUnlock(q.GetScope(), q.GetType())
	case sdp.QueryMethod_SEARCH:
		// We don't need to lock for a search since they are independent and
		// will only ever have a cache hit if the query is identical
	}

	span.SetAttributes(
		attribute.String("ovm.source.queryType", q.GetType()),
		attribute.String("ovm.source.queryScope", q.GetScope()),
	)

	for _, src := range relevantSources {
		if func() bool {
			// start querying the source after a cache miss
			ctx, span := tracer.Start(ctx, src.Name(), trace.WithAttributes(
				attribute.String("ovm.source.method", q.GetMethod().String()),
				attribute.String("ovm.source.queryMethod", q.GetMethod().String()),
				attribute.String("ovm.source.queryType", q.GetType()),
				attribute.String("ovm.source.queryScope", q.GetScope()),
				attribute.String("ovm.source.name", src.Name()),
				attribute.String("ovm.source.query", q.GetQuery()),
			))
			defer span.End()

			// Ensure that the span is closed when the context is done. This is based on
			// the assumption that some sources may not respect the context deadline and
			// may run indefinitely. This ensures that we at least get notified about
			// it.
			go func() {
				<-ctx.Done()
				if ctx.Err() != nil {
					// get a fresh copy of the span to avoid data races
					span := trace.SpanFromContext(ctx)
					span.RecordError(ctx.Err())
					span.SetAttributes(
						attribute.Bool("ovm.discover.hang", true),
					)
					span.End()
				}
			}()

			var resultItems []*sdp.Item
			var err error
			var sourceDuration time.Duration

			start := time.Now()

			switch q.GetMethod() {
			case sdp.QueryMethod_GET:
				var newItem *sdp.Item

				newItem, err = src.Get(ctx, q.GetScope(), q.GetQuery(), q.GetIgnoreCache())

				if err == nil {
					resultItems = []*sdp.Item{newItem}
				}
			case sdp.QueryMethod_LIST:
				resultItems, err = src.List(ctx, q.GetScope(), q.GetIgnoreCache())
			case sdp.QueryMethod_SEARCH:
				if searchableSrc, ok := src.(SearchableSource); ok {
					resultItems, err = searchableSrc.Search(ctx, q.GetScope(), q.GetQuery(), q.GetIgnoreCache())
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
				attribute.String("ovm.source.duration", sourceDuration.String()),
			)

			if considerFailed(err) {
				span.SetStatus(codes.Error, err.Error())
			}

			if err != nil {
				span.SetAttributes(attribute.String("ovm.source.error", err.Error()))

				var sdpErr *sdp.QueryError
				if errors.As(err, &sdpErr) {
					// Add details if they aren't populated
					scope := sdpErr.GetScope()
					if scope == "" {
						scope = q.GetScope()
					}
					errs = append(errs, &sdp.QueryError{
						UUID:          q.GetUUID(),
						ErrorType:     sdpErr.GetErrorType(),
						ErrorString:   sdpErr.GetErrorString(),
						Scope:         scope,
						SourceName:    src.Name(),
						ItemType:      src.Type(),
						ResponderName: e.Name,
					})
				} else {
					errs = append(errs, &sdp.QueryError{
						UUID:          q.GetUUID(),
						ErrorType:     sdp.QueryError_OTHER,
						ErrorString:   err.Error(),
						Scope:         q.GetScope(),
						SourceName:    src.Name(),
						ItemType:      q.GetType(),
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

			if q.GetMethod() == sdp.QueryMethod_GET {
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
		var sdpErr *sdp.QueryError
		if errors.As(err, &sdpErr) {
			if sdpErr.GetErrorType() == sdp.QueryError_NOTFOUND {
				return false
			} else {
				return true
			}
		} else {
			return true
		}
	}
}
