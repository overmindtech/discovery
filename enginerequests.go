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

// AllAdaptersFailedError Will be returned when all adapters have failed
type AllAdaptersFailedError struct {
	NumAdapters int
}

func (e AllAdaptersFailedError) Error() string {
	return fmt.Sprintf("all adapters (%v) failed", e.NumAdapters)
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
		// If we don't have any relevant adapters, exit
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
		e.EngineConfig.SourceName,
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
// linking. Will return an error if all adapters fail, or the Query couldn't be
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
		attribute.Int("ovm.adapter.numExpandedQueries", len(expanded)),
	)

	if len(expanded) == 0 {
		errs <- &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOSCOPE,
			ErrorString: "no matching adapters found",
			Scope:       query.GetScope(),
		}

		return errors.New("no matching adapters found")
	}

	// These are used to calculate whether all adapters have failed or not
	var numAdapters atomic.Int32
	var numErrs int

	// Since we need to wait for only the processing of this query's executions, we need a separate WaitGroup here
	// Overall MaxParallelExecutions evaluation is handled by e.executionPool
	wg := sync.WaitGroup{}
	expandedMutex := sync.RWMutex{}
	expandedMutex.RLock()
	for q, adapters := range expanded {
		wg.Add(1)
		// localize values for the closure below
		localQ, localAdapters := q, adapters

		var p *pool.Pool
		if localQ.GetMethod() == sdp.QueryMethod_LIST {
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
				defer func() {
					// Delete our query from the map so that we can track which
					// ones are still running
					expandedMutex.Lock()
					defer expandedMutex.Unlock()
					delete(expanded, localQ)

					// Mark the work as done
					wg.Done()
				}()
				defer func() {
					if localQ.GetMethod() == sdp.QueryMethod_LIST {
						listExecutionPoolCount.Add(-1)
					} else {
						getExecutionPoolCount.Add(-1)
					}
				}()
				var queryItems []*sdp.Item
				var queryErrors []*sdp.QueryError
				numAdapters.Add(1)

				// If the context is cancelled, don't even bother doing
				// anything. Since the `p.Go` will block, it's possible that if
				// the pool was exhausted, the context could be cancelled before
				// the goroutine is executed
				if ctx.Err() != nil {
					return
				}

				// query all adapters
				queryItems, queryErrors = e.Execute(ctx, localQ, localAdapters)

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
					if localQ != nil {
						numErrs++

						if errs != nil {
							errs <- e
						}
					}
				}
			})
		}()
	}
	expandedMutex.RUnlock()

	waitGroupDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitGroupDone)
	}()

	select {
	case <-waitGroupDone:
		// All adapters have finished
	case <-ctx.Done():
		// The context was cancelled, this should have propagated to all the
		// adapters and therefore we should see the wait group finish very
		// quickly now. We will check this though to make sure
		longRunningAdaptersTimeout := 10 * time.Second

		// Wait for the wait group, but ping the logs if it's taking
		// too long
		func() {
			for {
				select {
				case <-waitGroupDone:
					return
				case <-time.After(longRunningAdaptersTimeout):
					// If we're here, then the wait group didn't finish in time
					expandedMutex.RLock()
					for q, adapters := range expanded {
						adapterNames := make([]string, len(adapters))
						for i, a := range adapters {
							adapterNames[i] = a.Name()
						}
						log.WithContext(ctx).WithFields(log.Fields{
							"ovm.query.uuid":     q.ParseUuid().String(),
							"ovm.query.type":     q.GetType(),
							"ovm.query.scope":    q.GetScope(),
							"ovm.query.method":   q.GetMethod().String(),
							"ovm.query.adapters": adapterNames,
						}).Errorf("Wait group still running %v after context cancelled", longRunningAdaptersTimeout)
					}
					expandedMutex.RUnlock()
				}
			}
		}()
	}

	// If all failed then return first error
	if numAdaptersInt := numAdapters.Load(); numErrs == int(numAdaptersInt) {
		return AllAdaptersFailedError{
			NumAdapters: int(numAdaptersInt),
		}
	}

	return nil
}

// Execute Runs the query against known adapters in priority order. If nothing was
// found, returns the first error. This returns a slice if items for
// convenience, but this should always be of length 1 or 0
func (e *Engine) Execute(ctx context.Context, q *sdp.Query, relevantAdapters []Adapter) ([]*sdp.Item, []*sdp.QueryError) {
	adapters := relevantAdapters
	if q.GetMethod() == sdp.QueryMethod_SEARCH {
		adapters = make([]Adapter, 0)

		// Filter further by searchability
		for _, adapter := range relevantAdapters {
			if searchable, ok := adapter.(SearchableAdapter); ok {
				adapters = append(adapters, searchable)
			}
		}
	}

	return e.callAdapters(ctx, q, adapters)
}

func (e *Engine) callAdapters(ctx context.Context, q *sdp.Query, relevantAdapters []Adapter) ([]*sdp.Item, []*sdp.QueryError) {
	ctx, span := tracer.Start(ctx, "CallAdapters", trace.WithAttributes(
		attribute.String("ovm.adapter.queryMethod", q.GetMethod().String()),
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
				ResponderName: e.EngineConfig.SourceName,
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
		attribute.String("ovm.adapter.queryType", q.GetType()),
		attribute.String("ovm.adapter.queryScope", q.GetScope()),
	)

	for _, adapter := range relevantAdapters {
		if func() bool {
			// start querying the adapter after a cache miss
			ctx, span := tracer.Start(ctx, adapter.Name(), trace.WithAttributes(
				attribute.String("ovm.adapter.method", q.GetMethod().String()),
				attribute.String("ovm.adapter.queryMethod", q.GetMethod().String()),
				attribute.String("ovm.adapter.queryType", q.GetType()),
				attribute.String("ovm.adapter.queryScope", q.GetScope()),
				attribute.String("ovm.adapter.name", adapter.Name()),
				attribute.String("ovm.adapter.query", q.GetQuery()),
			))
			defer span.End()

			// Ensure that the span is closed when the context is done. This is based on
			// the assumption that some adapters may not respect the context deadline and
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
			var adapterDuration time.Duration

			start := time.Now()

			switch q.GetMethod() {
			case sdp.QueryMethod_GET:
				var newItem *sdp.Item

				newItem, err = adapter.Get(ctx, q.GetScope(), q.GetQuery(), q.GetIgnoreCache())

				if err == nil {
					resultItems = []*sdp.Item{newItem}
				}
			case sdp.QueryMethod_LIST:
				resultItems, err = adapter.List(ctx, q.GetScope(), q.GetIgnoreCache())
			case sdp.QueryMethod_SEARCH:
				if searchableAdapter, ok := adapter.(SearchableAdapter); ok {
					resultItems, err = searchableAdapter.Search(ctx, q.GetScope(), q.GetQuery(), q.GetIgnoreCache())
				} else {
					err = &sdp.QueryError{
						ErrorType:   sdp.QueryError_NOTFOUND,
						ErrorString: "adapter is not searchable",
					}
				}
			}

			adapterDuration = time.Since(start)

			span.SetAttributes(
				attribute.Int("ovm.adapter.numItems", len(resultItems)),
				attribute.Bool("ovm.adapter.cache", false),
				attribute.String("ovm.adapter.duration", adapterDuration.String()),
			)

			if considerFailed(err) {
				span.SetStatus(codes.Error, err.Error())
			}

			if err != nil {
				span.SetAttributes(attribute.String("ovm.adapter.error", err.Error()))

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
						SourceName:    adapter.Name(),
						ItemType:      adapter.Type(),
						ResponderName: e.EngineConfig.SourceName,
					})
				} else {
					errs = append(errs, &sdp.QueryError{
						UUID:          q.GetUUID(),
						ErrorType:     sdp.QueryError_OTHER,
						ErrorString:   err.Error(),
						Scope:         q.GetScope(),
						SourceName:    adapter.Name(),
						ItemType:      q.GetType(),
						ResponderName: e.EngineConfig.SourceName,
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
					SourceDuration:        durationpb.New(adapterDuration),
					SourceDurationPerItem: durationpb.New(time.Duration(adapterDuration.Nanoseconds() / int64(len(resultItems)))),
					SourceName:            adapter.Name(),
					SourceQuery:           q,
				}

				// Mark the item as hidden if the adapter is hidden
				if hs, ok := adapter.(HiddenAdapter); ok {
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
			// `get` queries only return the first adapter results
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
