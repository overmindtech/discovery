package discovery

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/overmindtech/sdp-go"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
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

// ItemRequestHandler Calls HandleItemRequest but in a goroutine so that it can
// happen in parallel
func (e *Engine) ItemRequestHandler(ctx context.Context, itemRequest *sdp.ItemRequest) {
	go e.HandleItemRequest(ctx, itemRequest)
}

// HandleItemRequest Handles a single request. This includes responses, linking
// etc.
func (e *Engine) HandleItemRequest(ctx context.Context, itemRequest *sdp.ItemRequest) {
	ctx, span := tracer.Start(ctx, "HandleItemRequest")
	defer span.End()

	if len(e.ExpandRequest(itemRequest)) == 0 {
		// If we don't have any relevant sources, exit
		span.AddEvent("no relevant sources, nothing to do")
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
// them along with the error, rather than paiing the results back along channels
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

	expanded := e.ExpandRequest(req)

	if len(expanded) == 0 {
		errs <- &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOSCOPE,
			ErrorString: "no matching sources found",
			Scope:       req.Scope,
		}

		return errors.New("no matching sources found")
	}

	// These are used to calculate whether all sources have failed or not
	var numSources int
	var numErrs int

	for request, sources := range expanded {
		wg.Add(1)

		e.throttle.Lock()

		go func(ctx context.Context, r *sdp.ItemRequest, sources []Source) {
			defer wg.Done()
			defer e.throttle.Unlock()
			var requestItems []*sdp.Item
			var requestErrors []*sdp.ItemRequestError
			numSources++

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
				// If the main request had a linkDepth of great than zero it means we
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
	if numErrs == numSources {
		return AllSourcesFailedError{
			NumSources: numSources,
		}
	}

	return nil
}

// ExpandRequest Expands requests with wildcards to no longer contain wildcards.
// Meaning that if we support 5 types, and a request comes in with a wildcard
// type, this function will expand that request into 5 requests, one for each
// type.
//
// The same goes for scopes, if we have a request with a wildcard scope, and
// a single source that supports 5 scopes, we will end up with 5 requests. The
// exception to this is if we have a source that supports all scopes, but is
// unable to list them. In this case there will still be some requests with
// wildcard scopes as they can't be expanded
//
// This functions returns a map of requests with the sources that they should be
// run against
func (e *Engine) ExpandRequest(request *sdp.ItemRequest) map[*sdp.ItemRequest][]Source {
	requests := make(map[string]*struct {
		Request *sdp.ItemRequest
		Sources []Source
	})

	var checkSources []Source

	if IsWildcard(request.Type) {
		// If the request has a wildcard type, all non-hidden sources might try
		// to respond
		checkSources = e.NonHiddenSources()
	} else {
		// If the type is specific, pull just sources for that type
		checkSources = e.sourceMap[request.Type]
	}

	for _, src := range checkSources {
		// Calculate if the source is hidden
		var isHidden bool

		if hs, ok := src.(HiddenSource); ok {
			isHidden = hs.Hidden()
		}

		for _, sourceScope := range src.Scopes() {
			// Create a new request if:
			//
			// * The source supports all scopes, or
			// * The request scope is a wildcard (and the source is not hidden), or
			// * The request scope matches source scope
			if IsWildcard(sourceScope) || (IsWildcard(request.Scope) && !isHidden) || sourceScope == request.Scope {
				var scope string

				// Choose the more specific scope
				if IsWildcard(sourceScope) {
					scope = request.Scope
				} else {
					scope = sourceScope
				}

				request := sdp.ItemRequest{
					Type:            src.Type(),
					Method:          request.Method,
					Query:           request.Query,
					Scope:           scope,
					ItemSubject:     request.ItemSubject,
					ResponseSubject: request.ResponseSubject,
					LinkDepth:       request.LinkDepth,
					IgnoreCache:     request.IgnoreCache,
					UUID:            request.UUID,
					Timeout:         request.Timeout,
				}

				hash, err := requestHash(&request)

				if err == nil {
					if existing, ok := requests[hash]; ok {
						existing.Sources = append(existing.Sources, src)
					} else {
						requests[hash] = &struct {
							Request *sdp.ItemRequest
							Sources []Source
						}{
							Request: &request,
							Sources: []Source{
								src,
							},
						}
					}
				}
			}
		}
	}

	// Convert back to final map
	finalMap := make(map[*sdp.ItemRequest][]Source)
	for _, expanded := range requests {
		finalMap[expanded.Request] = expanded.Sources
	}

	return finalMap
}

// requestHash Calculates a hash for a given request which can be used to
// determine if two requests are identical
func requestHash(req *sdp.ItemRequest) (string, error) {
	hash := sha1.New()

	// Marshall to bytes so that we can use sha1 to compare the raw binary
	b, err := proto.Marshal(req)

	if err != nil {
		return "", err
	}

	return base64.URLEncoding.EncodeToString(hash.Sum(b)), nil
}
