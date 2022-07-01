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
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

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
func (e *Engine) ItemRequestHandler(itemRequest *sdp.ItemRequest) {
	go e.HandleItemRequest(itemRequest)
}

// HandleItemRequest Handles a single request. This includes responses, linking
// etc.
func (e *Engine) HandleItemRequest(itemRequest *sdp.ItemRequest) {
	if len(e.ExpandRequest(itemRequest)) == 0 {
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
		pub,
		e.Name,
	)

	// Extract and parse the UUID
	reqUUID, uuidErr := uuid.FromBytes(itemRequest.UUID)

	log.WithFields(log.Fields{
		"type":              itemRequest.Type,
		"method":            itemRequest.Method,
		"query":             itemRequest.Query,
		"linkDepth":         itemRequest.LinkDepth,
		"context":           itemRequest.Context,
		"timeout":           itemRequest.Timeout.AsDuration().String(),
		"timeoutOverridden": timeoutOverride,
		"uuid":              reqUUID.String(),
		"ignoreCache":       itemRequest.IgnoreCache,
	}).Info("Received request")

	requestTracker := RequestTracker{
		Request: itemRequest,
		Engine:  e,
	}

	if uuidErr == nil {
		e.TrackRequest(reqUUID, &requestTracker)
	}

	_, _, err := requestTracker.Execute()

	// If all failed then return an error
	if err != nil {
		if err == context.Canceled {
			responder.Cancel()
		} else {
			responder.Error()
		}

		logEntry := log.WithFields(log.Fields{
			"errorType":          "OTHER",
			"errorString":        err.Error(),
			"requestType":        itemRequest.Type,
			"requestMethod":      itemRequest.Method,
			"requestQuery":       itemRequest.Query,
			"requestLinkDepth":   itemRequest.LinkDepth,
			"requestContext":     itemRequest.Context,
			"requestTimeout":     itemRequest.Timeout.AsDuration().String(),
			"requestUUID":        reqUUID.String(),
			"requestIgnoreCache": itemRequest.IgnoreCache,
		})

		if ire, ok := err.(*sdp.ItemRequestError); ok && ire.ErrorType == sdp.ItemRequestError_OTHER {
			logEntry.Error("Request ended with unknown error")
		} else {
			logEntry.Info("Request ended with error")
		}
	} else {
		responder.Done()

		log.WithFields(log.Fields{
			"type":        itemRequest.Type,
			"method":      itemRequest.Method,
			"query":       itemRequest.Query,
			"linkDepth":   itemRequest.LinkDepth,
			"context":     itemRequest.Context,
			"timeout":     itemRequest.Timeout.AsDuration().String(),
			"uuid":        reqUUID.String(),
			"ignoreCache": itemRequest.IgnoreCache,
		}).Info("Request complete")
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
// requered the channels can be nil
func (e *Engine) ExecuteRequest(ctx context.Context, req *sdp.ItemRequest, items chan<- *sdp.Item, errs chan<- *sdp.ItemRequestError) error {
	// Make sure we close channels once we're done
	defer close(items)
	defer close(errs)

	if ctx.Err() != nil {
		return ctx.Err()
	}

	wg := sync.WaitGroup{}

	expanded := e.ExpandRequest(req)

	if len(expanded) == 0 {
		errs <- &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: "no matching sources found",
			Context:     req.Context,
		}

		return errors.New("no matching sources found")
	}

	// These are used to calcualte whether all sourcces have failed or not
	var numSources int
	var numErrs int

	for request, sources := range expanded {
		wg.Add(1)

		e.throttle.Lock()

		go func(r *sdp.ItemRequest, sources []Source) {
			defer wg.Done()
			defer e.throttle.Unlock()
			var requestItems []*sdp.Item
			var requestErrors []*sdp.ItemRequestError
			numSources++

			// Make the request of all sources
			switch req.GetMethod() {
			case sdp.RequestMethod_GET:
				requestItems, requestErrors = e.Get(ctx, r, sources)
			case sdp.RequestMethod_FIND:
				requestItems, requestErrors = e.Find(ctx, r, sources)
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
		}(request, sources)
	}

	// Wait for all requests to complete
	wg.Wait()

	// If all failed then return first error
	if numErrs == numSources {
		return errors.New("all sources failed")
	}

	return nil
}

// ExpandRequest Expands requests with wildcards to no longer contain wildcards.
// Meaning that if we support 5 types, and a request comes in with a wildcard
// type, this function will expand that request into 5 requests, one for each
// type.
//
// The same goes for contexts, if we have a request with a wildcard context, and
// a single source that supports 5 contexts, we will end up with 5 requests. The
// exception to this is if we have a source that supports all contexts, but is
// unable to list them. In this case there will still be some requests with
// wildcard contexts as they can't be expanded
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

		for _, sourceContext := range src.Contexts() {
			// Create a new request if:
			//
			// * The source supports all contexts, or
			// * The request context is a wildcard (and the source is not hidden), or
			// * The request context matches source context
			if IsWildcard(sourceContext) || (IsWildcard(request.Context) && !isHidden) || sourceContext == request.Context {
				var itemContext string

				// Choose the more specific context
				if IsWildcard(sourceContext) {
					itemContext = request.Context
				} else {
					itemContext = sourceContext
				}

				request := sdp.ItemRequest{
					Type:            src.Type(),
					Method:          request.Method,
					Query:           request.Query,
					Context:         itemContext,
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
