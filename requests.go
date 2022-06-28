package discovery

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
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
	if !e.WillRespond(itemRequest) {
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

	_, err := requestTracker.Execute()

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

// ExecuteRequest Executes a single request and returns the results without any
// linking
func (e *Engine) ExecuteRequest(ctx context.Context, req *sdp.ItemRequest) ([]*sdp.Item, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	items := make(chan *sdp.Item)
	errors := make(chan error)
	done := make(chan bool)
	wg := sync.WaitGroup{}

	expanded := e.ExpandRequest(req)

	if len(expanded) == 0 {
		return []*sdp.Item{}, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: "No matching sources found",
			Context:     req.Context,
		}
	}

	allItems := make([]*sdp.Item, 0)
	allErrors := make([]error, 0)

	go func() {
		for item := range items {
			allItems = append(allItems, item)
		}
		done <- true
	}()

	go func() {
		for err := range errors {
			allErrors = append(allErrors, err)
		}
		done <- true
	}()

	for request, sources := range expanded {
		wg.Add(1)

		e.throttle.Lock()

		go func(r *sdp.ItemRequest, sources []Source) {
			defer wg.Done()
			defer e.throttle.Unlock()
			var requestItems []*sdp.Item
			var requestError error

			// Make the request of all sources
			switch req.GetMethod() {
			case sdp.RequestMethod_GET:
				var requestItem *sdp.Item

				requestItem, requestError = e.Get(ctx, r, sources)
				requestItems = append(requestItems, requestItem)
			case sdp.RequestMethod_FIND:
				requestItems, requestError = e.Find(ctx, r, sources)
			case sdp.RequestMethod_SEARCH:
				requestItems, requestError = e.Search(ctx, r, sources)
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

				items <- i
			}
			errors <- requestError
		}(request, sources)
	}

	// Wait for all requests to complete
	wg.Wait()

	// Close channels as no more messages are coming
	close(items)
	close(errors)

	// Wait for all channels to empty and be added to slices
	<-done
	<-done

	// If all failed then return first error
	if len(allErrors) == len(expanded) && len(allErrors) > 0 {
		return allItems, allErrors[0]
	}

	return allItems, nil
}

// WillRespond Performs a cursory check to see if it's likely that this engine
// will respond to a given request based on the type and context of the request.
// Should be used an initial check before proceeding to detailed processing.
func (e *Engine) WillRespond(req *sdp.ItemRequest) bool {
	for _, src := range e.Sources() {
		typeMatch := (req.Type == src.Type() || IsWildcard(req.Type))

		for _, context := range src.Contexts() {
			contextMatch := (req.Context == context || IsWildcard(req.Context) || IsWildcard(context))

			if contextMatch && typeMatch {
				return true
			}
		}
	}

	return false
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
