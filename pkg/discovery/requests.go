package discovery

import (
	"github.com/dylanratcliffe/sdp-go"
	log "github.com/sirupsen/logrus"
)

// NewItemRequestHandler Returns a function whose job is to handle a single
// request. This includes responses, linking etc.
func (e *Engine) NewItemRequestHandler(sources []Source) func(req *sdp.ItemRequest) {
	return func(itemRequest *sdp.ItemRequest) {
		if !IsWildcard(itemRequest.GetType()) {
			// Check that we actually have a source for that type
			hasType := false

			for _, source := range sources {
				if source.Type() == itemRequest.GetType() {
					hasType = true
				}
			}

			if !hasType {
				// If we don't have a relevant type, just exit
				return
			}
		}

		// Respond saying we've got it
		responder := sdp.ResponseSender{
			ResponseSubject: itemRequest.ResponseSubject,
		}

		responder.Start(
			e.natsConnection,
			e.Name,
		)

		log.WithFields(log.Fields{
			"type":      itemRequest.Type,
			"method":    itemRequest.Method,
			"query":     itemRequest.Query,
			"linkDepth": itemRequest.LinkDepth,
			"context":   itemRequest.Context,
		}).Info("Received request")

		requestTracker := RequestTracker{
			Requests: e.expandRequest(itemRequest), // Expand type wildcard if required
			Engine:   e,
		}

		_, err := requestTracker.Execute()

		// If all failed then return an error
		if err != nil {
			if ire, ok := err.(*sdp.ItemRequestError); ok {
				responder.Error(ire)
			} else {
				ire = &sdp.ItemRequestError{
					ErrorType:   sdp.ItemRequestError_OTHER,
					ErrorString: err.Error(),
					Context:     itemRequest.Context,
				}

				responder.Error(ire)
			}

			logEntry := log.WithFields(log.Fields{
				"errorType":        "OTHER",
				"errorString":      err.Error(),
				"requestType":      itemRequest.Type,
				"requestMethod":    itemRequest.Method,
				"requestQuery":     itemRequest.Query,
				"requestLinkDepth": itemRequest.LinkDepth,
				"requestContext":   itemRequest.Context,
			})

			if ire, ok := err.(*sdp.ItemRequestError); ok && ire.ErrorType == sdp.ItemRequestError_OTHER {
				logEntry.Error("Request ended with unknown error")
			} else {
				logEntry.Info("Request ended with error")
			}
		} else {
			responder.Done()

			log.WithFields(log.Fields{
				"type":      itemRequest.Type,
				"method":    itemRequest.Method,
				"query":     itemRequest.Query,
				"linkDepth": itemRequest.LinkDepth,
				"context":   itemRequest.Context,
			}).Info("Request complete")
		}

	}
}

// ExecuteRequest Executes a single request and returns the results without any
// linking
func (e *Engine) ExecuteRequest(req *sdp.ItemRequest) ([]*sdp.Item, error) {
	var requestItem *sdp.Item
	var requestError error

	requestItems := make([]*sdp.Item, 0)

	// Make the request of all sources
	switch req.GetMethod() {
	case sdp.RequestMethod_GET:
		requestItem, requestError = e.Get(req.GetType(), req.GetContext(), req.GetQuery())
		requestItems = append(requestItems, requestItem)
	case sdp.RequestMethod_FIND:
		requestItems, requestError = e.Find(req.GetType(), req.GetContext())
	case sdp.RequestMethod_SEARCH:
		requestItems, requestError = e.Search(req.GetType(), req.GetContext(), req.GetQuery())
	}

	// If there was an error in the request then simply return
	if requestError != nil {
		return nil, sdp.NewItemRequestError(requestError)
	}

	for _, i := range requestItems {
		// If the main request had a linkDepth of great than zero it means we
		// need to keep linking, this means that we need to pass down all of the
		// subject info along with the number of remaining links. If the link
		// depth is zero then we just pass then back in their normal form as we
		// won't be executing them
		if req.GetLinkDepth() > 0 {
			for _, lir := range i.LinkedItemRequests {
				lir.LinkDepth = req.GetLinkDepth() - 1
				lir.ItemSubject = req.GetItemSubject()
				lir.ResponseSubject = req.GetResponseSubject()
			}
		}

		// Assign the item request
		if i.Metadata != nil {
			i.Metadata.SourceRequest = req
		}
	}

	return requestItems, requestError
}

// expandRequest Expands requests with wildcards to be list of requests, one for
// each type that the engine supports
func (e *Engine) expandRequest(request *sdp.ItemRequest) []*sdp.ItemRequest {
	if IsWildcard(request.GetType()) {
		requests := make([]*sdp.ItemRequest, 0)

		for typ := range e.sourceMap {
			newRequest := sdp.ItemRequest{
				Type:            typ,
				Method:          request.GetMethod(),
				Query:           request.GetQuery(),
				Context:         request.GetContext(),
				ItemSubject:     request.GetItemSubject(),
				ResponseSubject: request.GetResponseSubject(),
				LinkDepth:       request.GetLinkDepth(),
			}

			requests = append(requests, &newRequest)
		}

		return requests
	}

	return []*sdp.ItemRequest{request}
}
