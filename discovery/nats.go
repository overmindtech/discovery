package discovery

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dylanratcliffe/sdp-go"
	"google.golang.org/protobuf/proto"

	log "github.com/sirupsen/logrus"

	"github.com/nats-io/nats.go"
)

// UnlinkedItem represents an item that has not yet had its LinkedItemRequests
// processed. In this case we need to include with the item the subjects that it
// should be sent to once it has been linked
type UnlinkedItem struct {
	Item            *sdp.Item
	PublishSubjects []string
	LinkDepth       uint32
}

// RequestHandlerV2 Handles a number of requests. This includes linking,
// recursion and loop detection and should be invoked when a request is received
// from the NATS network
type RequestHandlerV2 struct {
	Requests           []*sdp.ItemRequest
	ResponseConnection *nats.EncodedConn
	Assistant          *Assistant

	uli   chan UnlinkedItem
	uliWG sync.WaitGroup

	findsMutex sync.RWMutex
	finds      map[string]*sdp.Item
}

// RegisterItem registers a new item with the handler. If this item has already
// been registered it return an error
func (r *RequestHandlerV2) RegisterItem(item *sdp.Item) error {
	r.findsMutex.Lock()
	defer r.findsMutex.Unlock()

	if _, found := r.finds[item.GloballyUniqueName()]; found {
		return fmt.Errorf("item %v has already been registered", item.GloballyUniqueName())
	}

	if r.finds == nil {
		r.finds = make(map[string]*sdp.Item)
	}

	r.finds[item.GloballyUniqueName()] = item

	return nil
}

// RegisteredItems Returns all registered items
func (r *RequestHandlerV2) RegisteredItems() []*sdp.Item {
	r.findsMutex.RLock()
	defer r.findsMutex.RUnlock()

	items := make([]*sdp.Item, 0)

	for _, i := range r.finds {
		items = append(items, i)
	}

	return items
}

func (r *RequestHandlerV2) queueUnlinkedItem(i UnlinkedItem) {
	r.uliWG.Add(1)

	r.uli <- i
}

// Run runs all of the requests that have currently been registed with the
// handler. This involves the following basic pipeline:
//
//  1. Run all initial requests in parallel. Items that are found from these
//  requests are unlinked.
//
//  2. Create a goroutine for each item that is found to resolve the linked items.
//  This involves executing all of the linked item requests and attaching
//  reference to the parent item. Any further items are fed back into this step
//  in order to have their linked items resolved assuming that the link depth
//  allows it
//
//  3. Once all queues are cleared and nothing more is running, return all found items
func (r *RequestHandlerV2) Run() ([]*sdp.Item, error) {
	var requestsWait sync.WaitGroup
	var errors []error
	var errorsMutex sync.Mutex

	// Protect from running on empty
	if len(r.Requests) == 0 {
		return nil, nil
	}

	// Create the channels
	r.uli = make(chan UnlinkedItem)

	// Populate the waitgroup with the initial number of requests
	requestsWait.Add(len(r.Requests))

	// Process requests
	for _, request := range r.Requests {
		go func(req *sdp.ItemRequest) {
			defer requestsWait.Done()

			// Run the request
			items, err := r.processRequest(req)

			// If it worked, put the items in the unlinked queue for linking
			if err == nil {
				for _, item := range items {
					r.queueUnlinkedItem(UnlinkedItem{
						Item:      item,
						LinkDepth: req.GetLinkDepth(),
						PublishSubjects: []string{
							req.ItemSubject, // The response subject
						},
					})
				}
			} else {
				errorsMutex.Lock()
				errors = append(errors, err)
				errorsMutex.Unlock()
			}
		}(request)
	}

	// Link items
	go func() {
		for unlinkedItem := range r.uli {
			go func(i UnlinkedItem) {
				defer r.uliWG.Done()

				// Register the item with the handler to ensure that we aren't being called
				// in a recursive manner. If this fails it means the item has already been
				// found and we should stop doing what we're doing in order to avoid
				// duplicates
				//
				// Note that we are only storing pointers to the items so we can still edit
				// them in other goroutines as long as we are locking appropriately to avoid
				// race conditions
				if err := r.RegisterItem(i.Item); err != nil {
					// If the item is already registered that's okay. We just don't want
					// to continue
					return
				}

				if i.LinkDepth > 0 {
					// Resolve links
					r.linkItem(i)
				}

				// Send the fully linked item back onto the network
				if r.ResponseConnection != nil {
					// Respond with the Item
					for _, subject := range i.PublishSubjects {
						err := r.ResponseConnection.Publish(subject, i.Item)

						if err != nil {
							// TODO: I probably shouldn't be logging directly here but I
							// don't want the error to be lost
							log.WithFields(log.Fields{
								"error": err,
							}).Error("Response publishing error")
						}
					}
				}
			}(unlinkedItem)
		}
	}()

	// Wait for all of the initial requests to be done processing
	requestsWait.Wait()

	// If everything has failed then just stop here
	if len(errors) == len(r.Requests) {
		return nil, errors[0]
	}

	// Wait for all of the unlinked items to be processed
	r.uliWG.Wait()
	close(r.uli)

	return r.RegisteredItems(), nil
}

// processRequest should take a request, run it and return the un-linked items
// and optionally an error
func (r *RequestHandlerV2) processRequest(request *sdp.ItemRequest) ([]*sdp.Item, error) {
	var requestItem *sdp.Item
	var requestItems []*sdp.Item
	var requestErr error

	// Validate the request
	if !IsWildcard(request.GetContext()) && request.GetContext() != r.Assistant.Context {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: fmt.Sprintf("Got request with context: '%v', expected '%v'", request.GetContext(), r.Assistant.Context),
			Context:     r.Assistant.Context,
		}
	}

	requestItems = make([]*sdp.Item, 0)

	// Make the request of the assistant
	switch request.GetMethod() {
	case sdp.RequestMethod_GET:
		requestItem, requestErr = r.Assistant.Get(request.GetType(), request.GetQuery())
		requestItems = append(requestItems, requestItem)
	case sdp.RequestMethod_FIND:
		requestItems, requestErr = r.Assistant.Find(request.GetType())
	case sdp.RequestMethod_SEARCH:
		requestItems, requestErr = r.Assistant.Search(request.GetType(), request.GetQuery())
	}

	// If there was an error in the request then simply return
	if requestErr != nil {
		return nil, sdp.NewItemRequestError(requestErr)
	}

	for _, i := range requestItems {
		// If the main request had a linkDepth of great than zero it means we
		// need to keep linking, this means that we need to pass down all of the
		// subject info along with the number of remaining links. If the link
		// depth is zero then we just pass then back in their normal form as we
		// won't be executing them
		if request.GetLinkDepth() > 0 {
			for _, lir := range i.LinkedItemRequests {
				lir.LinkDepth = request.GetLinkDepth() - 1
				lir.ItemSubject = request.GetItemSubject()
				lir.ResponseSubject = request.GetResponseSubject()
			}
		}

		// Assign the item request
		if i.Metadata != nil {
			i.Metadata.SourceRequest = request
		}
	}

	return requestItems, requestErr
}

// linkItem should run all linkedItemRequests that it can and modify the passed
// item with the results. Removing any linked item requests that were able to
// execute
func (r *RequestHandlerV2) linkItem(parent UnlinkedItem) {
	var lirWG sync.WaitGroup
	var itemMutex sync.RWMutex

	lirWG.Add(len(parent.Item.LinkedItemRequests))

	// Loop over the linked item requests
	itemMutex.RLock()
	for _, lir := range parent.Item.LinkedItemRequests {
		go func(p *sdp.Item, req *sdp.ItemRequest) {
			defer lirWG.Done()

			linkedItems, err := r.processRequest(req)

			if err == nil {
				itemMutex.Lock()

				for _, li := range linkedItems {
					// Add the new items back into the queue to be linked
					// further if required
					r.queueUnlinkedItem(UnlinkedItem{
						Item:      li,
						LinkDepth: req.GetLinkDepth(),
						PublishSubjects: []string{
							req.ItemSubject, // The response subject
						},
					})

					// Create a reference to the newly found item and attach to
					// the parent item
					ref := li.Reference()
					p.LinkedItems = append(p.LinkedItems, &ref)
				}

				p.LinkedItemRequests = deleteItemRequest(p.LinkedItemRequests, req)

				itemMutex.Unlock()
			} else {
				if sdpErr, ok := err.(*sdp.ItemRequestError); ok {
					if sdpErr.GetErrorType() == sdp.ItemRequestError_NOCONTEXT {
						// If there was no context, leave it for the remote linker
						return
					}
				}

				itemMutex.Lock()
				p.LinkedItemRequests = deleteItemRequest(p.LinkedItemRequests, req)
				itemMutex.Unlock()
			}
		}(parent.Item, lir)
	}
	itemMutex.RUnlock()

	lirWG.Wait()
}

// ConnectToNats connects to a given NATS URL, it also support retries. Servers
// should be supplied as a slice of URLs e.g.
//
// engine.ConnectToNats([]string{"nats://127.0.0.1:1222",
// "nats://127.0.0.1:1223"}, 5, 5)
//
func ConnectToNats(urls []string, retries int, timeout int) *nats.EncodedConn {
	var tries int
	var servers string
	var hostname string
	var timeoutDuration time.Duration

	// Set default values
	if retries == 0 {
		retries = 10
	}

	if timeout == 0 {
		timeoutDuration = 10 * time.Second
	} else {
		timeoutDuration = time.Duration(timeout) * time.Second

	}

	// Register our custom encoder
	nats.RegisterEncoder("sdp", &sdp.ENCODER)

	// Get the hostname to use as the connection name
	hostname, _ = os.Hostname()

	servers = strings.Join(urls, ",")

	// Loop until we have a connection
	for tries <= retries {
		log.WithFields(log.Fields{
			"servers": servers,
		}).Info("Connecting to NATS")

		// TODO: Make these options more configurable
		// https://docs.nats.io/developing-with-nats/connecting/pingpong
		nc, err := nats.Connect(
			servers,                       // The servers to connect to
			nats.Name(hostname),           // The connection name
			nats.Timeout(timeoutDuration), // Connection timeout (per server)
		)

		if err == nil {
			var enc *nats.EncodedConn

			enc, err = nats.NewEncodedConn(nc, "sdp")

			if err != nil {
				panic("could not find sdp encoder")
			}

			return enc
		}

		// Increment tries
		tries++

		log.WithFields(log.Fields{
			"servers": servers,
			"err":     err,
		}).Info("Connection failed")

		// TODO: Add a configurable backoff algorithm here
		time.Sleep(5 * time.Second)
	}

	panic("Could not connect to NATS, giving up")
}

// NewItemRequestServer returns a server that processes item requests and responds to
// them. It needs to be passed the message that you intend it to process and
// the assistant that you want it to use when finding the state information
func (a *Assistant) NewItemRequestServer(responseConnection *nats.EncodedConn) nats.MsgHandler {
	return func(m *nats.Msg) {
		itemRequest := &sdp.ItemRequest{}
		requestHandler := RequestHandlerV2{
			Assistant:          a,
			ResponseConnection: responseConnection,
		}

		// Read the data
		proto.Unmarshal(m.Data, itemRequest)

		// Validate the context
		if !IsWildcard(itemRequest.GetContext()) && itemRequest.GetContext() != a.Context {
			// If the context doesn't match just discard the message
			return
		}

		// Validate the type is something that we actually have
		if !IsWildcard(itemRequest.GetType()) {
			// Check that we actually have a source for that type
			if _, hasType := a.Sources[itemRequest.GetType()]; !hasType {
				return
			}
		}

		// Respond saying we've got it
		responder := sdp.ResponseSender{
			ResponseSubject: itemRequest.ResponseSubject,
		}

		responder.Start(
			responseConnection,
			a.Context,
		)

		log.WithFields(log.Fields{
			"type":      itemRequest.Type,
			"method":    itemRequest.Method,
			"query":     itemRequest.Query,
			"linkDepth": itemRequest.LinkDepth,
			"context":   itemRequest.Context,
		}).Info("Received request")

		// Expand item request to all types if a type wasn't specified
		if itemRequest.Type != "*" {
			requestHandler.Requests = append(requestHandler.Requests, itemRequest)
		} else {
			for t := range a.Sources {
				// Create an item request for each type
				r := sdp.ItemRequest{
					Type:            t,
					Method:          itemRequest.GetMethod(),
					Query:           itemRequest.GetQuery(),
					Context:         itemRequest.GetContext(),
					ItemSubject:     itemRequest.GetItemSubject(),
					ResponseSubject: itemRequest.GetResponseSubject(),
					LinkDepth:       itemRequest.GetLinkDepth(),
				}

				requestHandler.Requests = append(requestHandler.Requests, &r)
			}
		}

		// Execute all requests in parallel
		_, err := requestHandler.Run()

		// If all failed then return an error
		if err != nil {
			if ire, ok := err.(*sdp.ItemRequestError); ok {
				responder.Error(ire)
			} else {
				ire = &sdp.ItemRequestError{
					ErrorType:   sdp.ItemRequestError_OTHER,
					ErrorString: err.Error(),
					Context:     a.Context,
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
