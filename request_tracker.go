package discovery

import (
	"fmt"
	"sync"

	"github.com/overmindtech/sdp-go"
	log "github.com/sirupsen/logrus"
)

// RequestTracker is used for tracking the progress of a single request (or a set
// of related requests). This is required because a single request could have a
// link depth that results in many requests being executed meaning that we need
// to not only track the first request, but also all other requests and items
// that result from linking
type RequestTracker struct {
	// The list of requests to track
	Requests []*sdp.ItemRequest

	// The engine that this is connected to, used for sending NATS messages
	Engine *Engine

	// Channel of items that have been found but have not yet had their links
	// resolved
	unlinkedItems chan *sdp.Item
	// Waitgroup so we can ensure that nothing is processing before closing the
	// channel
	unlinkedItemsWG sync.WaitGroup

	// Items that have begin link processing. Note that items in this map mey
	// still be being modified if the linker is still running. Call
	// `stopLinking()` before accessing this map to avoid race conditions
	//
	// The keys in this map are the GloballyUniqueName to speed up searching
	linkedItems      map[string]*sdp.Item
	linkedItemsMutex sync.RWMutex
}

func (r *RequestTracker) LinkedItems() []*sdp.Item {
	r.linkedItemsMutex.RLock()
	defer r.linkedItemsMutex.RUnlock()

	items := make([]*sdp.Item, len(r.linkedItems))
	i := 0

	for _, item := range r.linkedItems {
		items[i] = item
		i++
	}

	return items
}

// registerLinkedItem Registers aqn item in the database of linked items,
// returns an error if the item is already present
func (r *RequestTracker) registerLinkedItem(i *sdp.Item) error {
	r.linkedItemsMutex.Lock()
	defer r.linkedItemsMutex.Unlock()

	if r.linkedItems == nil {
		r.linkedItems = make(map[string]*sdp.Item)
	}

	if _, exists := r.linkedItems[i.GloballyUniqueName()]; exists {
		return fmt.Errorf("item %v has already been registered", i.GloballyUniqueName())
	}

	r.linkedItems[i.GloballyUniqueName()] = i

	return nil
}

// queueUnlinkedItem Adds an item to the queue for linking. The engine will then
// execute the linked item requests and publish the completed item to the
// relevant NATS subject. Linking can be started and stopped using
// `startLinking()` and `stopLinking()`
func (r *RequestTracker) queueUnlinkedItem(i *sdp.Item) {
	r.unlinkedItemsWG.Add(1)
	r.unlinkedItems <- i
}

// startLinking Starts linking items that have been added to the queue using
// `queueUnlinkedItem()`. Once an item is fully linked it will be published ot
// NATS
func (r *RequestTracker) startLinking() {
	// Link items
	go func() {
		for unlinkedItem := range r.unlinkedItems {
			go func(i *sdp.Item) {
				defer r.unlinkedItemsWG.Done()

				// Register the item with the handler to ensure that we aren't being called
				// in a recursive manner. If this fails it means the item has already been
				// found and we should stop doing what we're doing in order to avoid
				// duplicates
				//
				// Note that we are only storing pointers to the items so we can still edit
				// them in other goroutines as long as we are locking appropriately to avoid
				// race conditions
				if err := r.registerLinkedItem(i); err != nil {
					// If the item is already registered that's okay. We just don't want
					// to continue
					return
				}

				if i.GetMetadata().GetSourceRequest().GetLinkDepth() > 0 {
					// Resolve links
					r.linkItem(i)
				}

				// Send the fully linked item back onto the network
				if r.Engine.IsNATSConnected() {
					// Respond with the Item
					err := r.Engine.natsConnection.Publish(i.Metadata.SourceRequest.ItemSubject, i)

					if err != nil {
						// TODO: I probably shouldn't be logging directly here but I
						// don't want the error to be lost
						log.WithFields(log.Fields{
							"error": err,
						}).Error("Response publishing error")
					}
				}
			}(unlinkedItem)
		}
	}()
}

// linkItem should run all linkedItemRequests that it can and modify the passed
// item with the results. Removing any linked item requests that were able to
// execute
func (r *RequestTracker) linkItem(parent *sdp.Item) {
	var lirWG sync.WaitGroup
	var itemMutex sync.RWMutex

	// Loop over the linked item requests
	itemMutex.RLock()

	lirWG.Add(len(parent.LinkedItemRequests))

	for _, lir := range parent.LinkedItemRequests {
		go func(p *sdp.Item, req *sdp.ItemRequest) {
			defer lirWG.Done()

			linkedItems, err := r.Engine.ExecuteRequest(req)

			if err == nil {
				itemMutex.Lock()

				for _, li := range linkedItems {
					// Add the new items back into the queue to be linked
					// further if required
					r.queueUnlinkedItem(li)

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
		}(parent, lir)
	}
	itemMutex.RUnlock()

	lirWG.Wait()
}

// stopLinking Stop linking items in the queue
func (r *RequestTracker) stopLinking() {
	r.unlinkedItemsWG.Wait()
	close(r.unlinkedItems)
}

func (r *RequestTracker) Execute() ([]*sdp.Item, error) {
	if len(r.Requests) == 0 {
		return nil, nil
	}

	var errors []error
	var errorsMutex sync.Mutex
	var requestsWait sync.WaitGroup

	if r.unlinkedItems == nil {
		r.unlinkedItems = make(chan *sdp.Item)
	}

	// Populate the waitgroup with the initial number of requests
	requestsWait.Add(len(r.Requests))

	r.startLinking()

	// Process requests
	for _, request := range r.Requests {
		go func(req *sdp.ItemRequest) {
			defer requestsWait.Done()

			// Run the request
			items, err := r.Engine.ExecuteRequest(req)

			// If it worked, put the items in the unlinked queue for linking
			if err == nil {
				for _, item := range items {
					// Add to the queue. This will be picked up by other
					// goroutine, linked and published once done
					r.queueUnlinkedItem(item)
				}
			} else {
				errorsMutex.Lock()
				errors = append(errors, err)
				errorsMutex.Unlock()
			}
		}(request)
	}

	// Wait for all of the initial requests to be done processing
	requestsWait.Wait()
	r.stopLinking()

	// If everything has failed then just stop here
	if len(errors) == len(r.Requests) {
		return nil, errors[0]
	}

	return r.LinkedItems(), nil
}

// deleteItemRequest Deletes an item request from a slice
func deleteItemRequest(requests []*sdp.ItemRequest, remove *sdp.ItemRequest) []*sdp.ItemRequest {
	finalRequests := make([]*sdp.ItemRequest, 0)
	for _, request := range requests {
		if request != remove {
			finalRequests = append(finalRequests, request)
		}
	}
	return finalRequests
}
