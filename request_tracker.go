package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/overmindtech/sdp-go"
	log "github.com/sirupsen/logrus"
)

// RequestTracker is used for tracking the progress of a single requestt. This
// is used because a single request could have a link depth that results in many
// requests being executed meaning that we need to not only track the first
// request, but also all other requests and items that result from linking
type RequestTracker struct {
	// The request to track
	Request *sdp.ItemRequest

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

	// cancelFunc A cuntion that will cancel all requests when called
	cancelFunc      context.CancelFunc
	cancelFuncMutex sync.Mutex
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
	if i != nil {
		r.unlinkedItemsWG.Add(1)
		r.unlinkedItems <- i
	}
}

// startLinking Starts linking items that have been added to the queue using
// `queueUnlinkedItem()`. Once an item is fully linked it will be published ot
// NATS
func (r *RequestTracker) startLinking(ctx context.Context) {
	// Link items
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case unlinkedItem := <-r.unlinkedItems:
				if unlinkedItem != nil {
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

						var hasSourceRequest bool
						var itemSubject string

						if metadata := i.GetMetadata(); metadata != nil {
							if sourceRequest := metadata.GetSourceRequest(); sourceRequest != nil {
								hasSourceRequest = true
								itemSubject = sourceRequest.ItemSubject

								if sourceRequest.GetLinkDepth() > 0 {
									// Resolve links
									r.linkItem(ctx, i)
								}
							}
						}

						// Send the fully linked item back onto the network
						if e := r.Engine; e != nil && hasSourceRequest {
							if e.IsNATSConnected() {
								// Respond with the Item
								err := e.natsConnection.Publish(itemSubject, i)

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
			}
		}
	}()
}

// linkItem should run all linkedItemRequests that it can and modify the passed
// item with the results. Removing any linked item requests that were able to
// execute
func (r *RequestTracker) linkItem(ctx context.Context, parent *sdp.Item) {
	if ctx.Err() != nil {
		return
	}

	var lirWG sync.WaitGroup
	var itemMutex sync.RWMutex

	// Loop over the linked item requests
	itemMutex.RLock()

	lirWG.Add(len(parent.LinkedItemRequests))

	for _, lir := range parent.LinkedItemRequests {
		go func(p *sdp.Item, req *sdp.ItemRequest) {
			defer lirWG.Done()

			if r.Engine == nil {
				return
			}

			items := make(chan *sdp.Item)
			errs := make(chan *sdp.ItemRequestError)
			requestErr := make(chan error)
			var shouldRemove bool

			go func(e chan error) {
				e <- r.Engine.ExecuteRequest(ctx, req, items, errs)
			}(requestErr)

			for {
				select {
				case li, ok := <-items:
					if ok {
						itemMutex.Lock()

						// Add the new items back into the queue to be linked
						// further if required
						r.queueUnlinkedItem(li)

						// Create a reference to the newly found item and attach
						// to the parent item
						p.LinkedItems = append(p.LinkedItems, li.Reference())

						itemMutex.Unlock()
					} else {
						items = nil
					}
				case err, ok := <-errs:
					if ok {
						if err.ErrorType == sdp.ItemRequestError_NOTFOUND {
							// If we looked and didn't find the item then
							// there is no point keeping the request around.
							// Mark as true so that it's removed from the
							// item
							shouldRemove = true
						}
					} else {
						errs = nil
					}
				}

				if items == nil && errs == nil {
					break
				}
			}

			err := <-requestErr

			if err == nil || shouldRemove {
				// Delete the item request if we were able to resolve it locally
				// OR it failed to resolve but because we looked and we know it
				// doesn't exist
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

// Execute Executes a given item request and publishes results and errors on the
// relevant nats subjects. Returns the full list of items, errors, and a final
// error. The final error will be populated if all sources failed, or some other
// error was encountered while trying run the request
func (r *RequestTracker) Execute() ([]*sdp.Item, []*sdp.ItemRequestError, error) {
	if r.unlinkedItems == nil {
		r.unlinkedItems = make(chan *sdp.Item)
	}

	if r.Request == nil {
		return nil, nil, nil
	}

	if r.Engine == nil {
		return nil, nil, errors.New("no engine supplied, cannot execute")
	}

	items := make(chan *sdp.Item)
	errs := make(chan *sdp.ItemRequestError)
	errChan := make(chan error)
	sdpErrs := make([]*sdp.ItemRequestError, 0)

	// Create context to enforce timeouts
	ctx, cancel := r.Request.TimeoutContext()
	r.cancelFuncMutex.Lock()
	r.cancelFunc = cancel
	r.cancelFuncMutex.Unlock()
	defer cancel()

	r.startLinking(ctx)

	// Run the request
	go func() {
		errChan <- r.Engine.ExecuteRequest(ctx, r.Request, items, errs)
	}()

	// Process the items and errors as they come in
	for {
		select {
		case item, ok := <-items:
			if ok {
				// Add to the queue. This will be picked up by other goroutine,
				// linked and published to NATS once done
				r.queueUnlinkedItem(item)
			} else {
				items = nil
			}
		case err, ok := <-errs:
			if ok {
				sdpErrs = append(sdpErrs, err)

				if r.Request.ErrorSubject != "" && r.Engine.natsConnection != nil {
					pubErr := r.Engine.natsConnection.Publish(r.Request.ErrorSubject, err)

					if pubErr != nil {
						// TODO: I probably shouldn't be logging directly here but I
						// don't want the error to be lost
						log.WithFields(log.Fields{
							"error": err,
						}).Error("Error publishing item request error")
					}
				}
			} else {
				errs = nil
			}
		}

		if items == nil && errs == nil {
			// If both channels have been closed and set to nil, we're done so
			// break
			break
		}
	}

	// Wait for all of the initial requests to be done processing
	r.stopLinking()

	// Get the result of the execution
	err := <-errChan

	if err != nil {
		return r.LinkedItems(), sdpErrs, err
	}

	return r.LinkedItems(), sdpErrs, ctx.Err()
}

// Cancel Cancells the currently running request
func (r *RequestTracker) Cancel() {
	r.cancelFuncMutex.Lock()
	defer r.cancelFuncMutex.Unlock()

	if r.cancelFunc != nil {
		r.cancelFunc()
	}
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
