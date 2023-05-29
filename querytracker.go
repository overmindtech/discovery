package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/overmindtech/sdp-go"
	log "github.com/sirupsen/logrus"
)

// QueryTracker is used for tracking the progress of a single query. This
// is used because a single query could have a link depth that results in many
// additional queries being executed meaning that we need to not only track the first
// query, but also all other queries and items that result from linking
type QueryTracker struct {
	// The query to track
	Query *sdp.Query

	// The engine that this is connected to, used for sending NATS messages
	Engine *Engine

	// Channel of items that have been found but have not yet had their links
	// resolved
	unlinkedItems chan *sdp.Item
	// Waitgroup so we can ensure that nothing is processing before closing the
	// channel
	unlinkedItemsWG sync.WaitGroup

	// Items that have begin link processing. Note that items in this map may
	// still be being modified if the linker is still running. Call
	// `stopLinking()` before accessing this map to avoid race conditions
	//
	// The keys in this map are the GloballyUniqueName to speed up searching
	linkedItems      map[string]*sdp.Item
	linkedItemsMutex sync.RWMutex

	// cancelFunc A function that will cancel all queries when called
	cancelFunc      context.CancelFunc
	cancelFuncMutex sync.Mutex
}

func (qt *QueryTracker) LinkedItems() []*sdp.Item {
	qt.linkedItemsMutex.RLock()
	defer qt.linkedItemsMutex.RUnlock()

	items := make([]*sdp.Item, len(qt.linkedItems))
	i := 0

	for _, item := range qt.linkedItems {
		items[i] = item
		i++
	}

	return items
}

// registerLinkedItem Registers an item in the database of linked items,
// returns an error if the item is already present
func (qt *QueryTracker) registerLinkedItem(i *sdp.Item) error {
	qt.linkedItemsMutex.Lock()
	defer qt.linkedItemsMutex.Unlock()

	if qt.linkedItems == nil {
		qt.linkedItems = make(map[string]*sdp.Item)
	}

	if _, exists := qt.linkedItems[i.GloballyUniqueName()]; exists {
		return fmt.Errorf("item %v has already been registered", i.GloballyUniqueName())
	}

	qt.linkedItems[i.GloballyUniqueName()] = i

	return nil
}

// queueUnlinkedItem Adds an item to the queue for linking. The engine will then
// execute the linked item queries and publish the completed item to the
// relevant NATS subject. Linking can be started and stopped using
// `startLinking()` and `stopLinking()`
func (qt *QueryTracker) queueUnlinkedItem(i *sdp.Item) {
	if i != nil {
		qt.unlinkedItemsWG.Add(1)
		qt.unlinkedItems <- i
	}
}

// startLinking Starts linking items that have been added to the queue using
// `queueUnlinkedItem()`. Once an item is fully linked it will be published to
// NATS
func (qt *QueryTracker) startLinking(ctx context.Context) {
	// Link items
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case unlinkedItem := <-qt.unlinkedItems:
				if unlinkedItem != nil {
					go func(i *sdp.Item) {
						defer qt.unlinkedItemsWG.Done()

						// Register the item with the handler to ensure that we aren't being called
						// in a recursive manner. If this fails it means the item has already been
						// found and we should stop doing what we're doing in order to avoid
						// duplicates
						//
						// Note that we are only storing pointers to the items so we can still edit
						// them in other goroutines as long as we are locking appropriately to avoid
						// race conditions
						if err := qt.registerLinkedItem(i); err != nil {
							// If the item is already registered that's okay. We just don't want
							// to continue
							return
						}

						var hasSourceQuery bool
						var natsSubject string

						if metadata := i.GetMetadata(); metadata != nil {
							if sourceQuery := metadata.GetSourceQuery(); sourceQuery != nil {
								hasSourceQuery = true
								natsSubject = sourceQuery.Subject()

								if sourceQuery.RecursionBehaviour.GetLinkDepth() > 0 {
									// Resolve links
									qt.linkItem(ctx, i)
								}
							}
						}

						// Send the fully linked item back onto the network
						if e := qt.Engine; e != nil && hasSourceQuery {
							if e.IsNATSConnected() {
								// Respond with the Item
								err := e.natsConnection.Publish(ctx, natsSubject, &sdp.QueryResponse{ResponseType: &sdp.QueryResponse_NewItem{NewItem: i}})

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

// linkItem should run all linkedItemQueries that it can and modify the passed
// item with the results. Removing any linked item queries that were able to
// execute
func (qt *QueryTracker) linkItem(ctx context.Context, parent *sdp.Item) {
	if ctx.Err() != nil {
		return
	}

	var lirWG sync.WaitGroup
	var itemMutex sync.RWMutex

	// Loop over the linked item queries
	itemMutex.RLock()

	lirWG.Add(len(parent.LinkedItemQueries))

	for _, lir := range parent.LinkedItemQueries {
		go func(p *sdp.Item, req *sdp.LinkedItemQuery) {
			defer lirWG.Done()

			if qt.Engine == nil {
				return
			}

			items := make(chan *sdp.Item)
			errs := make(chan *sdp.QueryError)
			queryErr := make(chan error)
			var shouldRemove bool

			go func(e chan error) {
				defer LogRecoverToReturn(&ctx, "linkItem -> ExecuteQuery")
				e <- qt.Engine.ExecuteQuery(ctx, req.Query, items, errs)
			}(queryErr)

			for {
				select {
				case li, ok := <-items:
					if ok {
						itemMutex.Lock()

						// Add the new items back into the queue to be linked
						// further if required
						qt.queueUnlinkedItem(li)

						// Create a reference to the newly found item and attach
						// to the parent item
						p.LinkedItems = append(p.LinkedItems, &sdp.LinkedItem{Item: li.Reference()})

						itemMutex.Unlock()
					} else {
						items = nil
					}
				case err, ok := <-errs:
					if ok {
						if err.ErrorType == sdp.QueryError_NOTFOUND {
							// If we looked and didn't find the item then
							// there is no point keeping the query around.
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

			err := <-queryErr

			if err == nil || shouldRemove {
				// Delete the item query if we were able to resolve it locally
				// OR it failed to resolve but because we looked and we know it
				// doesn't exist
				itemMutex.Lock()
				p.LinkedItemQueries = deleteQuery(p.LinkedItemQueries, req)
				itemMutex.Unlock()
			}
		}(parent, lir)
	}
	itemMutex.RUnlock()

	lirWG.Wait()
}

// stopLinking Stop linking items in the queue
func (qt *QueryTracker) stopLinking() {
	qt.unlinkedItemsWG.Wait()
	close(qt.unlinkedItems)
}

// Execute Executes a given item query and publishes results and errors on the
// relevant nats subjects. Returns the full list of items, errors, and a final
// error. The final error will be populated if all sources failed, or some other
// error was encountered while trying run the query
func (qt *QueryTracker) Execute(ctx context.Context) ([]*sdp.Item, []*sdp.QueryError, error) {
	if qt.unlinkedItems == nil {
		qt.unlinkedItems = make(chan *sdp.Item)
	}

	if qt.Query == nil {
		return nil, nil, nil
	}

	if qt.Engine == nil {
		return nil, nil, errors.New("no engine supplied, cannot execute")
	}

	items := make(chan *sdp.Item)
	errs := make(chan *sdp.QueryError)
	errChan := make(chan error)
	sdpErrs := make([]*sdp.QueryError, 0)

	// Create context to enforce timeouts
	ctx, cancel := qt.Query.TimeoutContext(ctx)
	qt.cancelFuncMutex.Lock()
	qt.cancelFunc = cancel
	qt.cancelFuncMutex.Unlock()
	defer cancel()

	qt.startLinking(ctx)

	// Run the query
	go func(e chan error) {
		defer LogRecoverToReturn(&ctx, "Execute -> ExecuteQuery")
		e <- qt.Engine.ExecuteQuery(ctx, qt.Query, items, errs)
	}(errChan)

	// Process the items and errors as they come in
	for {
		select {
		case item, ok := <-items:
			if ok {
				// Add to the queue. This will be picked up by other goroutine,
				// linked and published to NATS once done
				qt.queueUnlinkedItem(item)
			} else {
				items = nil
			}
		case err, ok := <-errs:
			if ok {
				sdpErrs = append(sdpErrs, err)

				if qt.Query.Subject() != "" && qt.Engine.natsConnection.Underlying() != nil {
					pubErr := qt.Engine.natsConnection.Publish(ctx, qt.Query.Subject(), &sdp.QueryResponse{ResponseType: &sdp.QueryResponse_Error{Error: err}})

					if pubErr != nil {
						// TODO: I probably shouldn't be logging directly here but I
						// don't want the error to be lost
						log.WithFields(log.Fields{
							"error": err,
						}).Error("Error publishing item query error")
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

	// Wait for all of the initial queries to be done processing
	qt.stopLinking()

	// Get the result of the execution
	err := <-errChan

	if err != nil {
		return qt.LinkedItems(), sdpErrs, err
	}

	return qt.LinkedItems(), sdpErrs, ctx.Err()
}

// Cancel Cancels the currently running query
func (qt *QueryTracker) Cancel() {
	qt.cancelFuncMutex.Lock()
	defer qt.cancelFuncMutex.Unlock()

	if qt.cancelFunc != nil {
		qt.cancelFunc()
	}
}

// deleteQuery Deletes an item query from a slice
func deleteQuery(queries []*sdp.LinkedItemQuery, remove *sdp.LinkedItemQuery) []*sdp.LinkedItemQuery {
	finalQueries := make([]*sdp.LinkedItemQuery, 0)
	for _, q := range queries {
		if q != remove {
			finalQueries = append(finalQueries, q)
		}
	}
	return finalQueries
}
