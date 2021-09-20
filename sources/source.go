package sources

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/dylanratcliffe/sdp-go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SourceMap is just a map of type names to their sources
type SourceMap map[string]*Source

// ItemSource is something that can discover items
type ItemSource interface {
	// Sources should also be able to find resources that match a certain
	// criteria. This method will not return an error if nothing is found, and
	// will always return an (array?) or results
	// Find()
	// This method will always return one resource and will fail if there is
	// more than one or it doesn't exist
	Get(name string) (*sdp.Item, error)
	Find() ([]*sdp.Item, error)
}

// SearchableItemSource Is a source of items that supports searching
type SearchableItemSource interface {
	ItemSource
	Search(query string) ([]*sdp.Item, error)
}

// Source is a struct that represents the higher lever abstraction
// that sits on top of a source, providing caching etc.
type Source struct {
	// This is an ordered slice of backends that are supported on this
	// platform. This shouldn't be accessed directly and people should add
	// backends using the RegisterBackend() method which will do compatability
	// checking, and will also ensure that the list is ordered. The ordering
	// only matters for a Get() where the source will relay the Get() to the
	// backends in order, returning the first one
	Backends BackendList
	// The type of items that this source will be responsible for finding
	Type string

	// This channel contains boolean permissions that can be used to control the
	// timing of backend execution. If this chan is not nil then each backend
	// that executes will read a value from the channel before commencing an
	// operation, and place the value `true` back onto the the channel once it's
	// done. This is designed to be used to limit the number of backends running
	// in parallel by providing all sources with a common buffered channel to
	// communicate on
	BackendExecutionPermissions chan bool

	// GetFindMutex is used for locking the source when doing a find. Basically
	// if something has a find lock then all get requests will be blocked until
	// that find has completed so as not to waste resources. This only makes any
	// difference when run in parallel
	GetFindMutex GetFindMutex

	// Callbacks
	//
	// newItemCallbacks: Called once each time a new item is found, if it was
	// already in the cache, it will not be triggered
	newItemCallbacks ItemCallbacks

	// parallelFind is used to determine if backends should be searched in
	// parallel when a the Find method is called
	parallelFind bool
}

// GetFindMutex A modified version of a RWMutex. Many get locks can be held but
// only one Find mutex. A waiting Find lock blocks all other get locks
type GetFindMutex interface {
	GetLock()
	GetUnlock()
	FindLock()
	FindUnlock()
}

// GFM A struct that satisfies the GetFindMutex interface
type GFM struct {
	Mutex sync.RWMutex
}

// GetLock Gets a lock that can be held by an unlimited number of goroutines,
// these locks are only blocked by FindLocks
func (g *GFM) GetLock() {
	g.Mutex.RLock()
}

// GetUnlock Unlocks the GetLock. This must be called once for each GetLock
// otherwise it will be impossible to ever obtain a FindLock
func (g *GFM) GetUnlock() {
	g.Mutex.RUnlock()
}

// FindLock An exclusive lock. Ensure that all GetLocks have been unlocked
// and stops any more from being obtained
func (g *GFM) FindLock() {
	g.Mutex.Lock()
}

// FindUnlock Unlocks a FindLock
func (g *GFM) FindUnlock() {
	g.Mutex.Unlock()
}

// ItemCallbacks A slice if callbacks that will be executed when new items are
// found
type ItemCallbacks []func(*sdp.Item)

// Call Calls all callbacks that have been registered passing a single item to
// them
func (calls ItemCallbacks) Call(item *sdp.Item) {
	var wg sync.WaitGroup

	// Call the callbacks
	for _, c := range calls {
		wg.Add(1)

		go func(cb func(*sdp.Item), i *sdp.Item) {
			defer wg.Done()

			cb(i)
		}(c, item)
	}

	wg.Wait()
}

// SourceSettings Contains settings for each source
type SourceSettings struct {
	// Wheather each backend should be executed in parallel or series when
	// fulfilling a Find() request
	ParallelFind bool
}

// SourceList is just a list of source interfaces. This is used mostly for
// scanning
type SourceList []*Source

// Get method for the source
//
// This method basically just passes on to the underlying Source, but it also
// does the caching element, checking if the element is in the cache, returning
// it if it is, and caching any newly found resources
//
func (s *Source) Get(name string) (*sdp.Item, error) {
	// If we have got this far we are going to have to actually git the backends
	// so first make sure we can obtain a get lock. This needs to be done before
	// the cache is consulted so that when it comes time for us to execute, we
	// check the cache at the latest possible moment. If instead we were tio
	// check the cache first, we might have made our decision to run based on an
	// outdated cache
	s.GetFindMutex.GetLock()

	for _, bi := range s.Backends {
		// Check the cache for a cached value
		searchTags := Tags{
			"uniqueAttributeValue": name,
			"type":                 s.Type,
		}
		cached, err := bi.Cache.Search(searchTags)

		// Check to see if we got an error from the cache and handle it
		switch e := err.(type) {
		case CacheNotFoundError:
			// If the item/error wasn't found in the cache then just continue on
		case *sdp.ItemRequestError:
			if e.ErrorType == sdp.ItemRequestError_NOTFOUND {
				// If the item wasn't found, but we've already looked then don't look
				// again and just return a blank result
				log.WithFields(log.Fields{
					"sourceType": s.Type,
					"name":       name,
				}).Debug("Was not found previously, skipping")

				s.GetFindMutex.GetUnlock()

				return &sdp.Item{}, e
			}
		case nil:
			if len(cached) == 1 {
				// If the cache found something then just return that
				log.WithFields(log.Fields{
					"sourceType":      s.Type,
					"backendPriority": bi.Priority,
					"backendPackage":  bi.Backend.BackendPackage(),
					"context":         bi.Context,
					"name":            name,
				}).Debug("Found item from cache")

				s.GetFindMutex.GetUnlock()

				return cached[0], nil
			}

			// If we got a weird number of stuff from the cache then
			// something is wrong
			log.WithFields(log.Fields{
				"type":                 s.Type,
				"uniqueAttributeValue": name,
				"backendPackage":       bi.Backend.BackendPackage(),
			}).Error("Cache returned >1 value, purging and continuing")

			bi.Cache.Delete(searchTags)
		}

		if !bi.Backend.Threadsafe() {
			// If the backend is not threadsafe then make sure we get a lock
			// before doing anything
			bi.mux.Lock()
			defer bi.mux.Unlock()
		}

		var getDuration time.Duration
		var result *sdp.Item
		var permission bool

		if s.BackendExecutionPermissions != nil {
			// Get a backend execution permission if we are using this. This is
			// used to limit the number of calls that are run in parallel
			permission = <-s.BackendExecutionPermissions
		}

		log.WithFields(log.Fields{
			"sourceType":      s.Type,
			"backendPriority": bi.Priority,
			"backendPackage":  bi.Backend.BackendPackage(),
			"context":         bi.Context,
			"name":            name,
		}).Debug("Executing get for backend")

		getDuration = timeOperation(func() {
			// Run Get() on each backend and return the first result
			result, err = bi.Backend.Get(name)
		})

		// A good backend should be careful to raise ItemNotFoundError if the
		// query was able to execute successfully, but the item wasn't found. If
		// however there was some failure in checking and therefore we aren't
		// sure if the item is actually there is not another type of error
		// should be raised and this will be logged
		if e, sdpErr := err.(*sdp.ItemRequestError); (sdpErr && e.ErrorType == sdp.ItemRequestError_NOTFOUND) || err == nil {
			log.WithFields(log.Fields{
				"backendPackage":  bi.Backend.BackendPackage(),
				"backendPriority": bi.Priority,
				"context":         bi.Context,
				"itemFound":       (err == nil),
				"name":            name,
				"sourceType":      s.Type,
			}).Debug("Get complete")
		} else {
			log.WithFields(log.Fields{
				"backendPackage":  bi.Backend.BackendPackage(),
				"backendPriority": bi.Priority,
				"context":         bi.Context,
				"name":            name,
				"sourceType":      s.Type,
				"error":           err.Error(),
			}).Error("Get failed")
		}

		if permission {
			// Give the permission back to the pool
			s.BackendExecutionPermissions <- permission
		}

		if err == nil {
			// Handle the case where we are given a nil pointer
			if result == nil {
				return &sdp.Item{}, &sdp.ItemRequestError{
					ErrorType:   sdp.ItemRequestError_OTHER,
					ErrorString: "Backend returned a nil pointer as an item",
				}
			}

			// Assign the context
			AssignDefaultContext(result, bi.Context)

			// Set the metadata
			result.Metadata = &sdp.Metadata{
				BackendPackage:         bi.Backend.BackendPackage(),
				BackendName:            (bi.Backend.BackendPackage() + "." + bi.Backend.Type()),
				Timestamp:              timestamppb.New(time.Now()),
				BackendDuration:        durationpb.New(getDuration),
				BackendDurationPerItem: durationpb.New(getDuration),
			}

			// Store the new item in the cache
			itemTags := Tags{
				"method": "get",
			}
			bi.Cache.StoreItem(result, bi.CacheDuration, itemTags)

			s.GetFindMutex.GetUnlock()

			// Call all the callbacks
			s.newItemCallbacks.Call(result)

			return result, nil
		}

		s.GetFindMutex.GetUnlock()

		// Mark this name as not found so that we don't look again
		err = &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOTFOUND,
			ErrorString: err.Error(),
		}
		bi.Cache.StoreError(err, bi.CacheDuration, searchTags)
	}

	// If we don't find anything then we should raise an error
	return &sdp.Item{}, &sdp.ItemRequestError{
		ErrorType:   sdp.ItemRequestError_NOTFOUND,
		ErrorString: fmt.Sprintf("No item found in %v backends", len(s.Backends)),
	}
}

// Find all instances of the item
//
// This will execute the `Find()` method on the backend and cache it
func (s *Source) Find() []*sdp.Item {
	var finds []*sdp.Item
	var backendMutex sync.Mutex // In case we're not running in parallel

	// This will store all found items from all backends
	findChannel := make(chan []*sdp.Item)
	finds = make([]*sdp.Item, 0)

	// Create a goroutine of each backend and kick off a find
	for _, bi := range s.Backends {
		go func(b *BackendInfo, c chan []*sdp.Item, t string) {
			// First thing to do is check the cache
			searchTags := Tags{
				"method": "find",
			}

			items, err := b.Cache.Search(searchTags)

			switch e := err.(type) {
			case CacheNotFoundError:
				// If the item/error wasn't found in the cache then just
				// continue on
			case *sdp.ItemRequestError:
				if e.ErrorType == sdp.ItemRequestError_NOTFOUND {
					log.WithFields(log.Fields{
						"backendPackage":  b.Backend.BackendPackage(),
						"backendPriority": b.Priority,
						"context":         b.Context,
						"sourceType":      s.Type,
						"method":          sdp.RequestMethod_FIND.String(),
					}).Debug("Found cached empty result, not executing")

					// This means that we have explicitly cached a blank find. In
					// this situation we want to do nothing
					c <- make([]*sdp.Item, 0)
					return
				}
			default:
				// If we get a result from the cache then return that
				if len(items) > 0 {
					log.WithFields(log.Fields{
						"backendPackage":  b.Backend.BackendPackage(),
						"backendPriority": b.Priority,
						"context":         b.Context,
						"sourceType":      s.Type,
						"method":          sdp.RequestMethod_FIND.String(),
						"items":           len(items),
					}).Debug("Found items from cache")

					c <- items
					return
				}
			}

			// If nothing was found in the cache then continue on
			if !s.parallelFind {
				// If we are not executing in parallel then ensure that we are
				// locking. There isn't much point having different logic for
				// parallel vs non parallel, we will just lock the goroutines up
				// instead
				backendMutex.Lock()
				defer backendMutex.Unlock()
			}

			var findDuration time.Duration
			var finds []*sdp.Item
			var permission bool

			// TODO: I have just realised that this will mean that only one find
			// for each source will be allowed to run at a time. Maybe we should
			// consider changing this
			//
			// Lock the mutex so that we don't execute a find in parallel. This
			// will also block all incoming Get requests until this find has
			// completed
			s.GetFindMutex.FindLock()

			if s.BackendExecutionPermissions != nil {
				// Get a backend execution permission if we are using this
				permission = <-s.BackendExecutionPermissions
			}

			log.WithFields(log.Fields{
				"backendPackage":  b.Backend.BackendPackage(),
				"backendPriority": b.Priority,
				"context":         b.Context,
				"sourceType":      s.Type,
				"method":          sdp.RequestMethod_FIND.String(),
			}).Debug("Executing find for backend")

			// Reset the error
			err = nil

			findDuration = timeOperation(func() {
				// Find items
				finds, err = b.Backend.Find()
			})

			if err == nil {
				log.WithFields(log.Fields{
					"backendPackage":  b.Backend.BackendPackage(),
					"backendPriority": b.Priority,
					"context":         b.Context,
					"items":           len(finds),
					"sourceType":      s.Type,
					"method":          sdp.RequestMethod_FIND.String(),
				}).Debug("Find complete")

				// Check too see if nothing was found, make sure we cache the
				// nothing
				if len(finds) == 0 {
					b.Cache.StoreError(&sdp.ItemRequestError{
						ErrorType: sdp.ItemRequestError_NOTFOUND,
					}, b.CacheDuration, searchTags)
				}
			} else {
				log.WithFields(log.Fields{
					"backendPackage":  b.Backend.BackendPackage(),
					"backendPriority": b.Priority,
					"context":         b.Context,
					"items":           len(finds),
					"sourceType":      s.Type,
					"error":           err.Error(),
					"method":          sdp.RequestMethod_FIND.String(),
				}).Error("Error during find")

				b.Cache.StoreError(err, b.CacheDuration, searchTags)
			}

			if permission {
				// Give the permission back to the pool
				s.BackendExecutionPermissions <- permission
			}

			// For each found item, add more details
			//
			// Use the index here to ensure that we're actually editing the
			// right thing
			for i := range finds {
				// Get a pointer to the item we're dealing with
				item := finds[i]

				// Handle the case where we are given a nil pointer
				if item == nil {
					continue
				}

				// Save context
				AssignDefaultContext(item, b.Context)

				// Store metadata
				item.Metadata = &sdp.Metadata{
					Timestamp:              timestamppb.New(time.Now()),
					BackendPackage:         b.Backend.BackendPackage(),
					BackendName:            (b.Backend.BackendPackage() + "." + b.Backend.Type()),
					BackendDuration:        durationpb.New(findDuration),
					BackendDurationPerItem: durationpb.New(time.Duration(findDuration.Nanoseconds() / int64(len(finds)))),
				}

				// Cache the item
				b.Cache.StoreItem(item, b.CacheDuration, searchTags)
			}

			// Unlock the  mutex to allow other operations to resume while we
			// continue with the housekeeping
			s.GetFindMutex.FindUnlock()

			// For each found item, call the callbacks
			for _, item := range finds {
				// Call the callbacks
				s.newItemCallbacks.Call(item)
			}

			// Send to the channel
			c <- finds
		}(bi, findChannel, s.Type)
	}

	// Receive all of the results and save to the slice
	for range s.Backends {
		// The syntax here is strange but we are appending each item to finds
		finds = append(finds, <-findChannel...)
	}

	return finds
}

// Search Executes some kind of custom search query for a given backend. The
// query must be a string but other than that the required structure us up to
// the backend. Each backend will also cache search in an intelligent way,
// meaning that even though we don't know what the results of given search will
// be, if the same search is executed twice the second query will be returned
// from the cache. The same goes for searches that don't find anything, the
// cache will remember that nothing was found and return the appropriate error
func (s *Source) Search(query string) []*sdp.Item {
	// TODO: Allow restriction of parallel execution in the same way that
	// parallelFind works

	// This will store all found items from all backends
	searchChannel := make(chan []*sdp.Item)
	searchResults := make([]*sdp.Item, 0)

	// Create a goroutine of each backend and kick off a search
	for _, bi := range s.Backends {
		go func(b *BackendInfo, c chan []*sdp.Item, q string) {
			var items []*sdp.Item
			var err error
			var permission bool
			var searchTags Tags

			// Check hat this backend can actually be searched
			searcher, ok := b.Backend.(SearchableItemSource)
			if !ok {
				log.WithFields(log.Fields{
					"backendPackage":  b.Backend.BackendPackage(),
					"backendPriority": b.Priority,
					"context":         b.Context,
					"sourceType":      s.Type,
					"method":          sdp.RequestMethod_SEARCH.String(),
				}).Debug("Backend does not implement Search(), skipping")

				// If the backend doesn't support search then just return
				// nothing
				c <- make([]*sdp.Item, 0)
				return
			}

			// Check the cache
			searchTags = Tags{
				"method": "search",
				"query":  q,
			}

			items, err = b.Cache.Search(searchTags)

			switch e := err.(type) {
			case CacheNotFoundError:
				// If the item/error wasn't found in the cache then just
				// continue on
			case *sdp.ItemRequestError:
				if e.ErrorType == sdp.ItemRequestError_NOTFOUND {
					log.WithFields(log.Fields{
						"backendPackage":  b.Backend.BackendPackage(),
						"backendPriority": b.Priority,
						"context":         b.Context,
						"sourceType":      s.Type,
						"method":          sdp.RequestMethod_SEARCH.String(),
					}).Debug("Found cached empty result, not executing")

					// This means that we have explicitly cached a blank find. In
					// this situation we want to do nothing
					c <- make([]*sdp.Item, 0)
					return
				}
			default:
				// If we get a result from the cache then return that
				if len(items) > 0 {
					log.WithFields(log.Fields{
						"backendPackage":  b.Backend.BackendPackage(),
						"backendPriority": b.Priority,
						"context":         b.Context,
						"sourceType":      s.Type,
						"method":          sdp.RequestMethod_SEARCH.String(),
						"items":           len(items),
						"query":           query,
					}).Debug("Found items from cache")

					c <- items
					return
				}
			}

			// If nothing was found in the cache then actually execute the search
			if s.BackendExecutionPermissions != nil {
				// Get a backend execution permission if we are using this
				permission = <-s.BackendExecutionPermissions
			}

			log.WithFields(log.Fields{
				"backendPackage":  b.Backend.BackendPackage(),
				"backendPriority": b.Priority,
				"context":         b.Context,
				"sourceType":      s.Type,
				"query":           q,
				"method":          sdp.RequestMethod_SEARCH.String(),
			}).Debug("Executing search for backend")

			// Reset the error
			err = nil

			searchDuration := timeOperation(func() {
				items, err = searcher.Search(q)
			})

			if err == nil {
				log.WithFields(log.Fields{
					"backendPackage":  b.Backend.BackendPackage(),
					"backendPriority": b.Priority,
					"context":         b.Context,
					"items":           len(items),
					"query":           q,
					"sourceType":      s.Type,
					"method":          sdp.RequestMethod_SEARCH.String(),
				}).Debug("Search completed")

				// Check too see if nothing was found, make sure we cache the
				// nothing
				if len(items) == 0 {
					b.Cache.StoreError(&sdp.ItemRequestError{
						ErrorType: sdp.ItemRequestError_NOTFOUND,
					}, b.CacheDuration, searchTags)
				}
			} else {
				log.WithFields(log.Fields{
					"backendPackage":  b.Backend.BackendPackage(),
					"backendPriority": b.Priority,
					"context":         b.Context,
					"items":           len(items),
					"query":           q,
					"sourceType":      s.Type,
					"error":           err.Error(),
					"method":          sdp.RequestMethod_SEARCH.String(),
				}).Error("Error during search")

				b.Cache.StoreError(err, b.CacheDuration, searchTags)
			}

			if permission {
				// Give the permission back to the pool
				s.BackendExecutionPermissions <- permission
			}

			// If nothing was found then store the error in the cache
			if len(items) == 0 {
				err = &sdp.ItemRequestError{
					ErrorType:   sdp.ItemRequestError_NOTFOUND,
					ErrorString: fmt.Sprintf("No %v was returned from search", b.Backend.Type()),
				}
				b.Cache.StoreError(err, b.CacheDuration, searchTags)
			}

			// For each found item, add more details
			//
			// Use the index here to ensure that we're actually editing the
			// right thing
			for i := range items {
				// Get a pointer to the item we're dealing with
				item := items[i]

				// Handle the case where we are given a nil pointer
				if item == nil {
					continue
				}

				// Save context
				AssignDefaultContext(item, b.Context)

				// Store metadata
				item.Metadata = &sdp.Metadata{
					Timestamp:              timestamppb.New(time.Now()),
					BackendPackage:         b.Backend.BackendPackage(),
					BackendName:            (b.Backend.BackendPackage() + "." + b.Backend.Type()),
					BackendDuration:        durationpb.New(searchDuration),
					BackendDurationPerItem: durationpb.New(time.Duration(searchDuration.Nanoseconds() / int64(len(items)))),
				}

				// Cache the item
				b.Cache.StoreItem(item, b.CacheDuration, searchTags)
			}

			// For each found item, call the callbacks
			for _, item := range items {
				// Call the callbacks
				s.newItemCallbacks.Call(item)
			}

			// Send to the channel
			c <- items

		}(bi, searchChannel, query)
	}

	// Receive all of the results and save to the slice
	for range s.Backends {
		// The syntax here is strange but we are appending each item to finds
		searchResults = append(searchResults, <-searchChannel...)
	}

	return searchResults
}

// RegisterBackend adds a backend to a source if it is compatible. If not it
// will simply not do anything. Backends should be initialized before being
// registered
func (s *Source) RegisterBackend(bi *BackendInfo) {
	// Only accept Backends that are the same type as the source, also check
	// this first as it will be basically instant and the Supported() method
	// call might take a while
	if bi.Backend.Type() != s.Type {
		log.WithFields(log.Fields{
			"backendType":    bi.Backend.Type(),
			"sourceType":     s.Type,
			"backendPackage": bi.Backend.BackendPackage(),
		}).Debug("Backend type doesn't match source type, skipping")

		return
	}

	// Check if the backend is supported
	if sb, ok := bi.Backend.(ConditionallySupportedBackend); ok {
		// If the backend is not supported then just skip it
		if !sb.Supported() {
			log.WithFields(log.Fields{
				"type":           bi.Backend.Type(),
				"backendPackage": bi.Backend.BackendPackage(),
				"priority":       bi.Priority,
				"cacheDuration":  bi.CacheDuration,
			}).Debug("Backend not supported, skipping")

			return
		}
	}

	log.WithFields(log.Fields{
		"type":           bi.Backend.Type(),
		"backendPackage": bi.Backend.BackendPackage(),
		"priority":       bi.Priority,
		"cacheDuration":  bi.CacheDuration,
		"context":        bi.Context,
	}).Debug("Adding new backend to source")

	s.Backends = append(s.Backends, bi)

	// Trigger a re-sort
	s.SortBackends()
}

// SortBackends sorts backends by their priority
func (s *Source) SortBackends() {
	sort.Slice(s.Backends, func(i, j int) bool {
		return s.Backends[i].Priority < s.Backends[j].Priority
	})
}

// GetType returns the type of this source
func (s *Source) GetType() string {
	return s.Type
}

// Find runs the find method on all sources and returns the resulting items
func (il SourceList) Find(context string) []*sdp.Item {
	var items []*sdp.Item
	// For all sources that support it, find everything
	for _, src := range il {
		// PERF: Use goroutines to make this mutithreaded... maybe. Don't
		// want to overwhelm the host
		//
		// Note that the dots here are how you do slice merging
		items = append(items, src.Find()...)
	}
	return items
}

// RegisterItemCallback Register a callback function that will be called
// once for each new item that is found. It will not be called for duplicates
//
// Callbacks are executed asynchronously are are non-blocking
func (s *Source) RegisterItemCallback(callback func(*sdp.Item)) {
	s.newItemCallbacks = append(s.newItemCallbacks, callback)
}

// AssignDefaultContext sets the context for an item. Some backends may be able
// to determine the context for their items, but others may not so this will
// assign the context if they don't have one
func AssignDefaultContext(item *sdp.Item, context string) {
	if item.Context == "" {
		// Set the context of the item
		item.Context = context
	}

	// Check to see if all the linked items requests have a valid context
	for i := range item.LinkedItemRequests {
		// If the context is blank then we will default it to the local context
		if item.LinkedItemRequests[i].Context == "" {
			item.LinkedItemRequests[i].Context = context
		}
	}
}

// timeOperation Times how lon an operation takes and stores it in the first
// parameter. The second parameter is the function to execute
func timeOperation(f func()) time.Duration {
	start := time.Now()

	f()

	return time.Since(start)
}
