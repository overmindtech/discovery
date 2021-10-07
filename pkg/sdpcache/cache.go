package sdpcache

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/dylanratcliffe/sdp-go"
	log "github.com/sirupsen/logrus"
)

// MinWaitDefault The default minimum wait time
const MinWaitDefault = (5 * time.Second)

// Cache is responsible for caching data between calls
//
type Cache struct {
	// Name used for logging since there are many caches
	Name    string
	Storage []CachedResult

	// The minimum amount of time to wait between purges, defaults to
	// MinWaitDefault
	MinWaitTime time.Duration

	// Mutexes that are used to ensure thread safety of the underlying storage
	storageMutex sync.RWMutex
}

// Tags A map of key-value pairs that can be searched on
type Tags map[string]string

// CachedResult An itemincluding cache metadata
type CachedResult struct {
	// Item is the actual cached item
	Item *sdp.Item

	// Error is the error that we want
	Error error

	// CacheDuration How long to cache the item for
	CacheDuration time.Duration

	// InterstTime When the item was inserted into the cache
	InsertTime time.Time

	// Tags are arbitrary strings maps that can be searched on
	Tags Tags
}

// Expired Tells us if the item's cache has expired
func (cr *CachedResult) Expired() bool {
	return cr.InsertTime.Add(cr.CacheDuration).Before(time.Now())
}

// HasTag returns a boolean if the CachedResult has a given tag with a given
// value
func (cr *CachedResult) HasTag(key string, value string) bool {
	// Range over the tags that the result has
	for tk, tv := range cr.Tags {
		// Return true as soon as we find a match
		if tk == key && tv == value {
			return true
		}
	}

	// If no match was found return false
	return false
}

// HasTags returns whether a given result has all of the tags supplied. This is
// a logical AND query
func (cr *CachedResult) HasTags(tags Tags) bool {
	// Range over all the required sets of tags
	for tk, tv := range tags {
		// Check if the result has the required tag, immediately return false if
		// not, otherwise continue on and check all of the other tags
		if !cr.HasTag(tk, tv) {
			return false
		}
	}

	return true
}

// CacheNotFoundError is returned when an item is not found in the cache.
// Usually this would be handled very gracefully
type CacheNotFoundError Tags

// StoreItem Store an item in the cache.
//
// Note that all stored items will have the following default tags applied:
//   type: (item.Type)
//   context: (item.Context)
//   uniqueAttributeValue: (item.Attributes[item.UniqueAttribute])
//
// These default tags mean that each full tag set will be unique to an item
//
func (c *Cache) StoreItem(item *sdp.Item, duration time.Duration, tags Tags) {
	var allTags Tags
	var err error
	var itemCopy sdp.Item

	allTags = addDefaultItemTags(item, tags)

	// Ensure that the item hasn't already been stored in the cache
	_, err = c.Search(allTags)

	if err == nil {
		// If the item is already in the cache then delete it since we have a
		// new version
		c.Delete(allTags)
	}

	// Lock to ensure thread safety
	c.storageMutex.Lock()
	defer c.storageMutex.Unlock()

	// Clone the item for storage
	item.Copy(&itemCopy)

	si := CachedResult{
		Item:          &itemCopy,
		Error:         nil,
		CacheDuration: duration,
		InsertTime:    time.Now(),
		Tags:          addDefaultItemTags(item, tags),
	}

	c.Storage = append(c.Storage, si)
}

// BulkStoreItem stores many items at once instead of just one
func (c *Cache) BulkStoreItem(items []*sdp.Item, duration time.Duration, tags Tags) {
	for _, item := range items {
		c.StoreItem(item, duration, tags)
	}
}

// StoreError Store an item in the cache.
//
// Note that all errors will get the following tags by default:
//   isError: true
//
func (c *Cache) StoreError(err error, duration time.Duration, tags Tags) {
	// Ensure that the error hasn't already been stored in the cache. Since
	// error uniqueness isn't determined in any good way we will use the tags
	// passed in here
	c.Delete(addDefaultErrorTags(err, tags))

	// Lock to ensure thread safety
	c.storageMutex.Lock()
	defer c.storageMutex.Unlock()

	si := CachedResult{
		Item:          &sdp.Item{},
		Error:         err,
		CacheDuration: duration,
		InsertTime:    time.Now(),
		Tags:          addDefaultErrorTags(err, tags),
	}

	c.Storage = append(c.Storage, si)
}

// Search will find all items with a given set of tags. If multiple tags
// are supplied an AND is used. If a cached error is found with matching tags it
// will be returned immediately
func (c *Cache) Search(searchTags Tags) ([]*sdp.Item, error) {
	c.storageMutex.RLock()
	defer c.storageMutex.RUnlock()

	results := make([]*sdp.Item, 0)

	for _, r := range c.Storage {
		if r.HasTags(searchTags) {
			var returnItem sdp.Item

			// If there is an error then we want that returned immediately
			if r.Error != nil {
				return results, r.Error
			}

			// Duplicate the item by value
			r.Item.Copy(&returnItem)

			// Return a pointer to the new duplicate. This should mean that
			// things like attaching linked items to something that was returned
			// from the cache shouldn't affect the cached items as they are no
			// longer pointers to the same objects in memory
			results = append(results, &returnItem)
		}
	}

	if len(results) == 0 {
		return make([]*sdp.Item, 0), CacheNotFoundError(searchTags)
	}

	return results, nil
}

// Delete Removes items form the cache that have matching tags
func (c *Cache) Delete(searchTags Tags) error {
	newStorage := make([]CachedResult, 0)

	c.storageMutex.Lock()
	defer c.storageMutex.Unlock()

	for _, r := range c.Storage {
		if !r.HasTags(searchTags) {
			newStorage = append(newStorage, r)
		}
	}

	// Write the new data
	c.Storage = newStorage

	return CacheNotFoundError(searchTags)
}

// GetMinWaitTime Returns the minimum wait time or the default if not set
func (c *Cache) GetMinWaitTime() time.Duration {
	if c.MinWaitTime == 0 {
		return MinWaitDefault
	}

	return c.MinWaitTime
}

// DefaultItemTags Returns default tags for a given item
func DefaultItemTags(item *sdp.Item) Tags {
	return Tags{
		"type":                 item.Type,
		"context":              item.Context,
		"uniqueAttributeValue": item.UniqueAttributeValue(),
	}
}

// DefaultErrorTags Returns default tags for any error
func DefaultErrorTags(e error) Tags {
	return Tags{
		"isError":    "true",
		"errorValue": e.Error(),
	}
}

// addDefaultItemTags Appends default tags
func addDefaultItemTags(item *sdp.Item, tags Tags) Tags {
	return mergeTags(tags, DefaultItemTags(item))
}

// addDefaultErrorTags Appends default tags
func addDefaultErrorTags(e error, tags Tags) Tags {
	return mergeTags(tags, DefaultErrorTags(e))
}

func mergeTags(aTags Tags, bTags Tags) Tags {
	endTags := make(Tags)

	for k, v := range aTags {
		endTags[k] = v
	}

	for k, v := range bTags {
		endTags[k] = v
	}

	return endTags
}

func (c CacheNotFoundError) Error() string {
	var tagPairs []string
	var tagString string

	for k, v := range c {
		tagPairs = append(tagPairs, (k + " = " + v))
	}

	tagString = strings.Join(tagPairs, ", ")

	return fmt.Sprintf("cache: Cannot find value with tags %v", tagString)
}

// PurgeStats Stats about the Purge
type PurgeStats struct {
	// How many items were timed out of the cache
	NumPurged int
	// How long the Purgeing took overall
	TimeTaken time.Duration
	// Shortest cache duration of the remaining items
	ShortestCacheRemaining time.Duration
}

// Purge Prunes old items from the cache
func (c *Cache) Purge() PurgeStats {
	start := time.Now()
	var newStorage []CachedResult
	var shortest time.Duration
	var numPurged int
	var timeTaken time.Duration

	// Create the slice
	newStorage = make([]CachedResult, 0)

	c.storageMutex.RLock()

	if len(c.Storage) > 0 {
		// Set the shortest to a very high value
		shortest = math.MaxInt64
	} else {
		// If there is nothing in the storage then just return zero
		shortest = 0
	}

	// Loop over the storage
	for _, ci := range c.Storage {
		// If the item is not expired then add it to the new storage
		if !ci.Expired() {
			newStorage = append(newStorage, ci)

			// Note the duration and store the shortest
			if ci.CacheDuration < shortest {
				shortest = ci.CacheDuration
			}
		} else {
			// If it is expired increment the counter
			numPurged++
		}
	}
	c.storageMutex.RUnlock()

	// Replace the old storage with the new storage
	c.storageMutex.Lock()
	c.Storage = newStorage
	c.storageMutex.Unlock()

	// Calculate the time taken
	timeTaken = time.Since(start)

	// Check of the shortest is unchanged, this can happen if all items were
	// purged in a single purge run. In this case we need to set the shortest
	// back to zero to avoid it being set to ~292 years
	if shortest == math.MaxInt64 {
		shortest = 0
	}

	return PurgeStats{
		NumPurged:              numPurged,
		TimeTaken:              timeTaken,
		ShortestCacheRemaining: shortest,
	}
}

// StartPurger Starts the automatic purging of the cache
func (c *Cache) StartPurger() {
	go func() {
		// We want this process to be efficient with how often it runs. To this end
		// we will make sure that it is able to dynamically increase and decrease
		// its frequency depending on the duration of the caches. The logic we will
		// use is that the purging frequency will be the shortest cache duration
		// divided by ten. Meaning that if there is an item in the cache with a
		// cache duration of 10 seconds, the Purger will run every second. If
		// however this item is removed and the rest of the items are to be cached
		// for 10 minutes, the Purger will revert to running every minute
		for {
			var sleepTime time.Duration

			stats := c.Purge()

			// If the shortest cache time is zero this means that there is
			// nothing left in the cache. In this cache we want to still sleep
			// otherwise we would end up looping like crazy
			if stats.ShortestCacheRemaining != 0 {
				sleepTime = stats.ShortestCacheRemaining / 10

				// Check that we aren't below the minimum wait time
				if sleepTime < c.GetMinWaitTime() {
					sleepTime = c.GetMinWaitTime()
				}
			} else {
				sleepTime = c.GetMinWaitTime()
			}

			if stats.NumPurged > 0 {
				log.WithFields(log.Fields{
					"numPurged":              stats.NumPurged,
					"timeTaken":              stats.TimeTaken,
					"shortestCacheRemaining": stats.ShortestCacheRemaining,
				}).Tracef("Finished %v cache purge, next purge in %v", c.Name, sleepTime)
			}

			time.Sleep(sleepTime)
		}
	}()
}
