package sdpcache

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/dylanratcliffe/sdp-go"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CacheTest struct {
	Name       string
	Inserts    []*sdp.Item
	Duration   time.Duration
	Tags       Tags
	ExpectFind bool
}

var goodAttributes, _ = sdp.ToAttributes(map[string]interface{}{
	"type":         "couch",
	"colour":       "black",
	"serialNumber": "982734928347",
})

var goodItem = &sdp.Item{
	Context:    "home",
	Attributes: goodAttributes,
	LinkedItemRequests: []*sdp.ItemRequest{
		{
			Type:   "furniture",
			Method: sdp.RequestMethod_GET,
			Query:  "1234",
		},
	},
	Type:            "furniture",
	UniqueAttribute: "serialNumber",
	Metadata: &sdp.Metadata{
		SourceName:            "test",
		SourceDuration:        durationpb.New(time.Millisecond),
		SourceDurationPerItem: durationpb.New(time.Millisecond),
		SourceRequest: &sdp.ItemRequest{
			Type:            "furniture",
			Method:          sdp.RequestMethod_GET,
			Query:           "1234",
			LinkDepth:       1,
			Context:         "testContext",
			ItemSubject:     "items",
			ResponseSubject: "responses",
		},
		Timestamp: timestamppb.Now(),
	},
	LinkedItems: []*sdp.Reference{
		{
			Type:                 "furniture",
			UniqueAttributeValue: "linked",
			Context:              "home",
		},
	},
}

var goodTags = Tags{
	"foo":  "bar",
	"when": "now",
}

var cacheDuration = (100 * time.Millisecond)

var cacheTests = []CacheTest{
	{"Cached items are immediately available", []*sdp.Item{goodItem}, (0 * time.Millisecond), goodTags, true},
	{"Cached items are available after 0.5x duration", []*sdp.Item{goodItem}, (cacheDuration / 2), goodTags, true},
	{"Cached items are gone after 1.5x duration", []*sdp.Item{goodItem}, (cacheDuration + (cacheDuration / 2)), goodTags, false},
	{"Cached items are gone after 2x duration", []*sdp.Item{goodItem}, (cacheDuration * 2), goodTags, false},
}

func TestItemCaching(t *testing.T) {
	for _, test := range cacheTests {
		c := Cache{
			Name:        test.Name,
			MinWaitTime: 1 * time.Millisecond,
		}

		t.Run(test.Name, func(t *testing.T) {
			// Sort the items first
			sort.Slice(
				test.Inserts,
				func(i, j int) bool {
					num, _ := test.Inserts[i].Compare(test.Inserts[j])
					return num < 0
				},
			)

			// Insert items
			for _, i := range test.Inserts {
				c.StoreItem(i, cacheDuration, test.Tags)
			}

			c.StartPurger()

			// Wait for the prescribed amount of time
			time.Sleep(test.Duration)

			// Check that they still exist
			results, err := c.Search(test.Tags)

			// Sort results
			sort.Slice(
				results,
				func(i, j int) bool {
					num, _ := test.Inserts[i].Compare(test.Inserts[j])
					return num < 0
				},
			)

			if test.ExpectFind {
				if len(test.Inserts) != len(results) {
					t.Errorf("Cache result expected %v but got %v", test.Inserts, results)
				}

				for i := range test.Inserts {
					if !reflect.DeepEqual(results[i], test.Inserts[i]) {
						t.Errorf("Cache result expected %v but got %v", test.Inserts, results)
					}
				}
			} else {
				switch err.(type) {
				case CacheNotFoundError:
					// This is fine
				default:
					t.Errorf("Cache expected not to find anything, but got an unexpected error. Expected [], CacheNotFoundError, got: %v, %v", results, err)
				}
			}
		})
	}
}

func TestErrorCache(t *testing.T) {
	c := Cache{
		Name:        "errors",
		MinWaitTime: 1 * time.Millisecond,
	}

	// Cache an item
	c.StoreItem(goodItem, cacheDuration, goodTags)

	// Cache an error with the same tags
	c.StoreError(&sdp.ItemRequestError{}, cacheDuration, goodTags)

	// Check to see what we get
	results, err := c.Search(goodTags)

	if err == nil {
		t.Errorf("Found empty error but expected state.ItemNotFoundError. Results: %v Error: %v", results, err)
	}
}

func TestDeleteCache(t *testing.T) {
	var results []*sdp.Item
	var err error

	c := Cache{
		Name:        "errors",
		MinWaitTime: 1 * time.Millisecond,
	}

	// Cache an item
	c.StoreItem(goodItem, cacheDuration, goodTags)

	// Check to see what we get
	results, err = c.Search(goodTags)

	if len(results) != 1 {
		t.Errorf("Found %v results, expected 1. Storing an item must have failed", len(results))
	}

	if err != nil {
		t.Error("Error should be nil when items were found")
	}

	// Delete the item
	c.Delete(goodTags)

	// Check to see what we get
	results, err = c.Search(goodTags)

	if len(results) != 0 {
		t.Errorf("Found %v results, expected 0. Deleting an item must have failed", len(results))
	}

	if err == nil {
		t.Error("Error should not be nil when no items were found")
	}
}

// Test auto tagging
func TestAutoTagging(t *testing.T) {
	t.Run("Items should contain expected tags", func(t *testing.T) {
		c := Cache{
			Name:        "errors",
			MinWaitTime: 1 * time.Millisecond,
		}

		// Items
		// Insert an item with manual tags
		c.StoreItem(goodItem, (1 * time.Hour), goodTags)

		// Note the expected automatic tags for an item
		autoTags := Tags{
			"type":                 goodItem.Type,
			"context":              goodItem.Context,
			"uniqueAttributeValue": fmt.Sprint(goodItem.UniqueAttributeValue()),
		}

		// Search using each of the expected tags
		t.Run("Item should be searchable using all supplied tags", func(t *testing.T) {
			for k, v := range goodTags {
				results, err := c.Search(Tags{k: v})

				expectNumItem(results, err, t, 1, Tags{k: v})
			}
		})

		// Search using all of the expected automatic tags
		t.Run("Item should be searchable using all default tags", func(t *testing.T) {
			for k, v := range autoTags {
				results, err := c.Search(Tags{k: v})

				expectNumItem(results, err, t, 1, Tags{k: v})
			}
		})

		// Search using the combination of automatic and manual tags
		t.Run("Item should be searchable using all tags at once", func(t *testing.T) {
			searchTags := Tags{}

			for k, v := range goodTags {
				searchTags[k] = v
			}

			for k, v := range autoTags {
				searchTags[k] = v
			}

			results, err := c.Search(searchTags)

			expectNumItem(results, err, t, 1, searchTags)
		})
	})

	// Errors
	t.Run("Errors should contain expected tags", func(t *testing.T) {
		c := Cache{
			Name:        "errors",
			MinWaitTime: 1 * time.Millisecond,
		}

		expectedError := errors.New("💩")

		// Insert an error with manual tags
		c.StoreError(expectedError, (1 * time.Hour), goodTags)

		// Search using each of the expected tags
		autoTags := Tags{
			"isError": "true",
		}

		// Search using all of the expected automatic tags
		t.Run("Error should be searchable using all default tags", func(t *testing.T) {
			for k, v := range autoTags {
				_, err := c.Search(Tags{k: v})

				if err != expectedError {
					t.Errorf("Expected %v but got %v", expectedError, err)
				}
			}
		})

		t.Run("Error should be searchable using all supplied tags", func(t *testing.T) {
			for k, v := range goodTags {
				_, err := c.Search(Tags{k: v})

				if err != expectedError {
					t.Errorf("Expected %v but got %v", expectedError, err)
				}
			}
		})

		// Search using the combination of automatic and manual tags
		t.Run("Error should be searchable using all tags at once", func(t *testing.T) {
			searchTags := Tags{}

			for k, v := range goodTags {
				searchTags[k] = v
			}

			for k, v := range autoTags {
				searchTags[k] = v
			}

			_, err := c.Search(searchTags)

			if err != expectedError {
				t.Errorf("Expected %v but got %v", expectedError, err)
			}
		})
	})
}

// Negative testing
// Tags that conflict with default tags
func TestConflictingTagging(t *testing.T) {
	c := Cache{
		Name:        "errors",
		MinWaitTime: 1 * time.Millisecond,
	}

	conflictTags := Tags{
		"type": "SOMETHING_BAD",
	}

	c.StoreItem(goodItem, cacheDuration, conflictTags)

	// Expect that the default tags take precedence since we rely on them
	items, err := c.Search(conflictTags)

	if err == nil || len(items) != 0 {
		t.Error("Tags that conflict with defaults should be discarded")
	}

	items, err = c.Search(Tags{
		"type": "furniture",
	})

	if err != nil || len(items) == 0 {
		t.Error("Default tags not found")
	}
}

// Inserting many errors
func TestManyErrors(t *testing.T) {
	c := Cache{
		Name:        "errors",
		MinWaitTime: 1 * time.Millisecond,
	}

	c.StoreError(errors.New("one"), cacheDuration, goodTags)
	c.StoreError(errors.New("two"), cacheDuration, goodTags)
	c.StoreError(errors.New("three"), cacheDuration, goodTags)

	_, err := c.Search(goodTags)

	if err == nil {
		t.Error("Returned no errors after storing three, that's bad")
	}

	if err.Error() != "one" {
		t.Error("When many errors are stored, the first one should be returned")
	}
}

// Bad deletions
func TestBadDeletions(t *testing.T) {
	c := Cache{
		Name:        "errors",
		MinWaitTime: 1 * time.Millisecond,
	}

	c.StoreItem(goodItem, cacheDuration, goodTags)

	c.Delete(Tags{
		"bad": "tag",
	})

	items, _ := c.Search(goodTags)

	i, _ := items[0].Compare(goodItem)

	if i != 0 {
		t.Error("Could not find item after unrelated deletion")
	}
}

// We need to be sure that if an item was found in many different ways that this
// is handled correctly. An example could be if an item was found using a FIND,
// then again using a SEARCH. If the new item was to simply overwrite the old,
// it would mean that if a user re-requested the find they would get s different
// number of results since one of the results would have had its tags removed
// when it was updated.
//
// The desired behavior is that if an item is found twice, but with a different
// set of tags, it should ismply be cached twice
func TestUpdating(t *testing.T) {
	var items []*sdp.Item
	var err error
	var i int

	c := Cache{
		Name:        "errors",
		MinWaitTime: 1 * time.Millisecond,
	}

	findTags := Tags{
		"method": "find",
	}
	searchTags := Tags{
		"method": "search",
		"query":  "something:here=",
	}

	// Create the first item
	c.StoreItem(goodItem, cacheDuration, findTags)

	// Create the second item
	c.StoreItem(goodItem, cacheDuration, searchTags)

	items, err = c.Search(findTags)

	if err != nil {
		t.Errorf("Expected to find item with no error, got %v", err)
	} else {

		i, _ = items[0].Compare(goodItem)

		if i != 0 {
			t.Error("Could not find item cached with findtags")
		}

		items, err = c.Search(searchTags)

		if err != nil {
			t.Errorf("Expected to find item with no error, got %v", err)
		} else {

			i, _ = items[0].Compare(goodItem)

			if i != 0 {
				t.Error("Could not find item cached with searchTags")
			}
		}
	}
}

// We want to ensure that when you cache an item, the cache isn't storing a
// pointer to the item that we supplied. This is because we might want to update
// or change the item later and we don't want this to affect the item within the
// cache. Upon intertion and retrieval items should be duplicated in memory to
// ensure that the cache is a *copy* if the items and not just a registry of
// references to the origfinal items that might be bing messed with as part of
// normal operation
func TestItemPointerDuplication(t *testing.T) {
	var expectedLinkedItems int
	var expectedLinkedItemRequests int

	c := Cache{
		Name:        "TestItemPointerDuplication",
		MinWaitTime: 1 * time.Millisecond,
	}

	tags := Tags{
		"testname": "TestItemPointerDuplication",
	}

	t.Run("Insert an item", func(t *testing.T) {
		c.StoreItem(goodItem, cacheDuration, tags)
	})

	t.Run("Modify the insterted item", func(t *testing.T) {
		goodItem.Type = "modified"
		goodItem.UniqueAttribute = "modified"

		// Attributes
		goodItem.Attributes.Set("modified", "modified")

		// Metadata
		goodItem.Metadata.SourceName = "modified"
		goodItem.Metadata.SourceRequest.Context = "modified"
		goodItem.Metadata.SourceRequest.Method = sdp.RequestMethod_SEARCH
		goodItem.Metadata.Timestamp = timestamppb.New(time.Unix(0, 0))
		goodItem.Metadata.SourceDuration = durationpb.New(10 * time.Second)
		goodItem.Metadata.SourceDurationPerItem = durationpb.New(10 * time.Second)
		goodItem.Context = "modified"

		// Links
		expectedLinkedItemRequests = len(goodItem.LinkedItemRequests)
		expectedLinkedItems = len(goodItem.LinkedItems)

		// Append to make sure the slice is being copied by value
		goodItem.LinkedItemRequests = append(goodItem.LinkedItemRequests, &sdp.ItemRequest{
			Type:      "modified",
			Method:    sdp.RequestMethod_FIND,
			Query:     "modified",
			LinkDepth: 2,
			Context:   "modified",
		})
		// Modify an existing item to ensure that they are being cloned deeply
		goodItem.LinkedItemRequests[0].Context = "modified"

		goodItem.LinkedItems = append(goodItem.LinkedItems, &sdp.Reference{
			Type:                 "modified",
			UniqueAttributeValue: "modified",
			Context:              "modifed",
		})
		goodItem.LinkedItems[0].Context = "modified"
	})

	t.Run("Check that the cached item has not been modified", func(t *testing.T) {
		// Get the item back from the cache
		cachedItems, err := c.Search(tags)

		if err != nil {
			t.Error(err)
		}

		if l := len(cachedItems); l != 1 {
			t.Errorf("Found %v items from cache, expected 1", l)
		}

		cachedItem := cachedItems[0]

		t.Run("Check Type", func(t *testing.T) {
			if cachedItem.GetType() == "modified" {
				t.Error("Type was modified")
			}
		})
		t.Run("Check UniqueAttribute", func(t *testing.T) {
			if cachedItem.GetUniqueAttribute() == "modified" {
				t.Error("UniqueAttribute was modified")
			}
		})
		t.Run("Check Attributes", func(t *testing.T) {
			_, err := cachedItem.GetAttributes().Get("modified")

			if err == nil {
				t.Error("Attributes was modified")
			}
		})
		t.Run("Check Metadata", func(t *testing.T) {
			if cachedItem.GetMetadata().GetSourceName() == "modified" {
				t.Error("Item Metadata SourceName was modififed")
			}
			if cachedItem.GetMetadata().GetSourceRequest().GetContext() != "testContext" {
				t.Error("Item Metadata SourceRequest Context was modififed")
			}
			if cachedItem.GetMetadata().GetSourceRequest().GetMethod() != sdp.RequestMethod_GET {
				t.Error("Item Metadata SourceRequest Method was modififed")
			}
			if cachedItem.GetMetadata().GetTimestamp().AsTime().Before(time.Unix(0, 1)) {
				t.Error("Item Metadata Timestamp was modififed")
			}
			if cachedItem.GetMetadata().GetSourceDuration().AsDuration() > time.Millisecond {
				t.Error("Item Metadata SourceDuration was modififed")
			}
			if cachedItem.GetMetadata().GetSourceDurationPerItem().AsDuration() > time.Millisecond {
				t.Error("Item Metadata SourceDurationPerItem was modififed")
			}

		})
		t.Run("Check Context", func(t *testing.T) {
			if cachedItem.GetContext() == "modified" {
				t.Error("Context was modified")
			}
		})
		t.Run("Check LinkedItemRequests", func(t *testing.T) {
			if len(cachedItem.GetLinkedItemRequests()) > expectedLinkedItemRequests {
				t.Error("LinkedItemRequests was modified by adding a value to the slice")
			}

			if cachedItem.LinkedItemRequests[0].Context == "modified" {
				t.Error("LinkedItemRequests was modified by changing an existing value")
			}
		})
		t.Run("Check LinkedItems", func(t *testing.T) {
			if len(cachedItem.GetLinkedItems()) > expectedLinkedItems {
				t.Error("LinkedItems was modified by adding a value to the slice")
			}

			if cachedItem.LinkedItems[0].Context == "modified" {
				t.Error("LinkedItems was modified by changing an existing value")
			}
		})
	})
}

func expectNumItem(results []*sdp.Item, err error, t *testing.T, numExpected int, searchTags Tags) {
	if err != nil {
		t.Errorf("Got error when searching for an item using the supplied tags %v", searchTags)
	}

	if len(results) != numExpected {
		t.Errorf("Expected %v result, got %v: %v", numExpected, len(results), results)
	}
}
