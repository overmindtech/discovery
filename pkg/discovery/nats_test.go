package discovery

import (
	"sync"
	"testing"
	"time"

	"github.com/dylanratcliffe/sdp-go"
	"github.com/dylanratcliffe/source-go/pkg/sources"
)

var A = Assistant{
	Sources: sources.SourceMap{
		"person": &sources.Source{
			Type: "person",
			GetFindMutex: &sources.GFM{
				Mutex: sync.RWMutex{},
			},
			Backends: sources.BackendList{
				&sources.BackendInfo{
					Backend:       TestBackend{},
					Priority:      1,
					CacheDuration: 10 * time.Second,
					Context:       "test",
					Cache: sources.Cache{
						Name:        "person",
						Storage:     make([]sources.CachedResult, 0),
						MinWaitTime: 100 * time.Millisecond,
					},
				},
			},
		},
	},
	Context: "test",
}

type RequestTest struct {
	Description        string
	Request            *sdp.ItemRequest
	ExpectError        bool
	NumExpectedResults int
}

var Tests = []RequestTest{
	{
		Description: "Valid GET request without context",
		Request: &sdp.ItemRequest{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
			Query:  "Bint Chud",
		},
		ExpectError: true,
	},
	{
		Description: "Valid GET request with linking",
		Request: &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Query:     "Bint Chud",
			Context:   "test",
			LinkDepth: 5,
		},
		ExpectError:        false,
		NumExpectedResults: 6,
	},
	{
		Description: "Valid GET request with context",
		Request: &sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_GET,
			Query:   "Bint Chud",
			Context: "test",
		},
		ExpectError:        false,
		NumExpectedResults: 1,
	},
	{
		Description: "Valid GET request with unknown context",
		Request: &sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_GET,
			Query:   "Bint Chud",
			Context: "something_else",
		},
		ExpectError: true,
	},
	{
		Description: "Valid GET request with unknown type",
		Request: &sdp.ItemRequest{
			Type:   "bird",
			Method: sdp.RequestMethod_GET,
			Query:  "Bint Chud",
		},
		ExpectError: true,
	},
	{
		Description: "GET request without type",
		Request: &sdp.ItemRequest{
			Method:  sdp.RequestMethod_GET,
			Query:   "Bint Chud",
			Context: "test",
		},
		ExpectError: true,
	},
	{
		Description: "GET request without query",
		Request: &sdp.ItemRequest{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
		},
		ExpectError: true,
	},
	{
		Description: "GET request without type",
		Request: &sdp.ItemRequest{
			Method: sdp.RequestMethod_GET,
			Query:  "foo",
		},
		ExpectError: true,
	},
	{
		Description: "FIND request with valid type",
		Request: &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_FIND,
			LinkDepth: 0,
			Context:   "test",
		},
		ExpectError:        false,
		NumExpectedResults: 10,
	},
	{
		Description: "FIND request with linking",
		Request: &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_FIND,
			LinkDepth: 10,
			Context:   "test",
		},
		ExpectError:        false,
		NumExpectedResults: 110,
	},
	{
		Description: "FIND request with redundant query",
		Request: &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_FIND,
			LinkDepth: 0,
			Query:     "this is ignored",
			Context:   "test",
		},
		ExpectError:        false,
		NumExpectedResults: 10,
	},
	{
		Description: "FIND request with unknown context",
		Request: &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_FIND,
			LinkDepth: 0,
			Context:   "unknown",
		},
		ExpectError: true,
	},
	{
		Description: "SEARCH request",
		Request: &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_SEARCH,
			LinkDepth: 3,
			Context:   "test",
		},
		ExpectError:        false,
		NumExpectedResults: 40,
	},
	{
		Description: "SEARCH request with unknown context",
		Request: &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_SEARCH,
			LinkDepth: 0,
			Context:   "unknown",
		},
		ExpectError: true,
	},
	{
		Description: "SEARCH request with linking",
		Request: &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_SEARCH,
			LinkDepth: 5,
			Context:   "test",
		},
		ExpectError:        false,
		NumExpectedResults: 60,
	},
}

func TestProcessRequest(t *testing.T) {
	for _, test := range Tests {
		t.Run(test.Description, func(t *testing.T) {
			handler := RequestHandlerV2{
				Assistant: &A,
				Requests: []*sdp.ItemRequest{
					test.Request,
				},
			}

			requestItems, err := handler.Run()

			if err != nil {
				// If there is an error that we weren't expecting then return it
				if test.ExpectError == false {
					t.Error(err)
				}
			} else {
				if test.ExpectError {
					t.Error("Expected an error did not get one")
				}

				if l := len(requestItems); l != test.NumExpectedResults {
					t.Errorf(
						"Expected %v results but got %v.",
						test.NumExpectedResults,
						l,
					)
				}

				var reqFound = false

				for _, i := range requestItems {
					if i.Metadata.SourceRequest == test.Request {
						reqFound = true
					}
				}

				if !reqFound {
					t.Error("Original request not found in Metadata.SourceRequest of any item")
				}

				if test.Request.LinkDepth == 0 {
					// If there was no link depth then we expect the linked item
					// requests to remain present as the assistant shouldn't
					// have tried to execute them
					if len(requestItems) > 0 {
						item := requestItems[0]
						if len(item.LinkedItemRequests) != 3 {
							t.Errorf(
								"Item %v had %v linked item requests, expected 3",
								item.GloballyUniqueName(),
								len(item.LinkedItemRequests),
							)
						}
					}
				}
			}
		})
	}
}

func TestLinkedItemRequests(t *testing.T) {
	request := &sdp.ItemRequest{
		Type:            "person",
		Method:          sdp.RequestMethod_GET,
		Query:           "Bint Chud",
		Context:         "test",
		LinkDepth:       5,
		ItemSubject:     "items",
		ResponseSubject: "responses",
	}

	handler := RequestHandlerV2{
		Assistant: &A,
		Requests: []*sdp.ItemRequest{
			request,
		},
	}

	results, err := handler.Run()

	if err != nil {
		t.Error(err)
	}

	if l := len(results); l != 6 {
		t.Fatalf("Expected 6 results but got %v", l)
	}

	found := false

	for _, item := range results {
		if item.UniqueAttributeValue() == "Bint Chud" {
			// We want to see the following:
			//
			// 1. The linked item request that failed should be gone
			//
			// 3. The linked item request that worked should be gone
			//
			// 2. The linked item request that we didn't have a context for
			// should remain
			if l := len(item.LinkedItemRequests); l != 1 {
				t.Errorf("Bint Chud linked item requests length should be 1, got %v", l)
			}

			if item.LinkedItemRequests[0].ItemSubject != "items" {
				t.Error("Linked item request from inaccessible context doesn't have ItemSubject")
			}

			if item.LinkedItemRequests[0].ResponseSubject != "responses" {
				t.Error("Linked item request from inaccessible context doesn't have ResponseSubject")
			}

			found = true
		}
	}

	// Check the final item to ensure that it has the expected
	// linkedItemRequests, these should all have a linkDepth of 0
	var finalItem *sdp.Item

	for _, item := range results {
		if len(item.LinkedItems) == 0 {
			finalItem = item
		}
	}

	if finalItem == nil {
		t.Fatal("Could not find final item (item with no linked items)")
	}

	if len(finalItem.LinkedItemRequests) != 3 {
		t.Errorf("Final item has %v linkedItemRequests, expected 3", len(finalItem.LinkedItemRequests))
	}

	for _, r := range finalItem.LinkedItemRequests {
		if r.LinkDepth != 0 {
			t.Errorf("Link depth on request attached to final item was not zero. Request: %v", r)
		}
	}

	if !found {
		t.Error("Could not find Bint Chud among found items")
	}
}
