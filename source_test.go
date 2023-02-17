package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/overmindtech/sdp-go"
)

func TestFilterSources(t *testing.T) {
	e := NewEngine()

	e.AddSources(
		&TestSource{
			ReturnScopes: []string{"test"},
			ReturnType:   "person",
		},
		&TestSource{
			ReturnScopes: []string{"test"},
			ReturnType:   "fish",
		},
		&TestSource{
			ReturnScopes: []string{sdp.WILDCARD},
			ReturnType:   "person",
		},
		&TestSource{
			ReturnScopes: []string{
				"testA",
				"testB",
			},
			ReturnType: "chair",
		},
		&TestSource{
			ReturnScopes: []string{"test"},
			ReturnType:   "hidden_person",
			IsHidden:     true,
		},
	)

	t.Run("Right type wrong scope", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "person",
			Scope: "wrong",
		}

		ee := ExpectExpand{
			NumRequests: 1,
			NumSources:  1,
		}

		ee.Validate(t, e.ExpandRequest(&req))
	})

	t.Run("Right scope wrong type", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "wrong",
			Scope: "test",
		}

		ee := ExpectExpand{
			NumRequests: 0,
			NumSources:  0,
		}

		ee.Validate(t, e.ExpandRequest(&req))
	})

	t.Run("Right both", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "person",
			Scope: "test",
		}

		ee := ExpectExpand{
			NumRequests: 1,
			NumSources:  2,
		}

		ee.Validate(t, e.ExpandRequest(&req))
	})

	t.Run("Multi-scope", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "chair",
			Scope: "testB",
		}

		ee := ExpectExpand{
			NumRequests: 1,
			NumSources:  1,
		}

		ee.Validate(t, e.ExpandRequest(&req))
	})

	t.Run("Wildcard scope", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "person",
			Scope: sdp.WILDCARD,
		}

		ee := ExpectExpand{
			NumRequests: 2,
			NumSources:  2,
		}

		ee.Validate(t, e.ExpandRequest(&req))

		req = sdp.ItemRequest{
			Type:  "chair",
			Scope: sdp.WILDCARD,
		}

		ee = ExpectExpand{
			NumRequests: 2,
			NumSources:  2,
		}

		ee.Validate(t, e.ExpandRequest(&req))
	})

	t.Run("Wildcard type", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  sdp.WILDCARD,
			Scope: "test",
		}

		ee := ExpectExpand{
			NumRequests: 2,
			NumSources:  3,
		}

		ee.Validate(t, e.ExpandRequest(&req))
	})

	t.Run("Wildcard both", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  sdp.WILDCARD,
			Scope: sdp.WILDCARD,
		}

		ee := ExpectExpand{
			NumRequests: 5,
			NumSources:  5,
		}

		ee.Validate(t, e.ExpandRequest(&req))
	})

	t.Run("Listing hidden source with wildcard scope", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "hidden_person",
			Scope: sdp.WILDCARD,
		}
		if x := len(e.ExpandRequest(&req)); x != 0 {
			t.Errorf("expected to find 0 sources, found %v", x)
		}

		req = sdp.ItemRequest{
			Type:  "hidden_person",
			Scope: "test",
		}
		if x := len(e.ExpandRequest(&req)); x != 1 {
			t.Errorf("expected to find 1 sources, found %v", x)
		}
	})
}

type ExpectExpand struct {
	NumRequests int

	// Note that this is not the number of unique sources, but teh number of
	// sources total. So if a source would be hit twice this will be 2
	NumSources int
}

func (e *ExpectExpand) Validate(t *testing.T, m map[*sdp.ItemRequest][]Source) {
	t.Helper()

	numSources := 0
	numRequests := 0

	for _, v := range m {
		numRequests++
		numSources = numSources + len(v)
	}

	if e.NumRequests != numRequests {
		t.Errorf("Expected %v requests, got %v", e.NumRequests, numRequests)
	}

	if e.NumSources != numSources {
		t.Errorf("Expected %v sources, got %v", e.NumSources, numSources)
	}
}

func TestSourceAdd(t *testing.T) {
	e := NewEngine()

	src := TestSource{}

	e.AddSources(&src)

	if x := len(e.Sources()); x != 1 {
		t.Fatalf("Expected 1 source, got %v", x)
	}
}

func TestGet(t *testing.T) {
	e := NewEngine()
	e.Name = "testEngine"

	src := TestSource{
		ReturnName: "orange",
		ReturnScopes: []string{
			"test",
			"empty",
		},
	}

	e.AddSources(&src)

	t.Run("Basic test", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		e.ExecuteRequest(context.Background(), &sdp.ItemRequest{
			Type:   "person",
			Scope:  "test",
			Query:  "three",
			Method: sdp.RequestMethod_GET,
		}, nil, nil)

		if x := len(src.GetCalls); x != 1 {
			t.Fatalf("Expected 1 get call, got %v", x)
		}

		firstCall := src.GetCalls[0]

		if firstCall[0] != "test" || firstCall[1] != "three" {
			t.Fatalf("First get call parameters unexpected: %v", firstCall)
		}
	})

	t.Run("not found error", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		items, errs, err := e.ExecuteRequestSync(context.Background(), &sdp.ItemRequest{
			Type:   "person",
			Scope:  "empty",
			Query:  "three",
			Method: sdp.RequestMethod_GET,
		})

		if err == nil {
			t.Error("execpected all sources failed")
		}

		if len(errs) == 1 {
			if errs[0].ErrorType != sdp.ItemRequestError_NOTFOUND {
				t.Errorf("expected ErrorType to be %v, got %v", sdp.ItemRequestError_NOTFOUND, errs[0].ErrorType)
			}
			if errs[0].ErrorString != "not found (test)" {
				t.Errorf("expected ErrorString to be %v, got %v", "", errs[0].ErrorString)
			}
			if errs[0].Scope != "empty" {
				t.Errorf("expected Scope to be %v, got %v", "empty", errs[0].Scope)
			}
			if errs[0].SourceName != "testSource-orange" {
				t.Errorf("expected SourceName to be %v, got %v", "testSource-orange", errs[0].SourceName)
			}
			if errs[0].ItemType != "person" {
				t.Errorf("expected ItemType to be %v, got %v", "person", errs[0].ItemType)
			}
			if errs[0].ResponderName != "testEngine" {
				t.Errorf("expected ResponderName to be %v, got %v", "testEngine", errs[0].ResponderName)
			}
		} else {
			t.Errorf("expected 1 error, got %v", len(errs))
		}

		if len(items) != 0 {
			t.Errorf("expected 0 items, got %v", len(items))
		}
	})

	t.Run("Test caching", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var finds1 []*sdp.Item
		var item2 []*sdp.Item
		var item3 []*sdp.Item
		var err error

		e.cache.MinWaitTime = (10 * time.Millisecond)
		e.cache.StartPurger(context.Background())
		req := sdp.ItemRequest{
			Type:   "person",
			Scope:  "test",
			Query:  "Dylan",
			Method: sdp.RequestMethod_GET,
		}

		finds1, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(20 * time.Millisecond)

		item2, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if finds1[0].Metadata.Timestamp.String() != item2[0].Metadata.Timestamp.String() {
			t.Errorf("Get requests 10ms apart had different timestamps, caching not working. %v != %v", finds1[0].Metadata.Timestamp.String(), item2[0].Metadata.Timestamp.String())
		}

		time.Sleep(200 * time.Millisecond)

		item3, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if item2[0].Metadata.Timestamp.String() == item3[0].Metadata.Timestamp.String() {
			t.Error("Get requests 200ms apart had the same timestamps, cache not expiring")
		}
	})

	t.Run("Test Get() caching errors", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		req := sdp.ItemRequest{
			Type:   "person",
			Scope:  "empty",
			Query:  "query",
			Method: sdp.RequestMethod_GET,
		}

		e.ExecuteRequestSync(context.Background(), &req)
		e.ExecuteRequestSync(context.Background(), &req)

		if l := len(src.GetCalls); l != 1 {
			t.Errorf("Expected 1 Get call due to caching og NOTFOUND errors, got %v", l)
		}
	})

	t.Run("Hidden items", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		src.IsHidden = true

		t.Run("Get", func(t *testing.T) {
			item, _, err := e.ExecuteRequestSync(context.Background(), &sdp.ItemRequest{
				Type:   "person",
				Scope:  "test",
				Query:  "three",
				Method: sdp.RequestMethod_GET,
			})

			if err != nil {
				t.Fatal(err)
			}

			if !item[0].Metadata.Hidden {
				t.Fatal("Item was not marked as hidden in metedata")
			}
		})

		t.Run("List", func(t *testing.T) {
			items, _, err := e.ExecuteRequestSync(context.Background(), &sdp.ItemRequest{
				Type:   "person",
				Scope:  "test",
				Method: sdp.RequestMethod_LIST,
			})

			if err != nil {
				t.Fatal(err)
			}

			if !items[0].Metadata.Hidden {
				t.Fatal("Item was not marked as hidden in metedata")
			}
		})

		t.Run("Search", func(t *testing.T) {
			items, _, err := e.ExecuteRequestSync(context.Background(), &sdp.ItemRequest{
				Type:   "person",
				Scope:  "test",
				Query:  "three",
				Method: sdp.RequestMethod_SEARCH,
			})

			if err != nil {
				t.Fatal(err)
			}

			if !items[0].Metadata.Hidden {
				t.Fatal("Item was not marked as hidden in metedata")
			}
		})
	})
}

func TestList(t *testing.T) {
	e := NewEngine()

	src := TestSource{}

	e.AddSources(&src)

	e.ExecuteRequestSync(context.Background(), &sdp.ItemRequest{
		Type:   "person",
		Scope:  "test",
		Method: sdp.RequestMethod_LIST,
	})

	if x := len(src.ListCalls); x != 1 {
		t.Fatalf("Expected 1 find call, got %v", x)
	}

	firstCall := src.ListCalls[0]

	if firstCall[0] != "test" {
		t.Fatalf("First find call parameters unexpected: %v", firstCall)
	}
}

func TestSearch(t *testing.T) {
	e := NewEngine()

	src := TestSource{}

	e.AddSources(&src)

	e.ExecuteRequestSync(context.Background(), &sdp.ItemRequest{
		Type:   "person",
		Scope:  "test",
		Query:  "query",
		Method: sdp.RequestMethod_SEARCH,
	})

	if x := len(src.SearchCalls); x != 1 {
		t.Fatalf("Expected 1 Search call, got %v", x)
	}

	firstCall := src.SearchCalls[0]

	if firstCall[0] != "test" || firstCall[1] != "query" {
		t.Fatalf("First Search call parameters unexpected: %v", firstCall)
	}
}

func TestListSearchCaching(t *testing.T) {
	e := NewEngine()

	src := TestSource{
		ReturnScopes: []string{
			"test",
			"empty",
			"error",
		},
	}

	e.AddSources(&src)
	e.cache.MinWaitTime = (10 * time.Millisecond)
	e.cache.StartPurger(context.Background())

	t.Run("caching with successful find", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var finds1 []*sdp.Item
		var finds2 []*sdp.Item
		var finds3 []*sdp.Item
		var err error
		req := sdp.ItemRequest{
			Type:   "person",
			Scope:  "test",
			Method: sdp.RequestMethod_LIST,
		}

		finds1, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		finds2, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if finds1[0].Metadata.Timestamp.String() != finds2[0].Metadata.Timestamp.String() {
			t.Error("List requests 10ms apart had different timestamps, caching not working")
		}

		time.Sleep(200 * time.Millisecond)

		finds3, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if finds2[0].Metadata.Timestamp.String() == finds3[0].Metadata.Timestamp.String() {
			t.Error("List requests 200ms apart had the same timestamps, cache not expiring")
		}
	})

	t.Run("empty find", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var err error
		req := sdp.ItemRequest{
			Type:   "person",
			Scope:  "empty",
			Method: sdp.RequestMethod_LIST,
		}

		_, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err == nil {
			t.Error("expected error but got nil")
		}

		time.Sleep(10 * time.Millisecond)

		_, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err == nil {
			t.Error("expected error but got nil")
		}

		if l := len(src.ListCalls); l != 1 {
			t.Errorf("Exected only 1 find call, got %v, cache not working", l)
		}

		time.Sleep(200 * time.Millisecond)

		_, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err == nil {
			t.Error("expected error but got nil")
		}

		if l := len(src.ListCalls); l != 2 {
			t.Errorf("Exected 2 find calls, got %v, cache not clearing", l)
		}
	})

	t.Run("caching with successful search", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var finds1 []*sdp.Item
		var finds2 []*sdp.Item
		var finds3 []*sdp.Item
		var err error
		req := sdp.ItemRequest{
			Type:   "person",
			Scope:  "test",
			Query:  "query",
			Method: sdp.RequestMethod_SEARCH,
		}

		finds1, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		finds2, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if finds1[0].Metadata.Timestamp.String() != finds2[0].Metadata.Timestamp.String() {
			t.Error("List requests 10ms apart had different timestamps, caching not working")
		}

		time.Sleep(200 * time.Millisecond)

		finds3, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if finds2[0].Metadata.Timestamp.String() == finds3[0].Metadata.Timestamp.String() {
			t.Error("List requests 200ms apart had the same timestamps, cache not expiring")
		}
	})

	t.Run("empty search", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var err error
		req := sdp.ItemRequest{
			Type:   "person",
			Scope:  "empty",
			Query:  "query",
			Method: sdp.RequestMethod_SEARCH,
		}

		_, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err == nil {
			t.Error("expected error but got nil")
		}

		time.Sleep(10 * time.Millisecond)

		_, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err == nil {
			t.Error("expected error but got nil")
		}

		if l := len(src.SearchCalls); l != 1 {
			t.Errorf("Exected only 1 find call, got %v, cache not working", l)
		}

		time.Sleep(200 * time.Millisecond)

		_, _, err = e.ExecuteRequestSync(context.Background(), &req)

		if err == nil {
			t.Error("expected error but got nil")
		}

		if l := len(src.SearchCalls); l != 2 {
			t.Errorf("Exected 2 find calls, got %v, cache not clearing", l)
		}
	})

	t.Run("non-caching of OTHER errors", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		req := sdp.ItemRequest{
			Type:   "person",
			Scope:  "error",
			Query:  "query",
			Method: sdp.RequestMethod_GET,
		}

		e.ExecuteRequestSync(context.Background(), &req)
		e.ExecuteRequestSync(context.Background(), &req)

		if l := len(src.GetCalls); l != 2 {
			t.Errorf("Exected 2 get calls, got %v, OTHER errors should not be cached", l)
		}
	})

	t.Run("non-caching when ignoreCache is specified", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		req := sdp.ItemRequest{
			Type:   "person",
			Scope:  "error",
			Query:  "query",
			Method: sdp.RequestMethod_GET,
		}

		e.ExecuteRequestSync(context.Background(), &req)
		e.ExecuteRequestSync(context.Background(), &req)

		req.Method = sdp.RequestMethod_LIST

		e.ExecuteRequestSync(context.Background(), &req)
		e.ExecuteRequestSync(context.Background(), &req)

		req.Method = sdp.RequestMethod_SEARCH

		e.ExecuteRequestSync(context.Background(), &req)
		e.ExecuteRequestSync(context.Background(), &req)

		if l := len(src.GetCalls); l != 2 {
			t.Errorf("Exected 2 get calls, got %v", l)
		}

		if l := len(src.ListCalls); l != 2 {
			t.Errorf("Exected 2 List calls, got %v", l)
		}

		if l := len(src.SearchCalls); l != 2 {
			t.Errorf("Exected 2 Search calls, got %v", l)
		}
	})

}

func TestSearchGetCaching(t *testing.T) {
	// We want to be sure that if an item has been found via a search and
	// cached, the cache will be hit if a Get is run for that particular item
	e := NewEngine()

	src := TestSource{
		ReturnScopes: []string{
			"test",
		},
	}

	e.AddSources(&src)
	e.cache.MinWaitTime = (10 * time.Millisecond)
	e.cache.StartPurger(context.Background())

	t.Run("caching with successful search", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var searchResult []*sdp.Item
		var searchErrors []*sdp.ItemRequestError
		var getResult []*sdp.Item
		var getErrors []*sdp.ItemRequestError
		var err error
		req := sdp.ItemRequest{
			Type:   "person",
			Scope:  "test",
			Query:  "Dylan",
			Method: sdp.RequestMethod_SEARCH,
		}

		searchResult, searchErrors, err = e.ExecuteRequestSync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if len(searchErrors) != 0 {
			for _, err := range searchErrors {
				t.Error(err)
			}
		}

		if len(searchResult) == 0 {
			t.Fatal("Got no results")
		}

		time.Sleep(10 * time.Millisecond)

		// Do a get request for that same item
		req.Method = sdp.RequestMethod_GET
		req.Query = searchResult[0].UniqueAttributeValue()

		getResult, getErrors, err = e.ExecuteRequestSync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if len(getErrors) != 0 {
			for _, err := range getErrors {
				t.Error(err)
			}
		}

		if len(getResult) == 0 {
			t.Error("No result from GET")
		}

		if searchResult[0].Metadata.Timestamp.String() != getResult[0].Metadata.Timestamp.String() {
			t.Error("Item timestamps do not match, caching has not worked")
		}
	})
}
