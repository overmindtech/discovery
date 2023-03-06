package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/overmindtech/sdp-go"
)

func TestEngineAddSources(t *testing.T) {
	e, err := NewEngine()
	if err != nil {
		t.Fatalf("Error initializing Engine: %v", err)
	}

	src := TestSource{}

	e.AddSources(&src)

	if x := len(e.sh.Sources()); x != 4 {
		t.Fatalf("Expected 4 source, got %v", x)
	}
}

func TestGet(t *testing.T) {
	src := TestSource{
		ReturnName: "orange",
		ReturnScopes: []string{
			"test",
			"empty",
		},
	}

	e := newStartedEngine(t, "TestGet", nil, &src)

	t.Run("Basic test", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		e.ExecuteQuery(context.Background(), &sdp.Query{
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

		items, errs, err := e.ExecuteQuerySync(context.Background(), &sdp.Query{
			Type:   "person",
			Scope:  "empty",
			Query:  "three",
			Method: sdp.RequestMethod_GET,
		})

		if err == nil {
			t.Error("expected all sources failed")
		}

		if len(errs) == 1 {
			if errs[0].ErrorType != sdp.QueryError_NOTFOUND {
				t.Errorf("expected ErrorType to be %v, got %v", sdp.QueryError_NOTFOUND, errs[0].ErrorType)
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
			if errs[0].ResponderName != "TestGet" {
				t.Errorf("expected ResponderName to be %v, got %v", "TestGet", errs[0].ResponderName)
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
		req := sdp.Query{
			Type:   "person",
			Scope:  "test",
			Query:  "Dylan",
			Method: sdp.RequestMethod_GET,
		}

		finds1, _, err = e.ExecuteQuerySync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(20 * time.Millisecond)

		item2, _, err = e.ExecuteQuerySync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if finds1[0].Metadata.Timestamp.String() != item2[0].Metadata.Timestamp.String() {
			t.Errorf("Get queries 10ms apart had different timestamps, caching not working. %v != %v", finds1[0].Metadata.Timestamp.String(), item2[0].Metadata.Timestamp.String())
		}

		time.Sleep(200 * time.Millisecond)

		item3, _, err = e.ExecuteQuerySync(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if item2[0].Metadata.Timestamp.String() == item3[0].Metadata.Timestamp.String() {
			t.Error("Get queries 200ms apart had the same timestamps, cache not expiring")
		}
	})

	t.Run("Test Get() caching errors", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		req := sdp.Query{
			Type:   "person",
			Scope:  "empty",
			Query:  "query",
			Method: sdp.RequestMethod_GET,
		}

		e.ExecuteQuerySync(context.Background(), &req)
		e.ExecuteQuerySync(context.Background(), &req)

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
			item, _, err := e.ExecuteQuerySync(context.Background(), &sdp.Query{
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
			items, _, err := e.ExecuteQuerySync(context.Background(), &sdp.Query{
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
			items, _, err := e.ExecuteQuerySync(context.Background(), &sdp.Query{
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
	src := TestSource{}

	e := newStartedEngine(t, "TestList", nil, &src)

	e.ExecuteQuerySync(context.Background(), &sdp.Query{
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
	src := TestSource{}

	e := newStartedEngine(t, "TestSearch", nil, &src)

	e.ExecuteQuerySync(context.Background(), &sdp.Query{
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
	src := TestSource{
		ReturnScopes: []string{
			"test",
			"empty",
			"error",
		},
	}

	e := newStartedEngine(t, "TestListSearchCaching", nil, &src)

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
		q := sdp.Query{
			Type:   "person",
			Scope:  "test",
			Method: sdp.RequestMethod_LIST,
		}

		finds1, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		finds2, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err != nil {
			t.Error(err)
		}

		if finds1[0].Metadata.Timestamp.String() != finds2[0].Metadata.Timestamp.String() {
			t.Error("List queries 10ms apart had different timestamps, caching not working")
		}

		time.Sleep(200 * time.Millisecond)

		finds3, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err != nil {
			t.Error(err)
		}

		if finds2[0].Metadata.Timestamp.String() == finds3[0].Metadata.Timestamp.String() {
			t.Error("List queries 200ms apart had the same timestamps, cache not expiring")
		}
	})

	t.Run("empty find", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var err error
		q := sdp.Query{
			Type:   "person",
			Scope:  "empty",
			Method: sdp.RequestMethod_LIST,
		}

		_, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err == nil {
			t.Error("expected error but got nil")
		}

		time.Sleep(10 * time.Millisecond)

		_, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err == nil {
			t.Error("expected error but got nil")
		}

		if l := len(src.ListCalls); l != 1 {
			t.Errorf("Expected only 1 find call, got %v, cache not working", l)
		}

		time.Sleep(200 * time.Millisecond)

		_, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err == nil {
			t.Error("expected error but got nil")
		}

		if l := len(src.ListCalls); l != 2 {
			t.Errorf("Expected 2 find calls, got %v, cache not clearing", l)
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
		q := sdp.Query{
			Type:   "person",
			Scope:  "test",
			Query:  "query",
			Method: sdp.RequestMethod_SEARCH,
		}

		finds1, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		finds2, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err != nil {
			t.Error(err)
		}

		if finds1[0].Metadata.Timestamp.String() != finds2[0].Metadata.Timestamp.String() {
			t.Error("List queries 10ms apart had different timestamps, caching not working")
		}

		time.Sleep(200 * time.Millisecond)

		finds3, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err != nil {
			t.Error(err)
		}

		if finds2[0].Metadata.Timestamp.String() == finds3[0].Metadata.Timestamp.String() {
			t.Error("List queries 200ms apart had the same timestamps, cache not expiring")
		}
	})

	t.Run("empty search", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var err error
		q := sdp.Query{
			Type:   "person",
			Scope:  "empty",
			Query:  "query",
			Method: sdp.RequestMethod_SEARCH,
		}

		_, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err == nil {
			t.Error("expected error but got nil")
		}

		time.Sleep(10 * time.Millisecond)

		_, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err == nil {
			t.Error("expected error but got nil")
		}

		if l := len(src.SearchCalls); l != 1 {
			t.Errorf("Expected only 1 find call, got %v, cache not working", l)
		}

		time.Sleep(200 * time.Millisecond)

		_, _, err = e.ExecuteQuerySync(context.Background(), &q)

		if err == nil {
			t.Error("expected error but got nil")
		}

		if l := len(src.SearchCalls); l != 2 {
			t.Errorf("Expected 2 find calls, got %v, cache not clearing", l)
		}
	})

	t.Run("non-caching of OTHER errors", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		q := sdp.Query{
			Type:   "person",
			Scope:  "error",
			Query:  "query",
			Method: sdp.RequestMethod_GET,
		}

		e.ExecuteQuerySync(context.Background(), &q)
		e.ExecuteQuerySync(context.Background(), &q)

		if l := len(src.GetCalls); l != 2 {
			t.Errorf("Expected 2 get calls, got %v, OTHER errors should not be cached", l)
		}
	})

	t.Run("non-caching when ignoreCache is specified", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		q := sdp.Query{
			Type:   "person",
			Scope:  "error",
			Query:  "query",
			Method: sdp.RequestMethod_GET,
		}

		e.ExecuteQuerySync(context.Background(), &q)
		e.ExecuteQuerySync(context.Background(), &q)

		q.Method = sdp.RequestMethod_LIST

		e.ExecuteQuerySync(context.Background(), &q)
		e.ExecuteQuerySync(context.Background(), &q)

		q.Method = sdp.RequestMethod_SEARCH

		e.ExecuteQuerySync(context.Background(), &q)
		e.ExecuteQuerySync(context.Background(), &q)

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

	src := TestSource{
		ReturnScopes: []string{
			"test",
		},
	}

	e := newStartedEngine(t, "TestSearchGetCaching", nil, &src)
	e.cache.MinWaitTime = (10 * time.Millisecond)
	e.cache.StartPurger(context.Background())

	t.Run("caching with successful search", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var searchResult []*sdp.Item
		var searchErrors []*sdp.QueryError
		var getResult []*sdp.Item
		var getErrors []*sdp.QueryError
		var err error
		q := sdp.Query{
			Type:   "person",
			Scope:  "test",
			Query:  "Dylan",
			Method: sdp.RequestMethod_SEARCH,
		}

		searchResult, searchErrors, err = e.ExecuteQuerySync(context.Background(), &q)

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

		// Do a get query for that same item
		q.Method = sdp.RequestMethod_GET
		q.Query = searchResult[0].UniqueAttributeValue()

		getResult, getErrors, err = e.ExecuteQuerySync(context.Background(), &q)

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
