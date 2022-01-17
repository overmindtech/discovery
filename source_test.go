package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/overmindtech/sdp-go"
)

func TestFilterSources(t *testing.T) {
	e := Engine{
		Name: "testEngine",
	}

	e.AddSources(
		&TestSource{
			ReturnContexts: []string{"test"},
			ReturnType:     "person",
		},
		&TestSource{
			ReturnContexts: []string{"test"},
			ReturnType:     "fish",
		},
		&TestSource{
			ReturnContexts: []string{sdp.WILDCARD},
			ReturnType:     "person",
		},
		&TestSource{
			ReturnContexts: []string{
				"testA",
				"testB",
			},
			ReturnType: "chair",
		},
		&TestSource{
			ReturnContexts: []string{"test"},
			ReturnType:     "hidden_person",
			IsHidden:       true,
		},
	)

	t.Run("Right type wrong context", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:    "person",
			Context: "wrong",
		}

		ee := ExpectExpand{
			NumRequests: 1,
			NumSources:  1,
		}

		ee.Validate(t, e.ExpandRequest(&req))

		if !e.WillRespond(&req) {
			t.Error("Engine reported that it will not respond to this request, it should be responding")
		}
	})

	t.Run("Right context wrong type", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:    "wrong",
			Context: "test",
		}

		ee := ExpectExpand{
			NumRequests: 0,
			NumSources:  0,
		}

		ee.Validate(t, e.ExpandRequest(&req))
	})

	t.Run("Right both", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:    "person",
			Context: "test",
		}

		ee := ExpectExpand{
			NumRequests: 1,
			NumSources:  2,
		}

		ee.Validate(t, e.ExpandRequest(&req))

		if !e.WillRespond(&req) {
			t.Error("Engine reported that it will not respond to this request, it should be responding")
		}
	})

	t.Run("Multi-context", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:    "chair",
			Context: "testB",
		}

		ee := ExpectExpand{
			NumRequests: 1,
			NumSources:  1,
		}

		ee.Validate(t, e.ExpandRequest(&req))

		if !e.WillRespond(&req) {
			t.Error("Engine reported that it will not respond to this request, it should be responding")
		}
	})

	t.Run("Wildcard context", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:    "person",
			Context: sdp.WILDCARD,
		}

		ee := ExpectExpand{
			NumRequests: 2,
			NumSources:  2,
		}

		ee.Validate(t, e.ExpandRequest(&req))

		if !e.WillRespond(&req) {
			t.Error("Engine reported that it will not respond to this request, it should be responding")
		}

		req = sdp.ItemRequest{
			Type:    "chair",
			Context: sdp.WILDCARD,
		}

		ee = ExpectExpand{
			NumRequests: 2,
			NumSources:  2,
		}

		ee.Validate(t, e.ExpandRequest(&req))

		if !e.WillRespond(&req) {
			t.Error("Engine reported that it will not respond to this request, it should be responding")
		}
	})

	t.Run("Wildcard type", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:    sdp.WILDCARD,
			Context: "test",
		}

		ee := ExpectExpand{
			NumRequests: 2,
			NumSources:  3,
		}

		ee.Validate(t, e.ExpandRequest(&req))

		if !e.WillRespond(&req) {
			t.Error("Engine reported that it will not respond to this request, it should be responding")
		}
	})

	t.Run("Wildcard both", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:    sdp.WILDCARD,
			Context: sdp.WILDCARD,
		}

		ee := ExpectExpand{
			NumRequests: 5,
			NumSources:  5,
		}

		ee.Validate(t, e.ExpandRequest(&req))

		if !e.WillRespond(&req) {
			t.Error("Engine reported that it will not respond to this request, it should be responding")
		}
	})

	t.Run("Finding hidden source with wildcard context", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:    "hidden_person",
			Context: sdp.WILDCARD,
		}
		if x := len(e.ExpandRequest(&req)); x != 0 {
			t.Errorf("expected to find 0 sources, found %v", x)
		}

		req = sdp.ItemRequest{
			Type:    "hidden_person",
			Context: "test",
		}
		if x := len(e.ExpandRequest(&req)); x != 1 {
			t.Errorf("expected to find 1 sources, found %v", x)
		}

		if !e.WillRespond(&req) {
			t.Error("Engine reported that it will not respond to this request, it should be responding")
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
	e := Engine{
		Name: "testEngine",
	}

	src := TestSource{}

	e.AddSources(&src)

	if x := len(e.Sources()); x != 1 {
		t.Fatalf("Expected 1 source, got %v", x)
	}
}

func TestGet(t *testing.T) {
	e := Engine{
		Name: "testEngine",
	}

	src := TestSource{
		ReturnContexts: []string{
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
			Type:    "person",
			Context: "test",
			Query:   "three",
			Method:  sdp.RequestMethod_GET,
		})

		if x := len(src.GetCalls); x != 1 {
			t.Fatalf("Expected 1 get call, got %v", x)
		}

		firstCall := src.GetCalls[0]

		if firstCall[0] != "test" || firstCall[1] != "three" {
			t.Fatalf("First get call parameters unexpected: %v", firstCall)
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
		e.cache.StartPurger()
		req := sdp.ItemRequest{
			Type:    "person",
			Context: "test",
			Query:   "Dylan",
			Method:  sdp.RequestMethod_GET,
		}

		finds1, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(20 * time.Millisecond)

		item2, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if finds1[0].Metadata.Timestamp.String() != item2[0].Metadata.Timestamp.String() {
			t.Errorf("Get requests 10ms apart had different timestamps, caching not working. %v != %v", finds1[0].Metadata.Timestamp.String(), item2[0].Metadata.Timestamp.String())
		}

		time.Sleep(200 * time.Millisecond)

		item3, err = e.ExecuteRequest(context.Background(), &req)

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
			Type:    "person",
			Context: "empty",
			Query:   "query",
			Method:  sdp.RequestMethod_GET,
		}

		e.ExecuteRequest(context.Background(), &req)
		e.ExecuteRequest(context.Background(), &req)

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
			item, err := e.ExecuteRequest(context.Background(), &sdp.ItemRequest{
				Type:    "person",
				Context: "test",
				Query:   "three",
				Method:  sdp.RequestMethod_GET,
			})

			if err != nil {
				t.Fatal(err)
			}

			if !item[0].Metadata.Hidden {
				t.Fatal("Item was not marked as hidden in metedata")
			}
		})

		t.Run("Find", func(t *testing.T) {
			items, err := e.ExecuteRequest(context.Background(), &sdp.ItemRequest{
				Type:    "person",
				Context: "test",
				Method:  sdp.RequestMethod_FIND,
			})

			if err != nil {
				t.Fatal(err)
			}

			if !items[0].Metadata.Hidden {
				t.Fatal("Item was not marked as hidden in metedata")
			}
		})

		t.Run("Search", func(t *testing.T) {
			items, err := e.ExecuteRequest(context.Background(), &sdp.ItemRequest{
				Type:    "person",
				Context: "test",
				Query:   "three",
				Method:  sdp.RequestMethod_SEARCH,
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

func TestFind(t *testing.T) {
	e := Engine{
		Name: "testEngine",
	}

	src := TestSource{}

	e.AddSources(&src)

	e.ExecuteRequest(context.Background(), &sdp.ItemRequest{
		Type:    "person",
		Context: "test",
		Method:  sdp.RequestMethod_FIND,
	})

	if x := len(src.FindCalls); x != 1 {
		t.Fatalf("Expected 1 find call, got %v", x)
	}

	firstCall := src.FindCalls[0]

	if firstCall[0] != "test" {
		t.Fatalf("First find call parameters unexpected: %v", firstCall)
	}
}

func TestSearch(t *testing.T) {
	e := Engine{
		Name: "testEngine",
	}

	src := TestSource{}

	e.AddSources(&src)

	e.ExecuteRequest(context.Background(), &sdp.ItemRequest{
		Type:    "person",
		Context: "test",
		Query:   "query",
		Method:  sdp.RequestMethod_SEARCH,
	})

	if x := len(src.SearchCalls); x != 1 {
		t.Fatalf("Expected 1 Search call, got %v", x)
	}

	firstCall := src.SearchCalls[0]

	if firstCall[0] != "test" || firstCall[1] != "query" {
		t.Fatalf("First Search call parameters unexpected: %v", firstCall)
	}
}

func TestFindSearchCaching(t *testing.T) {
	e := Engine{
		Name: "testEngine",
	}

	src := TestSource{
		ReturnContexts: []string{
			"test",
			"empty",
			"error",
		},
	}

	e.AddSources(&src)
	e.cache.MinWaitTime = (10 * time.Millisecond)
	e.cache.StartPurger()

	t.Run("caching with successful find", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var finds1 []*sdp.Item
		var finds2 []*sdp.Item
		var finds3 []*sdp.Item
		var err error
		req := sdp.ItemRequest{
			Type:    "person",
			Context: "test",
			Method:  sdp.RequestMethod_FIND,
		}

		finds1, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		finds2, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if finds1[0].Metadata.Timestamp.String() != finds2[0].Metadata.Timestamp.String() {
			t.Error("Find requests 10ms apart had different timestamps, caching not working")
		}

		time.Sleep(200 * time.Millisecond)

		finds3, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if finds2[0].Metadata.Timestamp.String() == finds3[0].Metadata.Timestamp.String() {
			t.Error("Find requests 200ms apart had the same timestamps, cache not expiring")
		}
	})

	t.Run("empty find", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var err error
		req := sdp.ItemRequest{
			Type:    "person",
			Context: "empty",
			Method:  sdp.RequestMethod_FIND,
		}

		_, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		_, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if l := len(src.FindCalls); l != 1 {
			t.Errorf("Exected only 1 find call, got %v, cache not working", l)
		}

		time.Sleep(200 * time.Millisecond)

		_, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if l := len(src.FindCalls); l != 2 {
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
			Type:    "person",
			Context: "test",
			Query:   "query",
			Method:  sdp.RequestMethod_SEARCH,
		}

		finds1, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		finds2, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if finds1[0].Metadata.Timestamp.String() != finds2[0].Metadata.Timestamp.String() {
			t.Error("Find requests 10ms apart had different timestamps, caching not working")
		}

		time.Sleep(200 * time.Millisecond)

		finds3, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if finds2[0].Metadata.Timestamp.String() == finds3[0].Metadata.Timestamp.String() {
			t.Error("Find requests 200ms apart had the same timestamps, cache not expiring")
		}
	})

	t.Run("empty search", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		var err error
		req := sdp.ItemRequest{
			Type:    "person",
			Context: "empty",
			Query:   "query",
			Method:  sdp.RequestMethod_SEARCH,
		}

		_, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		_, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
		}

		if l := len(src.SearchCalls); l != 1 {
			t.Errorf("Exected only 1 find call, got %v, cache not working", l)
		}

		time.Sleep(200 * time.Millisecond)

		_, err = e.ExecuteRequest(context.Background(), &req)

		if err != nil {
			t.Error(err)
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
			Type:    "person",
			Context: "error",
			Query:   "query",
			Method:  sdp.RequestMethod_GET,
		}

		e.ExecuteRequest(context.Background(), &req)
		e.ExecuteRequest(context.Background(), &req)

		if l := len(src.GetCalls); l != 2 {
			t.Errorf("Exected 2 get calls, got %v, OTHER errors should not be cached", l)
		}
	})

	t.Run("non-caching when ignoreCache is specified", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		req := sdp.ItemRequest{
			Type:    "person",
			Context: "error",
			Query:   "query",
			Method:  sdp.RequestMethod_GET,
		}

		e.ExecuteRequest(context.Background(), &req)
		e.ExecuteRequest(context.Background(), &req)

		req.Method = sdp.RequestMethod_FIND

		e.ExecuteRequest(context.Background(), &req)
		e.ExecuteRequest(context.Background(), &req)

		req.Method = sdp.RequestMethod_SEARCH

		e.ExecuteRequest(context.Background(), &req)
		e.ExecuteRequest(context.Background(), &req)

		if l := len(src.GetCalls); l != 2 {
			t.Errorf("Exected 2 get calls, got %v", l)
		}

		if l := len(src.FindCalls); l != 2 {
			t.Errorf("Exected 2 Find calls, got %v", l)
		}

		if l := len(src.SearchCalls); l != 2 {
			t.Errorf("Exected 2 Search calls, got %v", l)
		}
	})

}
