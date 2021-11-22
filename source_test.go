package discovery

import (
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
		if s := e.FilterSources("person", "wrong"); len(s) != 1 {
			t.Error("expected only wildcard context to match")
		}
	})

	t.Run("Right context wrong type", func(t *testing.T) {
		if s := e.FilterSources("wrong", "test"); len(s) != 0 {
			t.Error("found source when expecting no filter results")
		}
	})

	t.Run("Right both", func(t *testing.T) {
		if x := len(e.FilterSources("person", "test")); x != 2 {
			t.Errorf("expected to find 2 sources, found %v", x)
		}
	})

	t.Run("Multi-context", func(t *testing.T) {
		if x := len(e.FilterSources("chair", "testB")); x != 1 {
			t.Errorf("expected to find 1 source, found %v", x)
		}
	})

	t.Run("Wildcard context", func(t *testing.T) {
		if x := len(e.FilterSources("person", sdp.WILDCARD)); x != 2 {
			t.Errorf("expected to find 2 sources, found %v", x)
		}

		if x := len(e.FilterSources("chair", sdp.WILDCARD)); x != 1 {
			t.Errorf("expected to find 1 sources, found %v", x)
		}
	})

	t.Run("Wildcard type", func(t *testing.T) {
		if x := len(e.FilterSources(sdp.WILDCARD, "test")); x != 3 {
			t.Errorf("expected to find 3 sources, found %v", x)
		}
	})

	t.Run("Wildcard both", func(t *testing.T) {
		if x := len(e.FilterSources(sdp.WILDCARD, sdp.WILDCARD)); x != 4 {
			t.Errorf("expected to find 4 sources, found %v", x)
		}
	})

	t.Run("Finding hidden source with wildcard context", func(t *testing.T) {
		if x := len(e.FilterSources("hidden_person", sdp.WILDCARD)); x != 0 {
			t.Errorf("expected to find 0 sources, found %v", x)
		}

		if x := len(e.FilterSources("hidden_person", "test")); x != 1 {
			t.Errorf("expected to find 1 sources, found %v", x)
		}
	})
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

		e.Get(&sdp.ItemRequest{
			Type:    "person",
			Context: "test",
			Query:   "three",
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

		var finds1 *sdp.Item
		var item2 *sdp.Item
		var item3 *sdp.Item
		var err error

		e.cache.StartPurger()
		e.cache.MinWaitTime = (10 * time.Millisecond)
		req := sdp.ItemRequest{
			Type:    "person",
			Context: "test",
			Query:   "Dylan",
		}

		finds1, err = e.Get(&req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(20 * time.Millisecond)

		item2, err = e.Get(&req)

		if err != nil {
			t.Error(err)
		}

		if finds1.Metadata.Timestamp.String() != item2.Metadata.Timestamp.String() {
			t.Errorf("Get requests 10ms apart had different timestamps, caching not working. %v != %v", finds1.Metadata.Timestamp.String(), item2.Metadata.Timestamp.String())
		}

		time.Sleep(200 * time.Millisecond)

		item3, err = e.Get(&req)

		if err != nil {
			t.Error(err)
		}

		if item2.Metadata.Timestamp.String() == item3.Metadata.Timestamp.String() {
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
		}

		e.Get(&req)
		e.Get(&req)

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
			item, err := e.Get(&sdp.ItemRequest{
				Type:    "person",
				Context: "test",
				Query:   "three",
			})

			if err != nil {
				t.Fatal(err)
			}

			if !item.Metadata.Hidden {
				t.Fatal("Item was not marked as hidden in metedata")
			}
		})

		t.Run("Find", func(t *testing.T) {
			items, err := e.Find(&sdp.ItemRequest{
				Type:    "person",
				Context: "test",
			})

			if err != nil {
				t.Fatal(err)
			}

			if !items[0].Metadata.Hidden {
				t.Fatal("Item was not marked as hidden in metedata")
			}
		})

		t.Run("Search", func(t *testing.T) {
			items, err := e.Search(&sdp.ItemRequest{
				Type:    "person",
				Context: "test",
				Query:   "three",
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

	e.Find(&sdp.ItemRequest{
		Type:    "person",
		Context: "test",
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

	e.Search(&sdp.ItemRequest{
		Type:    "person",
		Context: "test",
		Query:   "query",
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
		}

		finds1, err = e.Find(&req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		finds2, err = e.Find(&req)

		if err != nil {
			t.Error(err)
		}

		if finds1[0].Metadata.Timestamp.String() != finds2[0].Metadata.Timestamp.String() {
			t.Error("Find requests 10ms apart had different timestamps, caching not working")
		}

		time.Sleep(200 * time.Millisecond)

		finds3, err = e.Find(&req)

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
		}

		_, err = e.Find(&req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		_, err = e.Find(&req)

		if err != nil {
			t.Error(err)
		}

		if l := len(src.FindCalls); l != 1 {
			t.Errorf("Exected only 1 find call, got %v, cache not working", l)
		}

		time.Sleep(200 * time.Millisecond)

		_, err = e.Find(&req)

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
		}

		finds1, err = e.Search(&req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		finds2, err = e.Search(&req)

		if err != nil {
			t.Error(err)
		}

		if finds1[0].Metadata.Timestamp.String() != finds2[0].Metadata.Timestamp.String() {
			t.Error("Find requests 10ms apart had different timestamps, caching not working")
		}

		time.Sleep(200 * time.Millisecond)

		finds3, err = e.Search(&req)

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
		}

		_, err = e.Search(&req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		_, err = e.Search(&req)

		if err != nil {
			t.Error(err)
		}

		if l := len(src.SearchCalls); l != 1 {
			t.Errorf("Exected only 1 find call, got %v, cache not working", l)
		}

		time.Sleep(200 * time.Millisecond)

		_, err = e.Search(&req)

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
		}

		e.Get(&req)
		e.Get(&req)

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
		}

		e.Get(&req)
		e.Get(&req)
		e.Find(&req)
		e.Find(&req)
		e.Search(&req)
		e.Search(&req)

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
