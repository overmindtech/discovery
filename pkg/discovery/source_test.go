package discovery

import (
	"testing"
	"time"

	"github.com/dylanratcliffe/sdp-go"
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
			ReturnContexts: []string{AllContexts},
			ReturnType:     "person",
		},
		&TestSource{
			ReturnContexts: []string{
				"testA",
				"testB",
			},
			ReturnType: "chair",
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

	src := TestSource{}

	e.AddSources(&src)

	t.Run("Basic test", func(t *testing.T) {
		e.Get("person", "test", "three")

		if x := len(src.GetCalls); x != 1 {
			t.Fatalf("Expected 1 get call, got %v", x)
		}

		firstCall := src.GetCalls[0]

		if firstCall[0] != "test" || firstCall[1] != "three" {
			t.Fatalf("First get call parameters unexpected: %v", firstCall)
		}
	})

	t.Run("Test caching", func(t *testing.T) {
		var item1 *sdp.Item
		var item2 *sdp.Item
		var item3 *sdp.Item
		var err error

		e.cache.StartPurger()
		e.cache.MinWaitTime = (10 * time.Millisecond)

		item1, err = e.Get("person", "test", "Dylan")

		if err != nil {
			t.Error(err)
		}

		time.Sleep(10 * time.Millisecond)

		item2, err = e.Get("person", "test", "Dylan")

		if err != nil {
			t.Error(err)
		}

		if item1.Metadata.Timestamp.String() != item2.Metadata.Timestamp.String() {
			t.Error("Get requests 10ms apart had different timestamps, caching not working")
		}

		time.Sleep(200 * time.Millisecond)

		item3, err = e.Get("person", "test", "Dylan")

		if err != nil {
			t.Error(err)
		}

		if item2.Metadata.Timestamp.String() == item3.Metadata.Timestamp.String() {
			t.Error("Get requests 200ms apart had the same timestamps, cache not expiring")
		}
	})
}

func TestFind(t *testing.T) {
	e := Engine{
		Name: "testEngine",
	}

	src := TestSource{}

	e.AddSources(&src)

	e.Find("person", "test")

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

	e.Search("person", "test", "query")

	if x := len(src.SearchCalls); x != 1 {
		t.Fatalf("Expected 1 Search call, got %v", x)
	}

	firstCall := src.SearchCalls[0]

	if firstCall[0] != "test" || firstCall[1] != "query" {
		t.Fatalf("First Search call parameters unexpected: %v", firstCall)
	}
}
