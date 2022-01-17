package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestExecuteRequest(t *testing.T) {
	e := Engine{
		Name: "test",
	}

	src := TestSource{
		ReturnType:     "person",
		ReturnContexts: []string{"test"},
	}

	e.AddSources(&src)

	t.Run("Basic happy-path Get request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "foo",
			Context:         "test",
			LinkDepth:       3,
			ItemSubject:     "items",
			ResponseSubject: "responses",
		}

		items, err := e.ExecuteRequest(context.Background(), request)

		if err != nil {
			t.Error(err)
		}

		if x := len(src.GetCalls); x != 1 {
			t.Errorf("expected source's Get() to have been called 1 time, got %v", x)
		}

		if x := len(items); x != 1 {
			t.Errorf("expected 1 item, got %v", x)
		}

		item := items[0]
		itemRequest := item.LinkedItemRequests[0]

		if ld := itemRequest.LinkDepth; ld != 2 {
			t.Errorf("expected linked item depth to be 1 less than the query (2), got %v", ld)
		}

		if is := itemRequest.ItemSubject; is != "items" {
			t.Errorf("expected linked item request itemsubject to be \"items\" got %v", is)
		}

		if is := itemRequest.ResponseSubject; is != "responses" {
			t.Errorf("expected linked item request ResponseSubject to be \"responses\" got %v", is)
		}

		if item.Metadata.SourceRequest != request {
			t.Error("source request mismatch")
		}

	})

	t.Run("Wrong context Get request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Query:     "foo",
			Context:   "wrong",
			LinkDepth: 0,
		}

		_, err := e.ExecuteRequest(context.Background(), request)

		if ire, ok := err.(*sdp.ItemRequestError); ok {
			if ire.ErrorType != sdp.ItemRequestError_NOCONTEXT {
				t.Errorf("expected error type to be NOCONTEXT, got %v", ire.ErrorType)
			}
		} else {
			t.Errorf("expected error to be ItemRequestError, got %T", err)
		}
	})

	t.Run("Wrong type Get request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:      "house",
			Method:    sdp.RequestMethod_GET,
			Query:     "foo",
			Context:   "test",
			LinkDepth: 0,
		}

		_, err := e.ExecuteRequest(context.Background(), request)

		if ire, ok := err.(*sdp.ItemRequestError); ok {
			if ire.ErrorType != sdp.ItemRequestError_NOCONTEXT {
				t.Errorf("expected error type to be NOCONTEXT, got %v", ire.ErrorType)
			}
		} else {
			t.Errorf("expected error to be ItemRequestError, got %T", err)
		}
	})

	t.Run("Basic Find request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_FIND,
			Context:   "test",
			LinkDepth: 5,
		}

		items, err := e.ExecuteRequest(context.Background(), request)

		if err != nil {
			t.Error(err)
		}

		if len(items) < 1 {
			t.Error("expected at least one item")
		}
	})

	t.Run("Basic Search request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_SEARCH,
			Query:     "TEST",
			Context:   "test",
			LinkDepth: 5,
		}

		items, err := e.ExecuteRequest(context.Background(), request)

		if err != nil {
			t.Error(err)
		}

		if len(items) < 1 {
			t.Error("expected at least one item")
		}
	})

}

func TestNewItemRequestHandler(t *testing.T) {
	e := Engine{
		Name: "test",
	}

	personSource := TestSource{
		ReturnType: "person",
		ReturnContexts: []string{
			"test1",
			"test2",
		},
	}

	dogSource := TestSource{
		ReturnType: "dog",
		ReturnContexts: []string{
			"test1",
			"testA",
			"testB",
		},
	}

	e.AddSources(&personSource, &dogSource)

	t.Run("Wildcard type should be expanded", func(t *testing.T) {
		t.Cleanup(func() {
			personSource.ClearCalls()
			dogSource.ClearCalls()
		})

		req := sdp.ItemRequest{
			Type:      sdp.WILDCARD,
			Method:    sdp.RequestMethod_GET,
			Query:     "Dylan",
			Context:   "test1",
			LinkDepth: 0,
		}

		// Run the handler
		e.ItemRequestHandler(&req)

		// I'm expecting both sources to get a request since the type was *
		if l := len(personSource.GetCalls); l != 1 {
			t.Errorf("expected person backend to have 1 Get call, got %v", l)
		}

		if l := len(dogSource.GetCalls); l != 1 {
			t.Errorf("expected dog backend to have 1 Get call, got %v", l)
		}
	})

	t.Run("Wildcard context should be expanded", func(t *testing.T) {
		t.Cleanup(func() {
			personSource.ClearCalls()
			dogSource.ClearCalls()
		})

		req := sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Query:     "Dylan1",
			Context:   sdp.WILDCARD,
			LinkDepth: 0,
		}

		// Run the handler
		e.ItemRequestHandler(&req)

		if l := len(personSource.GetCalls); l != 2 {
			t.Errorf("expected person backend to have 2 Get calls, got %v", l)
		}

		if l := len(dogSource.GetCalls); l != 0 {
			t.Errorf("expected dog backend to have 0 Get calls, got %v", l)
		}
	})

}

func TestWildcardSourceExpansion(t *testing.T) {
	e := Engine{
		Name: "test",
	}

	personSource := TestSource{
		ReturnType: "person",
		ReturnContexts: []string{
			sdp.WILDCARD,
		},
	}

	e.AddSources(&personSource)

	t.Run("request context should be preserved", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Query:     "Dylan1",
			Context:   "something.specific",
			LinkDepth: 0,
		}

		// Run the handler
		e.ItemRequestHandler(&req)

		if len(personSource.GetCalls) != 1 {
			t.Errorf("expected 1 get call got %v", len(personSource.GetCalls))
		}

		call := personSource.GetCalls[0]

		if expected := "something.specific"; call[0] != expected {
			t.Errorf("expected itemContext to be %v, got %v", expected, call[0])
		}

		if expected := "Dylan1"; call[1] != expected {
			t.Errorf("expected query to be %v, got %v", expected, call[1])
		}
	})
}

func TestSendRequestSync(t *testing.T) {
	SkipWithoutNats(t)

	e := Engine{
		Name: "nats-test",
		NATSOptions: &NATSOptions{
			URLs:           NatsTestURLs,
			ConnectionName: "test-connection",
			ConnectTimeout: time.Second,
			NumRetries:     5,
			QueueName:      "test",
		},
		MaxParallelExecutions: 10,
	}

	src := TestSource{
		ReturnType: "person",
		ReturnContexts: []string{
			"test",
		},
	}

	e.AddSources(&src)

	err := e.Connect()

	if err != nil {
		t.Fatal(err)
	}

	err = e.Start()

	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		u := uuid.New()

		var progress *sdp.RequestProgress
		var items []*sdp.Item

		progress, items, err = e.SendRequestSync(&sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "Dylan",
			Context:         "test",
			LinkDepth:       0,
			IgnoreCache:     false,
			UUID:            u[:],
			Timeout:         durationpb.New(10 * time.Minute),
			ItemSubject:     NewItemSubject(),
			ResponseSubject: NewResponseSubject(),
		})

		if err != nil {
			t.Fatal(err)
		}

		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %v", len(items))
		}

		if progress.NumComplete() != 1 {
			t.Fatalf("expected 1 to be complete, got %v", progress.NumComplete())
		}

	}

}

func TestExpandRequest(t *testing.T) {
	e := Engine{
		Name: "expand-test",
	}

	t.Run("with a single source with a single context", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		simple := TestSource{
			ReturnContexts: []string{
				"test1",
			},
		}

		e.AddSources(&simple)

		e.ItemRequestHandler(&sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_GET,
			Query:   "Debby",
			Context: "*",
		})

		if expected := 1; len(simple.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, len(simple.GetCalls))
		}
	})

	t.Run("with a single source with many contexts", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		many := TestSource{
			ReturnName: "many",
			ReturnContexts: []string{
				"test1",
				"test2",
				"test3",
			},
		}

		e.AddSources(&many)

		e.ItemRequestHandler(&sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_GET,
			Query:   "Debby",
			Context: "*",
		})

		if expected := 3; len(many.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, many.GetCalls)
		}
	})

	t.Run("with many sources with single contexts", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnContexts: []string{
				"test1",
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnContexts: []string{
				"test2",
			},
		}

		e.AddSources(&sx)
		e.AddSources(&sy)

		e.ItemRequestHandler(&sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_GET,
			Query:   "Daniel",
			Context: "*",
		})

		if expected := 1; len(sx.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.GetCalls)
		}

		if expected := 1; len(sy.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.GetCalls)
		}
	})

	t.Run("with many sources with many contexts", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnContexts: []string{
				"test1",
				"test2",
				"test3",
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnContexts: []string{
				"test4",
				"test5",
				"test6",
			},
		}

		e.AddSources(&sx)
		e.AddSources(&sy)

		e.ItemRequestHandler(&sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_GET,
			Query:   "Steven",
			Context: "*",
		})

		if expected := 3; len(sx.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.GetCalls)
		}

		if expected := 3; len(sy.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.GetCalls)
		}
	})

	t.Run("with many sources with many contexts which overlap GET", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnContexts: []string{
				"test1",
				"test2",
				"test3",
			},
			ReturnWeight: 10,
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnContexts: []string{
				"test2",
				"test3",
				"test4",
			},
			ReturnWeight: 11,
		}

		e.AddSources(&sx)
		e.AddSources(&sy)

		e.ItemRequestHandler(&sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_GET,
			Query:   "Jane",
			Context: "*",
		})

		if expected := 1; len(sx.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.GetCalls)
		}

		if expected := 3; len(sy.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.GetCalls)
		}
	})

	t.Run("with many sources with many contexts which overlap FIND", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnContexts: []string{
				"test1",
				"test2",
				"test3",
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnContexts: []string{
				"test2",
				"test3",
				"test4",
			},
		}

		e.AddSources(&sx)
		e.AddSources(&sy)

		e.ItemRequestHandler(&sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_FIND,
			Query:   "Jane",
			Context: "*",
		})

		if expected := 3; len(sx.FindCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.FindCalls)
		}

		if expected := 3; len(sy.FindCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.FindCalls)
		}
	})

	t.Run("with a single wildcard source", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnContexts: []string{
				sdp.WILDCARD,
			},
		}

		e.AddSources(&sx)

		e.ItemRequestHandler(&sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_FIND,
			Query:   "Rachel",
			Context: "*",
		})

		if expected := 1; len(sx.FindCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.FindCalls)
		}
	})

	t.Run("with a many wildcard sources", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnContexts: []string{
				sdp.WILDCARD,
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnContexts: []string{
				sdp.WILDCARD,
			},
		}

		sz := TestSource{
			ReturnName: "sz",
			ReturnContexts: []string{
				sdp.WILDCARD,
			},
		}

		e.AddSources(&sx)
		e.AddSources(&sy)
		e.AddSources(&sz)

		e.ItemRequestHandler(&sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_FIND,
			Query:   "Ross",
			Context: "*",
		})

		if expected := 1; len(sx.FindCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.FindCalls)
		}

		if expected := 1; len(sy.FindCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.FindCalls)
		}

		if expected := 1; len(sz.FindCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sz.FindCalls)
		}
	})

	t.Run("with a many wildcard sources and static sources", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnContexts: []string{
				sdp.WILDCARD,
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnContexts: []string{
				"test1",
			},
		}

		sz := TestSource{
			ReturnName: "sz",
			ReturnContexts: []string{
				"test2",
				"test3",
			},
		}

		e.AddSources(&sx)
		e.AddSources(&sy)
		e.AddSources(&sz)

		e.ItemRequestHandler(&sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_FIND,
			Query:   "Ross",
			Context: "*",
		})

		if expected := 1; len(sx.FindCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.FindCalls)
		}

		if expected := 1; len(sy.FindCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.FindCalls)
		}

		if expected := 2; len(sz.FindCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sz.FindCalls)
		}
	})
}
