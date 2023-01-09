package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindtech/connect"
	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestExecuteRequest(t *testing.T) {
	e := Engine{
		Name: "test",
	}

	src := TestSource{
		ReturnType:   "person",
		ReturnScopes: []string{"test"},
	}

	e.AddSources(&src)

	t.Run("Basic happy-path Get request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "foo",
			Scope:           "test",
			LinkDepth:       3,
			ItemSubject:     "items",
			ResponseSubject: "responses",
		}

		items, errs, err := e.ExecuteRequestSync(context.Background(), request)

		if err != nil {
			t.Error(err)
		}

		for _, e := range errs {
			t.Error(e)
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

	t.Run("Wrong scope Get request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Query:     "foo",
			Scope:     "wrong",
			LinkDepth: 0,
		}

		_, errs, err := e.ExecuteRequestSync(context.Background(), request)

		if err == nil {
			t.Error("expected erro but got nil")
		}

		if len(errs) == 1 {
			if errs[0].ErrorType != sdp.ItemRequestError_NOSCOPE {
				t.Errorf("expected error type to be NOSCOPE, got %v", errs[0].ErrorType)
			}
		} else {
			t.Errorf("expected 1 error, got %v", len(errs))
		}

	})

	t.Run("Wrong type Get request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:      "house",
			Method:    sdp.RequestMethod_GET,
			Query:     "foo",
			Scope:     "test",
			LinkDepth: 0,
		}

		_, errs, err := e.ExecuteRequestSync(context.Background(), request)

		if err == nil {
			t.Error("expected erro but got nil")
		}

		if len(errs) == 1 {
			if errs[0].ErrorType != sdp.ItemRequestError_NOSCOPE {
				t.Errorf("expected error type to be NOSCOPE, got %v", errs[0].ErrorType)
			}
		} else {
			t.Errorf("expected 1 error, got %v", len(errs))
		}
	})

	t.Run("Basic List request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_LIST,
			Scope:     "test",
			LinkDepth: 5,
		}

		items, errs, err := e.ExecuteRequestSync(context.Background(), request)

		if err != nil {
			t.Error(err)
		}

		for _, e := range errs {
			t.Error(e)
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
			Scope:     "test",
			LinkDepth: 5,
		}

		items, errs, err := e.ExecuteRequestSync(context.Background(), request)

		if err != nil {
			t.Error(err)
		}

		for _, e := range errs {
			t.Error(e)
		}

		if len(items) < 1 {
			t.Error("expected at least one item")
		}
	})

}

func TestHandleItemRequest(t *testing.T) {
	e := Engine{
		Name: "test",
	}

	personSource := TestSource{
		ReturnType: "person",
		ReturnScopes: []string{
			"test1",
			"test2",
		},
	}

	dogSource := TestSource{
		ReturnType: "dog",
		ReturnScopes: []string{
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
			Scope:     "test1",
			LinkDepth: 0,
		}

		// Run the handler
		e.HandleItemRequest(context.Background(), &req)

		// I'm expecting both sources to get a request since the type was *
		if l := len(personSource.GetCalls); l != 1 {
			t.Errorf("expected person backend to have 1 Get call, got %v", l)
		}

		if l := len(dogSource.GetCalls); l != 1 {
			t.Errorf("expected dog backend to have 1 Get call, got %v", l)
		}
	})

	t.Run("Wildcard scope should be expanded", func(t *testing.T) {
		t.Cleanup(func() {
			personSource.ClearCalls()
			dogSource.ClearCalls()
		})

		req := sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Query:     "Dylan1",
			Scope:     sdp.WILDCARD,
			LinkDepth: 0,
		}

		// Run the handler
		e.HandleItemRequest(context.Background(), &req)

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
		ReturnScopes: []string{
			sdp.WILDCARD,
		},
	}

	e.AddSources(&personSource)

	t.Run("request scope should be preserved", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Query:     "Dylan1",
			Scope:     "something.specific",
			LinkDepth: 0,
		}

		// Run the handler
		e.HandleItemRequest(context.Background(), &req)

		if len(personSource.GetCalls) != 1 {
			t.Errorf("expected 1 get call got %v", len(personSource.GetCalls))
		}

		call := personSource.GetCalls[0]

		if expected := "something.specific"; call[0] != expected {
			t.Errorf("expected scope to be %v, got %v", expected, call[0])
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
		NATSOptions: &connect.NATSOptions{
			Servers:           NatsTestURLs,
			ConnectionName:    "test-connection",
			ConnectionTimeout: time.Second,
			MaxReconnects:     5,
		},
		NATSQueueName:         "test",
		MaxParallelExecutions: 10,
	}

	src := TestSource{
		ReturnType: "person",
		ReturnScopes: []string{
			"test",
		},
	}

	e.AddSources(&src)

	err := e.Start()

	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 250; i++ {
		u := uuid.New()

		var progress *sdp.RequestProgress
		var items []*sdp.Item

		progress = sdp.NewRequestProgress(&sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "Dylan",
			Scope:           "test",
			LinkDepth:       0,
			IgnoreCache:     false,
			UUID:            u[:],
			Timeout:         durationpb.New(10 * time.Minute),
			ItemSubject:     NewItemSubject(),
			ResponseSubject: NewResponseSubject(),
		})

		items, errs, err := progress.Execute(context.Background(), e.natsConnection)

		if err != nil {
			t.Fatal(err)
		}

		if len(errs) != 0 {
			for _, err := range errs {
				t.Error(err)
			}
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

	t.Run("with a single source with a single scope", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		simple := TestSource{
			ReturnScopes: []string{
				"test1",
			},
		}

		e.AddSources(&simple)

		e.HandleItemRequest(context.Background(), &sdp.ItemRequest{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
			Query:  "Debby",
			Scope:  "*",
		})

		if expected := 1; len(simple.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, len(simple.GetCalls))
		}
	})

	t.Run("with a single source with many scopes", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		many := TestSource{
			ReturnName: "many",
			ReturnScopes: []string{
				"test1",
				"test2",
				"test3",
			},
		}

		e.AddSources(&many)

		e.HandleItemRequest(context.Background(), &sdp.ItemRequest{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
			Query:  "Debby",
			Scope:  "*",
		})

		if expected := 3; len(many.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, many.GetCalls)
		}
	})

	t.Run("with many sources with single scopes", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test2",
			},
		}

		e.AddSources(&sx)
		e.AddSources(&sy)

		e.HandleItemRequest(context.Background(), &sdp.ItemRequest{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
			Query:  "Daniel",
			Scope:  "*",
		})

		if expected := 1; len(sx.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.GetCalls)
		}

		if expected := 1; len(sy.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.GetCalls)
		}
	})

	t.Run("with many sources with many scopes", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
				"test2",
				"test3",
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test4",
				"test5",
				"test6",
			},
		}

		e.AddSources(&sx)
		e.AddSources(&sy)

		e.HandleItemRequest(context.Background(), &sdp.ItemRequest{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
			Query:  "Steven",
			Scope:  "*",
		})

		if expected := 3; len(sx.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.GetCalls)
		}

		if expected := 3; len(sy.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.GetCalls)
		}
	})

	t.Run("with many sources with many scopes which overlap GET", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
				"test2",
				"test3",
			},
			ReturnWeight: 10,
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test2",
				"test3",
				"test4",
			},
			ReturnWeight: 11,
		}

		e.AddSources(&sx)
		e.AddSources(&sy)

		e.HandleItemRequest(context.Background(), &sdp.ItemRequest{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
			Query:  "Jane",
			Scope:  "*",
		})

		if expected := 1; len(sx.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.GetCalls)
		}

		if expected := 3; len(sy.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.GetCalls)
		}
	})

	t.Run("with many sources with many scopes which overlap LIST", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
				"test2",
				"test3",
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test2",
				"test3",
				"test4",
			},
		}

		e.AddSources(&sx)
		e.AddSources(&sy)

		e.HandleItemRequest(context.Background(), &sdp.ItemRequest{
			Type:   "person",
			Method: sdp.RequestMethod_LIST,
			Query:  "Jane",
			Scope:  "*",
		})

		if expected := 3; len(sx.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.ListCalls)
		}

		if expected := 3; len(sy.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.ListCalls)
		}
	})

	t.Run("with a single wildcard source", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		e.AddSources(&sx)

		e.HandleItemRequest(context.Background(), &sdp.ItemRequest{
			Type:   "person",
			Method: sdp.RequestMethod_LIST,
			Query:  "Rachel",
			Scope:  "*",
		})

		if expected := 1; len(sx.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.ListCalls)
		}
	})

	t.Run("with a many wildcard sources", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		sz := TestSource{
			ReturnName: "sz",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		e.AddSources(&sx)
		e.AddSources(&sy)
		e.AddSources(&sz)

		e.HandleItemRequest(context.Background(), &sdp.ItemRequest{
			Type:   "person",
			Method: sdp.RequestMethod_LIST,
			Query:  "Ross",
			Scope:  "*",
		})

		if expected := 1; len(sx.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.ListCalls)
		}

		if expected := 1; len(sy.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.ListCalls)
		}

		if expected := 1; len(sz.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sz.ListCalls)
		}
	})

	t.Run("with a many wildcard sources and static sources", func(t *testing.T) {
		t.Cleanup(func() {
			e.sourceMap = nil
			e.ClearCache()
		})

		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test1",
			},
		}

		sz := TestSource{
			ReturnName: "sz",
			ReturnScopes: []string{
				"test2",
				"test3",
			},
		}

		e.AddSources(&sx)
		e.AddSources(&sy)
		e.AddSources(&sz)

		e.HandleItemRequest(context.Background(), &sdp.ItemRequest{
			Type:   "person",
			Method: sdp.RequestMethod_LIST,
			Query:  "Ross",
			Scope:  "*",
		})

		if expected := 1; len(sx.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.ListCalls)
		}

		if expected := 1; len(sy.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.ListCalls)
		}

		if expected := 2; len(sz.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sz.ListCalls)
		}
	})
}
