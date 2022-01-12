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
