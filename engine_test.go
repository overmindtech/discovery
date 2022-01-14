package discovery

import (
	"fmt"
	"sync"
	"testing"

	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/overmindtech/sdp-go"
)

func TestDeleteItemRequest(t *testing.T) {
	one := &sdp.ItemRequest{
		Context: "one",
		Method:  sdp.RequestMethod_FIND,
		Query:   "",
	}
	two := &sdp.ItemRequest{
		Context: "two",
		Method:  sdp.RequestMethod_SEARCH,
		Query:   "2",
	}
	irs := []*sdp.ItemRequest{
		one,
		two,
	}

	deleted := deleteItemRequest(irs, two)

	if len(deleted) > 1 {
		t.Errorf("Item not successfully deleted: %v", irs)
	}
}

func TestTrackRequest(t *testing.T) {
	e := Engine{
		Name: "test",
	}

	t.Run("With normal request", func(t *testing.T) {
		t.Parallel()

		u := uuid.New()

		rt := RequestTracker{
			Engine: &e,
			Request: &sdp.ItemRequest{
				Type:      "person",
				Method:    sdp.RequestMethod_FIND,
				LinkDepth: 10,
				UUID:      u[:],
			},
		}

		e.TrackRequest(u, &rt)

		if got, err := e.GetTrackedRequest(u); err == nil {
			if got != &rt {
				t.Errorf("Got mismatched RequestTracker objects %v and %v", got, &rt)
			}
		} else {
			t.Error(err)
		}
	})

	t.Run("With many requests", func(t *testing.T) {
		t.Parallel()

		var wg sync.WaitGroup

		for i := 1; i < 1000; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				u := uuid.New()

				rt := RequestTracker{
					Engine: &e,
					Request: &sdp.ItemRequest{
						Type:      "person",
						Query:     fmt.Sprintf("person-%v", i),
						Method:    sdp.RequestMethod_GET,
						LinkDepth: 10,
						UUID:      u[:],
					},
				}

				e.TrackRequest(u, &rt)
			}(i)
		}

		wg.Wait()

		if len(e.trackedRequests) != 1000 {
			t.Errorf("Expected 1000 tracked requests, got %v", len(e.trackedRequests))
		}
	})
}

func TestDeleteTrackedRequest(t *testing.T) {
	t.Parallel()

	e := Engine{
		Name: "test",
	}

	var wg sync.WaitGroup

	// Add and delete many request in parallel
	for i := 1; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			u := uuid.New()

			rt := RequestTracker{
				Engine: &e,
				Request: &sdp.ItemRequest{
					Type:      "person",
					Query:     fmt.Sprintf("person-%v", i),
					Method:    sdp.RequestMethod_GET,
					LinkDepth: 10,
					UUID:      u[:],
				},
			}

			e.TrackRequest(u, &rt)
			wg.Add(1)
			go func(u uuid.UUID) {
				defer wg.Done()
				e.DeleteTrackedRequest(u)
			}(u)
		}(i)
	}

	wg.Wait()

	if len(e.trackedRequests) != 0 {
		t.Errorf("Expected 0 tracked requests, got %v", len(e.trackedRequests))
	}
}

func TestNats(t *testing.T) {
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

	src := TestSource{}

	e.AddSources(
		&src,
		&TestSource{
			ReturnContexts: []string{
				sdp.WILDCARD,
			},
		},
	)

	t.Run("Starting", func(t *testing.T) {
		err := e.Connect()

		if err != nil {
			t.Error(err)
		}

		err = e.Start()

		if err != nil {
			t.Error(err)
		}

		if len(e.subscriptions) != 4 {
			t.Errorf("Expected engine to have 4 subscriptions, got %v", len(e.subscriptions))
		}
	})

	t.Run("Handling a basic request", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		req := sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "dylan",
			LinkDepth:       0,
			Context:         "test",
			ResponseSubject: NewResponseSubject(),
			ItemSubject:     NewItemSubject(),
		}

		_, _, err := e.SendRequestSync(&req)

		if err != nil {
			t.Error(err)
		}

		if len(src.GetCalls) != 1 {
			t.Errorf("expected 1 get call, got %v: %v", len(src.GetCalls), src.GetCalls)
		}
	})

	t.Run("Handling a deeply linking request", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
		})

		req := sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "dylan",
			LinkDepth:       10,
			Context:         "test",
			ResponseSubject: NewResponseSubject(),
			ItemSubject:     NewItemSubject(),
		}

		_, _, err := e.SendRequestSync(&req)

		if err != nil {
			t.Error(err)
		}

		if len(src.GetCalls) != 10 {
			t.Errorf("expected 10 get calls, got %v: %v", len(src.GetCalls), src.GetCalls)
		}
	})

	t.Run("stopping", func(t *testing.T) {
		err := e.Stop()

		if err != nil {
			t.Error(err)
		}
	})
}

func TestNatsCancel(t *testing.T) {
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
		MaxParallelExecutions: 1,
	}

	src := SpeedTestSource{
		QueryDelay:     250 * time.Millisecond,
		ReturnType:     "person",
		ReturnContexts: []string{"test"},
	}

	e.AddSources(&src)

	t.Run("Starting", func(t *testing.T) {
		err := e.Connect()

		if err != nil {
			t.Error(err)
		}

		err = e.Start()

		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Cancelling requests", func(t *testing.T) {
		conn := e.natsConnection
		u := uuid.New()

		req := sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "foo",
			LinkDepth:       100,
			Context:         "test",
			ResponseSubject: nats.NewInbox(),
			ItemSubject:     "items.bin",
			UUID:            u[:],
		}

		progress := sdp.NewRequestProgress()

		conn.Subscribe(req.ResponseSubject, progress.ProcessResponse)

		err := conn.Publish("request.all", &req)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(1 * time.Second)

		conn.Publish("cancel.all", &sdp.CancelItemRequest{
			UUID: u[:],
		})

		<-progress.Done()

		if progress.NumCancelled() != 1 {
			t.Errorf("Expected query to be cancelled, got\n%v", progress.String())
		}
	})

	t.Run("Cancelling requests without a UUID", func(t *testing.T) {
		conn := e.natsConnection

		req := sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "bad-uuid",
			LinkDepth:       4,
			Context:         "test",
			ResponseSubject: nats.NewInbox(),
			ItemSubject:     "items.bin",
		}

		progress := sdp.NewRequestProgress()

		conn.Subscribe(req.ResponseSubject, progress.ProcessResponse)

		err := conn.Publish("request.all", &req)

		if err != nil {
			t.Error(err)
		}

		conn.Publish("cancel.all", &sdp.CancelItemRequest{
			UUID: []byte{},
		})

		<-progress.Done()

		// You shouldn't be able to cancel requests that don't have a UUID
		if progress.NumCancelled() != 0 {
			t.Errorf("Expected query to not cancelled, got\n%v", progress.String())
		}
	})

	t.Run("stopping", func(t *testing.T) {
		err := e.Stop()

		if err != nil {
			t.Error(err)
		}
	})
}
