package discovery

import (
	"fmt"
	"net"
	"sync"
	"testing"

	"time"

	"github.com/google/uuid"
	"github.com/overmindtech/sdp-go"
)

const NatsHost = "nats"
const NatsPort = "4222"

type TestBackend struct{}

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
			URLs: []string{
				"nats://nats:4222",
			},
			ConnectionName: "test-connection",
			ConnectTimeout: time.Second,
			NumRetries:     5,
			QueueName:      "test",
		},
		MaxParallelExecutions: 10,
	}

	src := TestSource{}

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
}

// SKipWithoutNats Skips a test if NATS is not available
func SkipWithoutNats(t *testing.T) {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(NatsHost, NatsPort), time.Second)
	if err != nil {
		t.Skip("NATS not available, skipping")
	}
	if conn != nil {
		conn.Close()
	}
}
