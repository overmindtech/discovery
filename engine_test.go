package discovery

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/overmindtech/multiconn"
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
		NATSOptions: &multiconn.NATSConnectionOptions{
			CommonOptions: multiconn.CommonOptions{
				NumRetries: 5,
				RetryDelay: time.Second,
			},
			Servers:           NatsTestURLs,
			ConnectionName:    "test-connection",
			ConnectionTimeout: time.Second,
			MaxReconnects:     5,
		},
		NATSQueueName:         "test",
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
		err := e.Start()

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
			e.ClearCache()
		})

		req := sdp.NewRequestProgress(&sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "basic",
			LinkDepth:       0,
			Context:         "test",
			ResponseSubject: NewResponseSubject(),
			ItemSubject:     NewItemSubject(),
		})

		_, _, err := req.Execute(e.natsConnection)

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
			e.ClearCache()
		})

		req := sdp.NewRequestProgress(&sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "deeplink",
			LinkDepth:       10,
			Context:         "test",
			ResponseSubject: NewResponseSubject(),
			ItemSubject:     NewItemSubject(),
		})

		_, _, err := req.Execute(e.natsConnection)

		if err != nil {
			t.Error(err)
		}

		if len(src.GetCalls) != 11 {
			t.Errorf("expected 11 get calls, got %v: %v", len(src.GetCalls), src.GetCalls)
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
		NATSOptions: &multiconn.NATSConnectionOptions{
			CommonOptions: multiconn.CommonOptions{
				NumRetries: 5,
				RetryDelay: time.Second,
			},
			Servers:           NatsTestURLs,
			ConnectionName:    "test-connection",
			ConnectionTimeout: time.Second,
			MaxReconnects:     5,
		},
		NATSQueueName:         "test",
		MaxParallelExecutions: 1,
	}

	src := SpeedTestSource{
		QueryDelay:     250 * time.Millisecond,
		ReturnType:     "person",
		ReturnContexts: []string{"test"},
	}

	e.AddSources(&src)

	t.Run("Starting", func(t *testing.T) {
		err := e.Start()

		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Cancelling requests", func(t *testing.T) {
		conn := e.natsConnection
		u := uuid.New()

		progress := sdp.NewRequestProgress(&sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "foo",
			LinkDepth:       100,
			Context:         "*",
			ResponseSubject: nats.NewInbox(),
			ItemSubject:     "items.bin",
			UUID:            u[:],
		})

		items := make(chan *sdp.Item, 1000)
		errs := make(chan *sdp.ItemRequestError, 1000)

		err := progress.Start(conn, items, errs)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(1 * time.Second)

		conn.Publish("cancel.all", &sdp.CancelItemRequest{
			UUID: u[:],
		})

		// Read and discard all items and errors until they are closed
		for range items {
		}
		for range errs {
		}

		if progress.NumCancelled() != 1 {
			t.Errorf("Expected query to be cancelled, got\n%v", progress.String())
		}
	})

	t.Run("stopping", func(t *testing.T) {
		err := e.Stop()

		if err != nil {
			t.Error(err)
		}
	})
}

func TestNatsConnections(t *testing.T) {
	// Need to change this to avoid port clashes in github actions
	test.DefaultTestOptions.Port = 4111

	t.Run("with a bad hostname", func(t *testing.T) {
		e := Engine{
			Name: "nats-test",
			NATSOptions: &multiconn.NATSConnectionOptions{
				Servers:           []string{"nats://bad.server"},
				ConnectionName:    "test-disconnection",
				ConnectionTimeout: time.Second,
				MaxReconnects:     1,
			},
			NATSQueueName:         "test",
			MaxParallelExecutions: 1,
		}

		err := e.Start()

		if err == nil {
			t.Error("expected error but got nil")
		}
	})

	t.Run("with a server that disconnects", func(t *testing.T) {
		// We are running a custom server here so that we can control its lifecycle
		s := test.RunServer(&test.DefaultTestOptions)

		if !s.ReadyForConnections(10 * time.Second) {
			t.Fatal("Could not start goroutine NATS server")
		}

		t.Cleanup(func() {
			if s != nil {
				s.Shutdown()
			}
		})

		e := Engine{
			Name: "nats-test",
			NATSOptions: &multiconn.NATSConnectionOptions{
				CommonOptions: multiconn.CommonOptions{
					NumRetries: 5,
					RetryDelay: time.Second,
				},
				Servers:           []string{"127.0.0.1:4111"},
				ConnectionName:    "test-disconnection",
				ConnectionTimeout: time.Second,
				MaxReconnects:     10,
				ReconnectWait:     time.Second,
				ReconnectJitter:   time.Second,
			},
			NATSQueueName:         "test",
			MaxParallelExecutions: 1,
		}

		err := e.Start()

		if err != nil {
			t.Fatal(err)
		}

		t.Log("Stopping NATS server")
		s.Shutdown()

		for i := 0; i <= 20; i++ {
			if i == 20 {
				t.Errorf("Engine did not report a NATS disconnect after %v tries", i)
			}

			if !e.IsNATSConnected() {
				break
			}

			time.Sleep(time.Second)
		}

		// Reset the server
		s = test.RunServer(&test.DefaultTestOptions)

		// Wait for the server to start
		s.ReadyForConnections(10 * time.Second)

		// Wait 2 more seconds for a reconnect
		time.Sleep(2 * time.Second)

		for i := 0; i <= 20; i++ {
			if e.IsNATSConnected() {
				return
			}

			time.Sleep(time.Second)
		}

		t.Error("Engine should have reconnected but hasn't")
	})

	t.Run("with a server that takes a while to start", func(t *testing.T) {
		e := Engine{
			Name: "nats-test",
			NATSOptions: &multiconn.NATSConnectionOptions{
				CommonOptions: multiconn.CommonOptions{
					NumRetries: 10,
					RetryDelay: time.Second,
				},
				Servers:           []string{"127.0.0.1:4111"},
				ConnectionName:    "test-disconnection",
				ConnectionTimeout: time.Second,
				MaxReconnects:     10,
				ReconnectWait:     time.Second,
				ReconnectJitter:   time.Second,
			},
			NATSQueueName:         "test",
			MaxParallelExecutions: 1,
		}

		var s *server.Server

		go func() {
			// Start the server after a delay
			time.Sleep(2 * time.Second)

			// We are running a custom server here so that we can control its lifecycle
			s = test.RunServer(&test.DefaultTestOptions)

			t.Cleanup(func() {
				if s != nil {
					s.Shutdown()
				}
			})
		}()

		err := e.Start()

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestNATSFailureRestart(t *testing.T) {
	test.DefaultTestOptions.Port = 4112

	// We are running a custom server here so that we can control its lifecycle
	s := test.RunServer(&test.DefaultTestOptions)

	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("Could not start goroutine NATS server")
	}

	e := Engine{
		Name: "nats-test",
		NATSOptions: &multiconn.NATSConnectionOptions{
			CommonOptions: multiconn.CommonOptions{
				NumRetries: 10,
				RetryDelay: time.Second,
			},
			Servers:           []string{"127.0.0.1:4112"},
			ConnectionName:    "test-disconnection",
			ConnectionTimeout: time.Second,
			MaxReconnects:     10,
			ReconnectWait:     100 * time.Millisecond,
			ReconnectJitter:   10 * time.Millisecond,
		},
		NATSQueueName:           "test",
		MaxParallelExecutions:   1,
		ConnectionWatchInterval: 1 * time.Second,
	}

	// Connect successfully
	err := e.Start()

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		e.Stop()
	})

	// Lose the connection
	t.Log("Stopping NATS server")
	s.Shutdown()
	s.WaitForShutdown()

	// The watcher should keep watching while the nats connection is
	// RECONNECTING, once it's CLOSED however it won't keep trying to connect so
	// we want to make sure that the watcher detects this and kills the whole
	// thing
	time.Sleep(2 * time.Second)

	s = test.RunServer(&test.DefaultTestOptions)
	s.ReadyForConnections(10 * time.Second)

	t.Cleanup(func() {
		s.Shutdown()
	})

	time.Sleep(3 * time.Second)

	if !e.IsNATSConnected() {
		t.Error("NATS didn't manage to reconnect")
	}
}

func TestNatsAuth(t *testing.T) {
	SkipWithoutNatsAuth(t)

	e := Engine{
		Name: "nats-test",
		NATSOptions: &multiconn.NATSConnectionOptions{
			CommonOptions: multiconn.CommonOptions{
				NumRetries: 5,
				RetryDelay: time.Second,
			},
			Servers:           NatsTestURLs,
			ConnectionName:    "test-connection",
			ConnectionTimeout: time.Second,
			MaxReconnects:     5,
			TokenClient:       GetTestOAuthTokenClient(t),
		},
		NATSQueueName:         "test",
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
		err := e.Start()

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
			e.ClearCache()
		})

		_, _, err := sdp.NewRequestProgress(&sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "basic",
			LinkDepth:       0,
			Context:         "test",
			ResponseSubject: NewResponseSubject(),
			ItemSubject:     NewItemSubject(),
		}).Execute(e.natsConnection)

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
			e.ClearCache()
		})

		_, _, err := sdp.NewRequestProgress(&sdp.ItemRequest{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "deeplink",
			LinkDepth:       10,
			Context:         "test",
			ResponseSubject: NewResponseSubject(),
			ItemSubject:     NewItemSubject(),
		}).Execute(e.natsConnection)

		if err != nil {
			t.Error(err)
		}

		if len(src.GetCalls) != 11 {
			t.Errorf("expected 11 get calls, got %v: %v", len(src.GetCalls), src.GetCalls)
		}
	})

	t.Run("stopping", func(t *testing.T) {
		err := e.Stop()

		if err != nil {
			t.Error(err)
		}
	})

}

func TestSetupMaxRequestTimeout(t *testing.T) {
	t.Run("with no value", func(t *testing.T) {
		e := Engine{}

		e.SetupMaxRequestTimeout()

		if e.MaxRequestTimeout != DefaultMaxRequestTimeout {
			t.Errorf("max request timeout did not default. Got %v expected %v", e.MaxRequestTimeout.String(), DefaultMaxRequestTimeout.String())
		}
	})

	t.Run("with a value", func(t *testing.T) {
		e := Engine{
			MaxRequestTimeout: 1 * time.Second,
		}

		e.SetupMaxRequestTimeout()

		if e.MaxRequestTimeout != 1*time.Second {
			t.Errorf("max request timeout did not take provided value. Got %v expected %v", e.MaxRequestTimeout.String(), (1 * time.Second).String())
		}
	})
}

func GetTestOAuthTokenClient(t *testing.T) *multiconn.OAuthTokenClient {
	var domain string
	var clientID string
	var clientSecret string
	var exists bool

	errorFormat := "environment variable %v not found. Set up your test environment first. See: https://github.com/overmindtech/auth0-test-data"

	// Read secrets form the environment
	if domain, exists = os.LookupEnv("OVERMIND_NTE_ALLPERMS_DOMAIN"); !exists || domain == "" {
		t.Errorf(errorFormat, "OVERMIND_NTE_ALLPERMS_DOMAIN")
		t.Skip("Skipping due to missing environment setup")
	}

	if clientID, exists = os.LookupEnv("OVERMIND_NTE_ALLPERMS_CLIENT_ID"); !exists || clientID == "" {
		t.Errorf(errorFormat, "OVERMIND_NTE_ALLPERMS_CLIENT_ID")
		t.Skip("Skipping due to missing environment setup")
	}

	if clientSecret, exists = os.LookupEnv("OVERMIND_NTE_ALLPERMS_CLIENT_SECRET"); !exists || clientSecret == "" {
		t.Errorf(errorFormat, "OVERMIND_NTE_ALLPERMS_CLIENT_SECRET")
		t.Skip("Skipping due to missing environment setup")
	}

	exchangeURL, err := GetWorkingTokenExchange()

	if err != nil {
		t.Fatal(err)
	}

	return multiconn.NewOAuthTokenClient(
		clientID,
		clientSecret,
		fmt.Sprintf("https://%v/oauth/token", domain),
		exchangeURL,
	)
}
