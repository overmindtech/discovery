package discovery

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdp-go/auth"
)

func newStartedEngine(t *testing.T, name string, no *auth.NATSOptions, sources ...Source) *Engine {
	e, err := NewEngine()
	if err != nil {
		t.Fatalf("Error initializing Engine: %v", err)
	}
	e.Name = name
	if no != nil {
		e.NATSOptions = no
	} else {
		e.NATSOptions = &auth.NATSOptions{
			NumRetries:        5,
			RetryDelay:        time.Second,
			Servers:           NatsTestURLs,
			ConnectionName:    "test-connection",
			ConnectionTimeout: time.Second,
			MaxReconnects:     5,
			TokenClient:       GetTestOAuthTokenClient(t, "org_hdeUXbB55sMMvJLa"),
		}
	}
	e.NATSQueueName = "test"
	e.MaxParallelExecutions = 10

	e.AddSources(sources...)

	err = e.Start()
	if err != nil {
		t.Fatalf("Error starting Engine: %v", err)
	}

	t.Cleanup(func() {
		e.Stop()
	})

	return e
}

func TestDeleteQuery(t *testing.T) {
	one := &sdp.LinkedItemQuery{Query: &sdp.Query{
		Scope:  "one",
		Method: sdp.QueryMethod_LIST,
		Query:  "",
	}}
	two := &sdp.LinkedItemQuery{Query: &sdp.Query{
		Scope:  "two",
		Method: sdp.QueryMethod_SEARCH,
		Query:  "2",
	}}
	irs := []*sdp.LinkedItemQuery{
		one,
		two,
	}

	deleted := deleteQuery(irs, two)

	if len(deleted) > 1 {
		t.Errorf("Item not successfully deleted: %v", irs)
	}
}

func TestTrackQuery(t *testing.T) {
	t.Run("With normal query", func(t *testing.T) {
		t.Parallel()

		e := newStartedEngine(t, "TestTrackQuery_normal", nil)

		u := uuid.New()

		qt := QueryTracker{
			Engine: e,
			Query: &sdp.Query{
				Type:   "person",
				Method: sdp.QueryMethod_LIST,
				RecursionBehaviour: &sdp.Query_RecursionBehaviour{
					LinkDepth: 10,
				},
				UUID: u[:],
			},
		}

		e.TrackQuery(u, &qt)

		if got, err := e.GetTrackedQuery(u); err == nil {
			if got != &qt {
				t.Errorf("Got mismatched QueryTracker objects %v and %v", got, &qt)
			}
		} else {
			t.Error(err)
		}
	})

	t.Run("With many queries", func(t *testing.T) {
		t.Parallel()

		e := newStartedEngine(t, "TestTrackQuery_many", nil)

		var wg sync.WaitGroup

		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				u := uuid.New()

				qt := QueryTracker{
					Engine: e,
					Query: &sdp.Query{
						Type:   "person",
						Query:  fmt.Sprintf("person-%v", i),
						Method: sdp.QueryMethod_GET,
						RecursionBehaviour: &sdp.Query_RecursionBehaviour{
							LinkDepth: 10,
						},
						UUID: u[:],
					},
				}

				e.TrackQuery(u, &qt)
			}(i)
		}

		wg.Wait()

		if len(e.trackedQueries) != 1000 {
			t.Errorf("Expected 1000 tracked querys, got %v", len(e.trackedQueries))
		}
	})
}

func TestDeleteTrackedQuery(t *testing.T) {
	t.Parallel()
	e := newStartedEngine(t, "TestDeleteTrackedQuery", nil)

	var wg sync.WaitGroup

	// Add and delete many query in parallel
	for i := 1; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			u := uuid.New()

			qt := QueryTracker{
				Engine: e,
				Query: &sdp.Query{
					Type:   "person",
					Query:  fmt.Sprintf("person-%v", i),
					Method: sdp.QueryMethod_GET,
					RecursionBehaviour: &sdp.Query_RecursionBehaviour{
						LinkDepth: 10,
					},
					UUID: u[:],
				},
			}

			e.TrackQuery(u, &qt)
			wg.Add(1)
			go func(u uuid.UUID) {
				defer wg.Done()
				e.DeleteTrackedQuery(u)
			}(u)
		}(i)
	}

	wg.Wait()

	if len(e.trackedQueries) != 0 {
		t.Errorf("Expected 0 tracked querys, got %v", len(e.trackedQueries))
	}
}

func TestNats(t *testing.T) {
	SkipWithoutNats(t)

	e, err := NewEngine()
	if err != nil {
		t.Fatalf("Error initializing Engine: %v", err)
	}
	e.Name = "nats-test"
	e.NATSOptions = &auth.NATSOptions{
		NumRetries:        5,
		RetryDelay:        time.Second,
		Servers:           NatsTestURLs,
		ConnectionName:    "test-connection",
		ConnectionTimeout: time.Second,
		MaxReconnects:     5,
	}
	e.NATSQueueName = "test"
	e.MaxParallelExecutions = 10

	src := TestSource{}

	e.AddSources(
		&src,
		&TestSource{
			ReturnScopes: []string{
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

	t.Run("Restarting", func(t *testing.T) {
		err := e.Stop()

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

	t.Run("Handling a basic query", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
			e.ClearCache()
		})

		req := sdp.NewQueryProgress(&sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
			Query:  "basic",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 0,
			},
			Scope: "test",
		})

		_, _, err := req.Execute(context.Background(), e.natsConnection)

		if err != nil {
			t.Error(err)
		}

		if len(src.GetCalls) != 1 {
			t.Errorf("expected 1 get call, got %v: %v", len(src.GetCalls), src.GetCalls)
		}
	})

	t.Run("Handling a deeply linking query", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
			e.ClearCache()
		})

		req := sdp.NewQueryProgress(&sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
			Query:  "deeplink",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 10,
			},
			Scope: "test",
		})

		_, _, err := req.Execute(context.Background(), e.natsConnection)

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

	e, err := NewEngine()
	if err != nil {
		t.Fatalf("Error initializing Engine: %v", err)
	}
	e.Name = "nats-test"
	e.NATSOptions = &auth.NATSOptions{
		NumRetries:        5,
		RetryDelay:        time.Second,
		Servers:           NatsTestURLs,
		ConnectionName:    "test-connection",
		ConnectionTimeout: time.Second,
		MaxReconnects:     5,
	}
	e.NATSQueueName = "test"
	e.MaxParallelExecutions = 1

	src := SpeedTestSource{
		QueryDelay:   250 * time.Millisecond,
		ReturnType:   "person",
		ReturnScopes: []string{"test"},
	}

	e.AddSources(&src)

	t.Run("Starting", func(t *testing.T) {
		err := e.Start()

		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Cancelling querys", func(t *testing.T) {
		conn := e.natsConnection
		u := uuid.New()

		progress := sdp.NewQueryProgress(&sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
			Query:  "foo",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 100,
			},
			Scope: "*",
			UUID:  u[:],
		})

		items := make(chan *sdp.Item, 1000)
		errs := make(chan *sdp.QueryError, 1000)

		err := progress.Start(context.Background(), conn, items, errs)

		if err != nil {
			t.Error(err)
		}

		time.Sleep(1 * time.Second)

		conn.Publish(context.Background(), "cancel.all", &sdp.CancelQuery{
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
	t.Run("with a bad hostname", func(t *testing.T) {
		e, err := NewEngine()
		if err != nil {
			t.Fatalf("Error initializing Engine: %v", err)
		}
		e.Name = "nats-test"
		e.NATSOptions = &auth.NATSOptions{
			Servers:           []string{"nats://bad.server"},
			ConnectionName:    "test-disconnection",
			ConnectionTimeout: time.Second,
			MaxReconnects:     1,
		}
		e.NATSQueueName = "test"
		e.MaxParallelExecutions = 1

		err = e.Start()

		if err == nil {
			t.Error("expected error but got nil")
		}
	})

	t.Run("with a server that disconnects", func(t *testing.T) {
		// We are running a custom server here so that we can control its lifecycle
		opts := test.DefaultTestOptions
		// Need to change this to avoid port clashes in github actions
		opts.Port = 4111
		s := test.RunServer(&opts)

		if !s.ReadyForConnections(10 * time.Second) {
			t.Fatal("Could not start goroutine NATS server")
		}

		t.Cleanup(func() {
			if s != nil {
				s.Shutdown()
			}
		})

		e, err := NewEngine()
		if err != nil {
			t.Fatalf("Error initializing Engine: %v", err)
		}
		e.Name = "nats-test"
		e.NATSOptions = &auth.NATSOptions{
			NumRetries:        5,
			RetryDelay:        time.Second,
			Servers:           []string{"127.0.0.1:4111"},
			ConnectionName:    "test-disconnection",
			ConnectionTimeout: time.Second,
			MaxReconnects:     10,
			ReconnectWait:     time.Second,
			ReconnectJitter:   time.Second,
		}
		e.NATSQueueName = "test"
		e.MaxParallelExecutions = 1

		err = e.Start()

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
		s = test.RunServer(&opts)

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
		// We are running a custom server here so that we can control its lifecycle
		opts := test.DefaultTestOptions
		// Need to change this to avoid port clashes in github actions
		opts.Port = 4112

		e, err := NewEngine()
		if err != nil {
			t.Fatalf("Error initializing Engine: %v", err)
		}
		e.Name = "nats-test"
		e.NATSOptions = &auth.NATSOptions{
			NumRetries:        10,
			RetryDelay:        time.Second,
			Servers:           []string{"127.0.0.1:4112"},
			ConnectionName:    "test-disconnection",
			ConnectionTimeout: time.Second,
			MaxReconnects:     10,
			ReconnectWait:     time.Second,
			ReconnectJitter:   time.Second,
		}
		e.NATSQueueName = "test"
		e.MaxParallelExecutions = 1

		var s *server.Server

		go func() {
			// Start the server after a delay
			time.Sleep(2 * time.Second)

			// We are running a custom server here so that we can control its lifecycle
			s = test.RunServer(&opts)

			t.Cleanup(func() {
				if s != nil {
					s.Shutdown()
				}
			})
		}()

		err = e.Start()

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestNATSFailureRestart(t *testing.T) {
	restartTestOption := test.DefaultTestOptions
	restartTestOption.Port = 4113

	// We are running a custom server here so that we can control its lifecycle
	s := test.RunServer(&restartTestOption)

	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("Could not start goroutine NATS server")
	}

	e, err := NewEngine()
	if err != nil {
		t.Fatalf("Error initializing Engine: %v", err)
	}
	e.Name = "nats-test"
	e.NATSOptions = &auth.NATSOptions{
		NumRetries:        10,
		RetryDelay:        time.Second,
		Servers:           []string{"127.0.0.1:4113"},
		ConnectionName:    "test-disconnection",
		ConnectionTimeout: time.Second,
		MaxReconnects:     10,
		ReconnectWait:     100 * time.Millisecond,
		ReconnectJitter:   10 * time.Millisecond,
	}
	e.NATSQueueName = "test"
	e.MaxParallelExecutions = 1
	e.ConnectionWatchInterval = 1 * time.Second

	// Connect successfully
	err = e.Start()

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

	s = test.RunServer(&restartTestOption)
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("Could not start goroutine NATS server a second time")
	}

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

	e, err := NewEngine()
	if err != nil {
		t.Fatalf("Error initializing Engine: %v", err)
	}
	e.Name = "nats-test"
	e.NATSOptions = &auth.NATSOptions{
		NumRetries:        5,
		RetryDelay:        time.Second,
		Servers:           NatsTestURLs,
		ConnectionName:    "test-connection",
		ConnectionTimeout: time.Second,
		MaxReconnects:     5,
		TokenClient:       GetTestOAuthTokenClient(t, "org_hdeUXbB55sMMvJLa"),
	}
	e.NATSQueueName = "test"
	e.MaxParallelExecutions = 10

	src := TestSource{}

	e.AddSources(
		&src,
		&TestSource{
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		},
	)

	t.Run("Starting", func(t *testing.T) {
		err := e.Start()

		if err != nil {
			t.Fatal(err)
		}

		if len(e.subscriptions) != 4 {
			t.Errorf("Expected engine to have 4 subscriptions, got %v", len(e.subscriptions))
		}
	})

	t.Run("Handling a basic query", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
			e.ClearCache()
		})

		_, _, err := sdp.NewQueryProgress(&sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
			Query:  "basic",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 0,
			},
			Scope: "test",
		}).Execute(context.Background(), e.natsConnection)

		if err != nil {
			t.Error(err)
		}

		if len(src.GetCalls) != 1 {
			t.Errorf("expected 1 get call, got %v: %v", len(src.GetCalls), src.GetCalls)
		}
	})

	t.Run("Handling a deeply linking query", func(t *testing.T) {
		t.Cleanup(func() {
			src.ClearCalls()
			e.ClearCache()
		})

		_, _, err := sdp.NewQueryProgress(&sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
			Query:  "deeplink",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 10,
			},
			Scope: "test",
		}).Execute(context.Background(), e.natsConnection)

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

func TestSetupMaxQueryTimeout(t *testing.T) {
	t.Run("with no value", func(t *testing.T) {
		e, err := NewEngine()
		if err != nil {
			t.Fatalf("Error initializing Engine: %v", err)
		}

		if e.MaxRequestTimeout != DefaultMaxRequestTimeout {
			t.Errorf("max request timeout did not default. Got %v expected %v", e.MaxRequestTimeout.String(), DefaultMaxRequestTimeout.String())
		}
	})

	t.Run("with a value", func(t *testing.T) {
		e, err := NewEngine()
		if err != nil {
			t.Fatalf("Error initializing Engine: %v", err)
		}
		e.MaxRequestTimeout = 1 * time.Second

		if e.MaxRequestTimeout != 1*time.Second {
			t.Errorf("max request timeout did not take provided value. Got %v expected %v", e.MaxRequestTimeout.String(), (1 * time.Second).String())
		}
	})
}

func GetTestOAuthTokenClient(t *testing.T, account string) auth.TokenClient {
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

	ccc := auth.ClientCredentialsConfig{
		ClientID:     clientID,
		ClientSecret: clientSecret,
	}

	return auth.NewOAuthTokenClient(
		fmt.Sprintf("https://%v/oauth/token", domain),
		exchangeURL,
		account,
		ccc,
	)
}
