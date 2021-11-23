package discovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

type SpeedTestSource struct {
	QueryDelay     time.Duration
	ReturnType     string
	ReturnContexts []string
}

func (s *SpeedTestSource) Type() string {
	if s.ReturnType != "" {
		return s.ReturnType
	}

	return "person"
}

func (s *SpeedTestSource) Name() string {
	return "SpeedTestSource"
}

func (s *SpeedTestSource) Contexts() []string {
	if len(s.ReturnContexts) > 0 {
		return s.ReturnContexts
	}

	return []string{"test"}
}

func (s *SpeedTestSource) Get(ctx context.Context, itemContext string, query string) (*sdp.Item, error) {
	select {
	case <-time.After(s.QueryDelay):
		return &sdp.Item{
			Type:            s.Type(),
			UniqueAttribute: "name",
			Attributes: &sdp.ItemAttributes{
				AttrStruct: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"name": {
							Kind: &structpb.Value_StringValue{
								StringValue: query,
							},
						},
					},
				},
			},
			LinkedItemRequests: []*sdp.ItemRequest{
				{
					Type:    "person",
					Method:  sdp.RequestMethod_GET,
					Query:   query + time.Now().String(),
					Context: itemContext,
				},
			},
			Context: itemContext,
		}, nil
	case <-ctx.Done():
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_TIMEOUT,
			ErrorString: ctx.Err().Error(),
			Context:     itemContext,
		}
	}

}

func (s *SpeedTestSource) Find(ctx context.Context, itemContext string) ([]*sdp.Item, error) {
	item, err := s.Get(ctx, itemContext, "dylan")

	return []*sdp.Item{item}, err
}

func (s *SpeedTestSource) Weight() int {
	return 10
}

func TestExecuteParallel(t *testing.T) {
	queryDelay := (200 * time.Millisecond)
	numSources := 10
	sources := make([]Source, numSources)

	// Create a number of sources
	for i := 0; i < len(sources); i++ {
		sources[i] = &SpeedTestSource{
			QueryDelay: queryDelay,
			ReturnType: fmt.Sprintf("type%v", i),
		}
	}

	t.Run("With no parallelism", func(t *testing.T) {
		t.Parallel()

		engine := Engine{
			Name:                  "no-parallel",
			MaxParallelExecutions: 1,
		}

		engine.AddSources(sources...)
		engine.SetupThrottle()

		tracker := RequestTracker{
			Engine: &engine,
			Request: &sdp.ItemRequest{
				Type:      "*",
				Method:    sdp.RequestMethod_FIND,
				LinkDepth: 0,
				Context:   "*",
			},
		}

		timeStart := time.Now()

		_, err := tracker.Execute()

		timeTaken := time.Since(timeStart)

		if err != nil {
			t.Fatal(err)
		}

		expectedTime := time.Duration(int64(queryDelay) * int64(numSources))

		if timeTaken < expectedTime {
			t.Errorf("Query with no parallelism took < %v. This means it must have run in parallel", expectedTime)
		}
	})

	t.Run("With lots of parallelism", func(t *testing.T) {
		t.Parallel()

		engine := Engine{
			Name:                  "no-parallel",
			MaxParallelExecutions: 999,
		}

		engine.AddSources(sources...)
		engine.SetupThrottle()

		tracker := RequestTracker{
			Engine: &engine,
			Request: &sdp.ItemRequest{
				Type:      "*",
				Method:    sdp.RequestMethod_FIND,
				LinkDepth: 0,
				Context:   "*",
			},
		}

		timeStart := time.Now()

		_, err := tracker.Execute()

		timeTaken := time.Since(timeStart)

		if err != nil {
			t.Fatal(err)
		}

		expectedTime := (queryDelay * 2) // Double it give us some wiggle room

		if timeTaken > expectedTime {
			t.Errorf("Query with no parallelism took %v which is > than the expected max of %v. This means it must not have run in parallel", timeTaken, expectedTime)
		}
	})
}

func TestExecute(t *testing.T) {
	engine := Engine{
		Name:                  "test",
		MaxParallelExecutions: 1,
	}

	src := TestSource{
		ReturnType: "person",
		ReturnContexts: []string{
			"test",
		},
	}

	engine.AddSources(&src)

	t.Run("Without linking", func(t *testing.T) {
		t.Parallel()

		rt := RequestTracker{
			Engine: &engine,
			Request: &sdp.ItemRequest{
				Type:      "person",
				Method:    sdp.RequestMethod_GET,
				Query:     "Dylan",
				LinkDepth: 0,
				Context:   "test",
			},
		}

		items, err := rt.Execute()

		if err != nil {
			t.Error(err)
		}

		if l := len(items); l != 1 {
			t.Errorf("expected 1 items, got %v", l)
		}
	})

	t.Run("With linking", func(t *testing.T) {
		t.Parallel()

		rt := RequestTracker{
			Engine: &engine,
			Request: &sdp.ItemRequest{
				Type:      "person",
				Method:    sdp.RequestMethod_GET,
				Query:     "Dylan",
				LinkDepth: 10,
				Context:   "test",
			},
		}

		items, err := rt.Execute()

		if err != nil {
			t.Error(err)
		}

		if l := len(items); l != 11 {
			t.Errorf("expected 10 items, got %v", l)
		}
	})

	t.Run("With no requests", func(t *testing.T) {
		t.Parallel()

		rt := RequestTracker{
			Engine: &engine,
		}

		_, err := rt.Execute()

		if err != nil {
			t.Error(err)
		}
	})

}

func TestTimeout(t *testing.T) {
	engine := Engine{
		Name:                  "test",
		MaxParallelExecutions: 1,
	}

	src := SpeedTestSource{
		QueryDelay: 100 * time.Millisecond,
	}

	engine.AddSources(&src)

	t.Run("With a timeout, but not exceeding it", func(t *testing.T) {
		t.Parallel()

		rt := RequestTracker{
			Engine: &engine,
			Request: &sdp.ItemRequest{
				Type:      "person",
				Method:    sdp.RequestMethod_GET,
				Query:     "Dylan",
				LinkDepth: 0,
				Context:   "test",
				Timeout:   durationpb.New(200 * time.Millisecond),
			},
		}

		items, err := rt.Execute()

		if err != nil {
			t.Error(err)
		}

		if l := len(items); l != 1 {
			t.Errorf("expected 1 items, got %v", l)
		}
	})

	t.Run("With a timeout that is exceeded", func(t *testing.T) {
		t.Parallel()

		rt := RequestTracker{
			Engine: &engine,
			Request: &sdp.ItemRequest{
				Type:      "person",
				Method:    sdp.RequestMethod_GET,
				Query:     "somethingElse",
				LinkDepth: 0,
				Context:   "test",
				Timeout:   durationpb.New(50 * time.Millisecond),
			},
		}

		_, err := rt.Execute()

		if err == nil {
			t.Error("Expected timout but got no error")
		}
	})

	t.Run("With linking that exceeds the timout", func(t *testing.T) {
		rt := RequestTracker{
			Engine: &engine,
			Request: &sdp.ItemRequest{
				Type:      "person",
				Method:    sdp.RequestMethod_GET,
				Query:     "somethingElse1",
				LinkDepth: 10,
				Context:   "test",
				Timeout:   durationpb.New(350 * time.Millisecond),
			},
		}

		items, err := rt.Execute()

		if err == nil {
			t.Error("Expected timeout but got no error")
		}

		if len(items) != 3 {
			t.Errorf("Expected 3 items, got %v", len(items))
		}
	})
}
