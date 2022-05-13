package discovery

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/overmindtech/sdp-go"
)

type SlowSource struct {
	RequestDuration time.Duration
}

func (s *SlowSource) Type() string {
	return "person"
}

func (s *SlowSource) Name() string {
	return "slow-source"
}

func (s *SlowSource) DefaultCacheDuration() time.Duration {
	return 10 * time.Minute
}

func (s *SlowSource) Contexts() []string {
	return []string{"test"}
}

func (s *SlowSource) Hidden() bool {
	return false
}

func (s *SlowSource) Get(ctx context.Context, itemContext string, query string) (*sdp.Item, error) {
	end := time.Now().Add(s.RequestDuration)
	attributes, _ := sdp.ToAttributes(map[string]interface{}{
		"name": query,
	})

	item := sdp.Item{
		Type:               "person",
		UniqueAttribute:    "name",
		Attributes:         attributes,
		Context:            "test",
		LinkedItemRequests: []*sdp.ItemRequest{},
	}

	for i := 0; i != 2; i++ {
		item.LinkedItemRequests = append(item.LinkedItemRequests, &sdp.ItemRequest{
			Type:    "person",
			Method:  sdp.RequestMethod_GET,
			Query:   RandomName(),
			Context: "test",
		})
	}

	time.Sleep(time.Until(end))

	return &item, nil
}

func (s *SlowSource) Find(ctx context.Context, itemContext string) ([]*sdp.Item, error) {
	return []*sdp.Item{}, nil
}

func (s *SlowSource) Weight() int {
	return 100
}

func TestParallelRequestPerformance(t *testing.T) {
	// This test is designed to ensure that request duration is linear up to a
	// certain point. Above that point the overhead caused by having so many
	// goroutines running will start to make the response times non-linear which
	// maybe isn't ideal but given realistic loads we probably don't care.
	t.Run("Without linking", func(t *testing.T) {
		RunLinearPerformanceTest(t, "10 requests", 10, 0, 1)
		RunLinearPerformanceTest(t, "100 requests", 100, 0, 10)
		RunLinearPerformanceTest(t, "1,000 requests", 1000, 0, 100)
	})

	t.Run("With linking", func(t *testing.T) {
		RunLinearPerformanceTest(t, "1 request 3 depth", 1, 3, 1)
		RunLinearPerformanceTest(t, "1 request 3 depth", 1, 3, 100)
		RunLinearPerformanceTest(t, "1 request 5 depth", 1, 5, 100)
		RunLinearPerformanceTest(t, "10 requests 5 depth", 10, 5, 100)
	})
}

// RunLinearPerformanceTest Runs a test with a given number in input requests,
// link depth and parallelisation limit. Expected results and expected duration
// are determined automatically meaning all this is testing for is the fact that
// the perfomance continues to be linear and predictable
func RunLinearPerformanceTest(t *testing.T, name string, numRequests int, linkDepth int, numParallel int) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		result := TimeRequests(numRequests, linkDepth, numParallel)

		if len(result.Results) != result.ExpectedItems {
			t.Errorf("Expected %v items, got %v", result.ExpectedItems, len(result.Results))
		}

		if result.TimeTaken > result.MaxTime {
			t.Errorf("Requests took too long: %v Max: %v", result.TimeTaken.String(), result.MaxTime.String())
		}
	})
}

type TimedResults struct {
	ExpectedItems int
	MaxTime       time.Duration
	TimeTaken     time.Duration
	Results       []*sdp.Item
	Errors        []error
}

func TimeRequests(numRequests int, linkDepth int, numParallel int) TimedResults {
	engine := Engine{
		Name:                  "performance-test",
		MaxParallelExecutions: numParallel,
	}
	engine.AddSources(&SlowSource{
		RequestDuration: 100 * time.Millisecond,
	})
	engine.Start()
	defer engine.Stop()

	// Calculate how many items to expect and the expected duration
	var expectedItems int
	var expectedDuration time.Duration
	for i := 0; i <= linkDepth; i++ {
		thisLayer := int(math.Pow(2, float64(i))) * numRequests
		thisDuration := 110 * math.Ceil(float64(thisLayer)/float64(numParallel))
		expectedDuration = expectedDuration + (time.Duration(thisDuration) * time.Millisecond)
		expectedItems = expectedItems + thisLayer
	}

	results := make([]*sdp.Item, 0)
	errors := make([]error, 0)
	resultsMutex := sync.Mutex{}
	wg := sync.WaitGroup{}

	start := time.Now()

	for i := 0; i < numRequests; i++ {
		rt := RequestTracker{
			Request: &sdp.ItemRequest{
				Type:      "person",
				Method:    sdp.RequestMethod_GET,
				Query:     RandomName(),
				Context:   "test",
				LinkDepth: uint32(linkDepth),
			},
			Engine: &engine,
		}

		wg.Add(1)

		go func(rt *RequestTracker) {
			defer wg.Done()

			items, err := rt.Execute()

			resultsMutex.Lock()
			results = append(results, items...)
			if err != nil {
				errors = append(errors, err)
			}
			resultsMutex.Unlock()
		}(&rt)
	}

	wg.Wait()

	return TimedResults{
		ExpectedItems: expectedItems,
		MaxTime:       expectedDuration,
		TimeTaken:     time.Since(start),
		Results:       results,
		Errors:        errors,
	}
}
