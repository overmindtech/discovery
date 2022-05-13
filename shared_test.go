package discovery

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/goombaio/namegenerator"
	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/structpb"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randString(length int) string {
	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func RandomName() string {
	seed := time.Now().UTC().UnixNano()
	nameGenerator := namegenerator.NewNameGenerator(seed)
	name := nameGenerator.Generate()
	randGarbage := randString(10)
	return fmt.Sprintf("%v-%v", name, randGarbage)
}

func (s *TestSource) NewTestItem(itemContext string, query string) *sdp.Item {
	return &sdp.Item{
		Type:            s.Type(),
		Context:         itemContext,
		UniqueAttribute: "name",
		Attributes: &sdp.ItemAttributes{
			AttrStruct: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"name": structpb.NewStringValue(query),
					"age":  structpb.NewNumberValue(28),
				},
			},
		},
		LinkedItemRequests: []*sdp.ItemRequest{
			{
				Type:    "person",
				Method:  sdp.RequestMethod_GET,
				Query:   RandomName(),
				Context: itemContext,
			},
		},
	}
}

type TestSource struct {
	ReturnContexts []string
	ReturnType     string
	GetCalls       [][]string
	FindCalls      [][]string
	SearchCalls    [][]string
	IsHidden       bool
	ReturnWeight   int
	ReturnName     string
	mutex          sync.Mutex
}

// ClearCalls Clears the call counters between tests
func (s *TestSource) ClearCalls() {
	s.FindCalls = make([][]string, 0)
	s.SearchCalls = make([][]string, 0)
	s.GetCalls = make([][]string, 0)
}

func (s *TestSource) Type() string {
	if s.ReturnType != "" {
		return s.ReturnType
	}

	return "person"
}

func (s *TestSource) Name() string {
	return fmt.Sprintf("testSource-%v", s.ReturnName)
}

func (s *TestSource) DefaultCacheDuration() time.Duration {
	return 100 * time.Millisecond
}

func (s *TestSource) Contexts() []string {
	if len(s.ReturnContexts) > 0 {
		return s.ReturnContexts
	}

	return []string{"test"}
}

func (s *TestSource) Hidden() bool {
	return s.IsHidden
}

func (s *TestSource) Get(ctx context.Context, itemContext string, query string) (*sdp.Item, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.GetCalls = append(s.GetCalls, []string{itemContext, query})

	switch itemContext {
	case "empty":
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOTFOUND,
			ErrorString: "not found (test)",
			Context:     itemContext,
		}
	case "error":
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_OTHER,
			ErrorString: "Error for testing",
			Context:     itemContext,
		}
	default:
		return s.NewTestItem(itemContext, query), nil
	}
}

func (s *TestSource) Find(ctx context.Context, itemContext string) ([]*sdp.Item, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.FindCalls = append(s.FindCalls, []string{itemContext})

	switch itemContext {
	case "empty":
		return nil, nil
	case "error":
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_OTHER,
			ErrorString: "Error for testing",
			Context:     itemContext,
		}
	default:
		return []*sdp.Item{s.NewTestItem(itemContext, "Dylan")}, nil
	}
}

func (s *TestSource) Search(ctx context.Context, itemContext string, query string) ([]*sdp.Item, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.SearchCalls = append(s.SearchCalls, []string{itemContext, query})

	switch itemContext {
	case "empty":
		return nil, nil
	case "error":
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_OTHER,
			ErrorString: "Error for testing",
			Context:     itemContext,
		}
	default:
		return []*sdp.Item{s.NewTestItem(itemContext, "Dylan")}, nil
	}
}

func (s *TestSource) Weight() int {
	return s.ReturnWeight
}
