package discovery

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/goombaio/namegenerator"
	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/structpb"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func RandomName() string {
	n := namegenerator.NewNameGenerator(time.Now().UTC().UnixNano())
	return n.Generate() + " " + n.Generate() + "-" + randSeq(10)
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
	return "testSource"
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
	return 10
}
