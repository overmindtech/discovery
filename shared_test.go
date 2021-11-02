package discovery

import (
	"sync"
	"time"

	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/structpb"
)

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

func (s *TestSource) Get(itemContext string, query string) (*sdp.Item, error) {
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

func (s *TestSource) Find(itemContext string) ([]*sdp.Item, error) {
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

func (s *TestSource) Search(itemContext string, query string) ([]*sdp.Item, error) {
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
