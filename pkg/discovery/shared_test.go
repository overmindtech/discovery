package discovery

import (
	"sync"

	"github.com/dylanratcliffe/sdp-go"
	"google.golang.org/protobuf/types/known/structpb"
)

var item = sdp.Item{
	Type:            "person",
	Context:         "global",
	UniqueAttribute: "name",
	Attributes: &sdp.ItemAttributes{
		AttrStruct: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"name": structpb.NewStringValue("Dylan"),
				"age":  structpb.NewNumberValue(28),
			},
		},
	},
	LinkedItemRequests: []*sdp.ItemRequest{
		{
			Type:    "dog",
			Method:  sdp.RequestMethod_GET,
			Query:   "Manny",
			Context: "global",
		},
	},
}

type TestSource struct {
	ReturnContexts []string
	ReturnType     string
	GetCalls       [][]string
	FindCalls      [][]string
	SearchCalls    [][]string
	mutex          sync.Mutex
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

	return &item, nil
}

func (s *TestSource) Find(itemContext string) ([]*sdp.Item, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.FindCalls = append(s.GetCalls, []string{itemContext})

	return []*sdp.Item{&item}, nil

}

func (s *TestSource) Search(itemContext string, query string) ([]*sdp.Item, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.SearchCalls = append(s.GetCalls, []string{itemContext, query})

	return []*sdp.Item{&item}, nil

}

func (s *TestSource) Weight() int {
	return 10
}
