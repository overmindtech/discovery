package discovery

import (
	"sync"
	"testing"

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

func TestFilterSources(t *testing.T) {
	e := Engine{
		Name: "testEngine",
	}

	e.AddSources(
		&TestSource{
			ReturnContexts: []string{"test"},
			ReturnType:     "person",
		},
		&TestSource{
			ReturnContexts: []string{AllContexts},
			ReturnType:     "person",
		},
		&TestSource{
			ReturnContexts: []string{
				"testA",
				"testB",
			},
			ReturnType: "chair",
		},
	)

	// Right type wrong context
	if s := e.FilterSources("person", "wrong"); len(s) != 1 {
		t.Error("expected only wildcard context to match")
	}

	// Right context wrong type
	if s := e.FilterSources("wrong", "test"); len(s) != 0 {
		t.Error("found source when expecting no filter results")
	}

	// Right both
	if x := len(e.FilterSources("person", "test")); x != 2 {
		t.Errorf("expected to find 2 sources, found %v", x)
	}

	// Multi-context
	if x := len(e.FilterSources("chair", "testB")); x != 1 {
		t.Errorf("expected to find 1 source, found %v", x)
	}
}

func TestSourceAdd(t *testing.T) {
	e := Engine{
		Name: "testEngine",
	}

	src := TestSource{}

	e.AddSources(&src)

	if x := len(e.Sources()); x != 1 {
		t.Fatalf("Expected 1 source, got %v", x)
	}
}

func TestGet(t *testing.T) {
	e := Engine{
		Name: "testEngine",
	}

	src := TestSource{}

	e.AddSources(&src)

	e.Get("person", "test", "three")

	if x := len(src.GetCalls); x != 1 {
		t.Fatalf("Expected 1 get call, got %v", x)
	}

	firstCall := src.GetCalls[0]

	if firstCall[0] != "test" || firstCall[1] != "three" {
		t.Fatalf("First get call parameters unexpected: %v", firstCall)
	}
}

func TestFind(t *testing.T) {
	e := Engine{
		Name: "testEngine",
	}

	src := TestSource{}

	e.AddSources(&src)

	e.Find("person", "test")

	if x := len(src.FindCalls); x != 1 {
		t.Fatalf("Expected 1 find call, got %v", x)
	}

	firstCall := src.FindCalls[0]

	if firstCall[0] != "test" {
		t.Fatalf("First find call parameters unexpected: %v", firstCall)
	}
}

func TestSearch(t *testing.T) {
	e := Engine{
		Name: "testEngine",
	}

	src := TestSource{}

	e.AddSources(&src)

	e.Search("person", "test", "query")

	if x := len(src.SearchCalls); x != 1 {
		t.Fatalf("Expected 1 Search call, got %v", x)
	}

	firstCall := src.SearchCalls[0]

	if firstCall[0] != "test" || firstCall[1] != "query" {
		t.Fatalf("First Search call parameters unexpected: %v", firstCall)
	}
}
