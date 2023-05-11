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

func (s *TestSource) NewTestItem(scope string, query string) *sdp.Item {
	return &sdp.Item{
		Type:            s.Type(),
		Scope:           scope,
		UniqueAttribute: "name",
		Attributes: &sdp.ItemAttributes{
			AttrStruct: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"name": structpb.NewStringValue(query),
					"age":  structpb.NewNumberValue(28),
				},
			},
		},
		LinkedItemQueries: []*sdp.LinkedItemQuery{
			{
				Query: &sdp.Query{
					Type:   "person",
					Method: sdp.QueryMethod_GET,
					Query:  RandomName(),
					Scope:  scope,
				},
			},
		},
	}
}

type TestSource struct {
	ReturnScopes []string
	ReturnType   string
	GetCalls     [][]string
	ListCalls    [][]string
	SearchCalls  [][]string
	IsHidden     bool
	ReturnWeight int    // Weight to be returned
	ReturnName   string // The name of the source
	mutex        sync.Mutex
}

// ClearCalls Clears the call counters between tests
func (s *TestSource) ClearCalls() {
	s.ListCalls = make([][]string, 0)
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

func (s *TestSource) Scopes() []string {
	if len(s.ReturnScopes) > 0 {
		return s.ReturnScopes
	}

	return []string{"test"}
}

func (s *TestSource) Hidden() bool {
	return s.IsHidden
}

func (s *TestSource) Get(ctx context.Context, scope string, query string) (*sdp.Item, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.GetCalls = append(s.GetCalls, []string{scope, query})

	switch scope {
	case "empty":
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOTFOUND,
			ErrorString: "not found (test)",
			Scope:       scope,
		}
	case "error":
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_OTHER,
			ErrorString: "Error for testing",
			Scope:       scope,
		}
	default:
		return s.NewTestItem(scope, query), nil
	}
}

func (s *TestSource) List(ctx context.Context, scope string) ([]*sdp.Item, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.ListCalls = append(s.ListCalls, []string{scope})

	switch scope {
	case "empty":
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOTFOUND,
			ErrorString: "no items found",
			Scope:       scope,
		}
	case "error":
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_OTHER,
			ErrorString: "Error for testing",
			Scope:       scope,
		}
	default:
		return []*sdp.Item{s.NewTestItem(scope, "Dylan")}, nil
	}
}

func (s *TestSource) Search(ctx context.Context, scope string, query string) ([]*sdp.Item, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.SearchCalls = append(s.SearchCalls, []string{scope, query})

	switch scope {
	case "empty":
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOTFOUND,
			ErrorString: "no items found",
			Scope:       scope,
		}
	case "error":
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_OTHER,
			ErrorString: "Error for testing",
			Scope:       scope,
		}
	default:
		return []*sdp.Item{s.NewTestItem(scope, "Dylan")}, nil
	}
}

func (s *TestSource) Weight() int {
	return s.ReturnWeight
}
