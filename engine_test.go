package discovery

import (
	"fmt"
	"sync"
	"testing"

	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/goombaio/namegenerator"
	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/structpb"
)

type TestBackend struct{}

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

func (tb TestBackend) Type() string {
	return "person"
}

func (tb TestBackend) BackendPackage() string {
	return "test"
}

func (tb TestBackend) Threadsafe() bool {
	return true
}

// Always is able to find a person for testing. Also always has a linked item
func (tb TestBackend) Get(name string) (*sdp.Item, error) {
	if name == "fail" {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOTFOUND,
			ErrorString: "Error requested",
		}
	}

	item := &sdp.Item{
		Type:            "person",
		Context:         "global",
		UniqueAttribute: "name",
		Attributes: &sdp.ItemAttributes{
			AttrStruct: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"name": structpb.NewStringValue(name),
					"age":  structpb.NewNumberValue(rand.Float64()),
				},
			},
		},
		LinkedItemRequests: []*sdp.ItemRequest{
			{
				Type:   "person",
				Method: sdp.RequestMethod_GET,
				Query:  RandomName(),
			},
			{
				Type:    "shadow",
				Method:  sdp.RequestMethod_GET,
				Query:   name,
				Context: "shadowrealm", // From another context
			},
			{
				Type:   "person",
				Method: sdp.RequestMethod_GET,
				Query:  "fail", // Will always fail
			},
		},
	}

	return item, nil
}

// Always returns 10 random items
func (tb TestBackend) Find() ([]*sdp.Item, error) {
	items := make([]*sdp.Item, 10)

	for i := 0; i < 10; i++ {
		items[i], _ = tb.Get(RandomName())
	}

	return items, nil
}

func (tb TestBackend) Search(query string) ([]*sdp.Item, error) {
	return tb.Find()
}

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
