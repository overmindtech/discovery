package discovery

import (
	"testing"

	"math/rand"
	"time"

	"github.com/dylanratcliffe/sdp-go"
	"github.com/goombaio/namegenerator"
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
