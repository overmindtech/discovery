package discovery

import (
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/overmindtech/multiconn"
	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

type TriggerTest struct {
	Name        string
	Trigger     Trigger
	ExpectError bool
	Item        sdp.Item
}

var testTrigger = Trigger{
	Type:                      "person",
	UniqueAttributeValueRegex: regexp.MustCompile(`^[Dd]ylan$`),
	RequestGenerator: func(in *sdp.Item) (*sdp.ItemRequest, error) {
		if in.GetContext() != "match" {
			return nil, errors.New("context not match")
		} else {
			return &sdp.ItemRequest{
				Type:   "dog",
				Method: sdp.RequestMethod_SEARCH,
				Query:  "pug",
			}, nil
		}
	},
}

var u = uuid.New()

var tests = []TriggerTest{
	{
		Name:        "with matching item",
		Trigger:     testTrigger,
		ExpectError: false,
		Item: sdp.Item{
			Type:            "person",
			UniqueAttribute: "name",
			Attributes: &sdp.ItemAttributes{
				AttrStruct: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"name": {
							Kind: &structpb.Value_StringValue{
								StringValue: "dylan",
							},
						},
					},
				},
			},
			Metadata: &sdp.Metadata{
				SourceName: "people",
				SourceRequest: &sdp.ItemRequest{
					Type:            "person",
					Method:          sdp.RequestMethod_GET,
					Query:           "dylan",
					LinkDepth:       6,
					Context:         "match",
					IgnoreCache:     true,
					UUID:            u[:],
					Timeout:         durationpb.New(20 * time.Second),
					ItemSubject:     "return.item." + nats.NewInbox(),
					ResponseSubject: "return.response." + nats.NewInbox(),
				},
			},
			Context:            "match",
			LinkedItems:        []*sdp.Reference{},
			LinkedItemRequests: []*sdp.ItemRequest{},
		},
	},
	{
		Name:        "with a mismatched type",
		Trigger:     testTrigger,
		ExpectError: true,
		Item: sdp.Item{
			Type:            "tree",
			UniqueAttribute: "name",
			Attributes: &sdp.ItemAttributes{
				AttrStruct: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"name": {
							Kind: &structpb.Value_StringValue{
								StringValue: "oak",
							},
						},
					},
				},
			},
			Metadata: &sdp.Metadata{
				SourceName: "trees",
				SourceRequest: &sdp.ItemRequest{
					Type:            "tree",
					Method:          sdp.RequestMethod_GET,
					Query:           "oak",
					LinkDepth:       6,
					Context:         "match",
					IgnoreCache:     true,
					UUID:            u[:],
					Timeout:         durationpb.New(20 * time.Second),
					ItemSubject:     "itemSubject",
					ResponseSubject: "responseSubject",
				},
			},
			Context:            "match",
			LinkedItems:        []*sdp.Reference{},
			LinkedItemRequests: []*sdp.ItemRequest{},
		},
	},
	{
		Name:        "with a mismatched UniqueAttributeValue",
		Trigger:     testTrigger,
		ExpectError: true,
		Item: sdp.Item{
			Type:            "person",
			UniqueAttribute: "name",
			Attributes: &sdp.ItemAttributes{
				AttrStruct: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"name": {
							Kind: &structpb.Value_StringValue{
								StringValue: "dylan-1",
							},
						},
					},
				},
			},
			Metadata: &sdp.Metadata{
				SourceName: "people",
				SourceRequest: &sdp.ItemRequest{
					Type:            "person",
					Method:          sdp.RequestMethod_GET,
					Query:           "dylan",
					LinkDepth:       6,
					Context:         "match",
					IgnoreCache:     true,
					UUID:            u[:],
					Timeout:         durationpb.New(20 * time.Second),
					ItemSubject:     "itemSubject",
					ResponseSubject: "responseSubject",
				},
			},
			Context:            "match",
			LinkedItems:        []*sdp.Reference{},
			LinkedItemRequests: []*sdp.ItemRequest{},
		},
	},
}

func TestStandaloneTriggers(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			req, err := tt.Trigger.ProcessItem(&tt.Item)

			if tt.ExpectError {
				if err == nil {
					t.Fatal("expeected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}

				if req.Type != "dog" {
					t.Errorf("expected request to have a type pug, got %v", req.Type)
				}

				if req.Context != tt.Item.Context {
					t.Errorf("expected request to have copied contextfrom item (%v) got %v", tt.Item.Context, req.Context)
				}

				if req.Method != sdp.RequestMethod_SEARCH {
					t.Error("mismatched request type")
				}

				if req.Query != "pug" {
					t.Errorf("mismatched query, expected pug got %v", req.Query)
				}

				if expected := tt.Item.Metadata.SourceRequest.LinkDepth - 1; req.LinkDepth != expected {
					t.Errorf("link depth was %v expected %v", req.LinkDepth, expected)
				}

				if req.IgnoreCache != tt.Item.Metadata.SourceRequest.IgnoreCache {
					t.Error("ignore cache mismatch")
				}

				if _, err := uuid.FromBytes(req.UUID); err != nil {
					t.Error(err)
				}

				if req.Timeout.AsDuration().Seconds() != float64(20) {
					t.Errorf("timeout mismatch. Expect 20s, got %v", req.Timeout.String())
				}

				if expected := tt.Item.Metadata.SourceRequest.ItemSubject; req.ItemSubject != expected {
					t.Errorf("item subject mismatch, expected %v got %v", expected, req.ItemSubject)
				}

				if expected := tt.Item.Metadata.SourceRequest.ResponseSubject; req.ResponseSubject != expected {
					t.Errorf("response subject mismatch, expected %v got %v", expected, req.ItemSubject)

				}
			}
		})
	}
}

func TestNATSTriggers(t *testing.T) {
	SkipWithoutNats(t)

	engine := Engine{
		Name: "trigger-testing",
		NATSOptions: &multiconn.NATSConnectionOptions{
			Servers: NatsTestURLs,
		},
		MaxParallelExecutions: 1,
	}

	source := TestSource{
		ReturnType: "dog",
		IsHidden:   false,
		ReturnContexts: []string{
			"match",
		},
	}

	engine.AddSources(&source)

	for _, tt := range tests {
		if tt.ExpectError {
			// Don't run if we don't expect the trigger to fire
			continue
		}

		t.Run(tt.Name, func(t *testing.T) {
			t.Cleanup(source.ClearCalls)

			// Start the engine so that it subscribes to the correct subjects
			engine.AddTriggers(tt.Trigger)
			err := engine.Start()

			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() {
				engine.ClearTriggers()
				err := engine.Stop()

				if err != nil {
					t.Error(err)
				}
			})

			// Track progress. Note that this engine should be sending responses on
			// the same subject that the original request was sent on
			progress := sdp.NewRequestProgress(tt.Item.Metadata.SourceRequest)
			_, err = engine.natsConnection.Subscribe(
				tt.Item.Metadata.SourceRequest.ResponseSubject,
				progress.ProcessResponse,
			)

			if err != nil {
				t.Fatal(err)
			}

			// Send the test item as if it was the result of some other query
			err = engine.natsConnection.Publish(
				"return.item."+nats.NewInbox(),
				&tt.Item,
			)

			if err != nil {
				t.Fatal(err)
			}

			items := make(chan *sdp.Item, 1000)
			errs := make(chan *sdp.ItemRequestError, 1000)

			progress.Start(engine.natsConnection, items, errs)

			for range items {
				// Do nothing
			}
			for range errs {
				// Do nothing
			}

			if len(source.SearchCalls) != 1 {
				t.Fatalf("expected 1 search call, got %v", len(source.SearchCalls))
			}

			call := source.SearchCalls[0]

			if call[1] != "pug" {
				t.Fatalf("expected search call query to be 'pug', got %v", call[1])
			}
		})
	}
}
