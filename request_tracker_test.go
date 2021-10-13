package discovery

import (
	"testing"

	"github.com/dylanratcliffe/sdp-go"
)

func TestExecute(t *testing.T) {
	engine := Engine{
		Name:                  "test",
		MaxParallelExecutions: 1,
	}

	src := TestSource{
		ReturnType: "person",
		ReturnContexts: []string{
			"test",
		},
	}

	engine.AddSources(&src)

	t.Run("Without linking", func(t *testing.T) {
		t.Parallel()

		rt := RequestTracker{
			Engine: &engine,
			Requests: []*sdp.ItemRequest{
				{
					Type:      "person",
					Method:    sdp.RequestMethod_GET,
					Query:     "Dylan",
					LinkDepth: 0,
					Context:   "test",
				},
			},
		}

		items, err := rt.Execute()

		if err != nil {
			t.Error(err)
		}

		if l := len(items); l != 1 {
			t.Errorf("expected 1 items, got %v", l)
		}
	})

	t.Run("With linking", func(t *testing.T) {
		t.Parallel()

		rt := RequestTracker{
			Engine: &engine,
			Requests: []*sdp.ItemRequest{
				{
					Type:      "person",
					Method:    sdp.RequestMethod_GET,
					Query:     "Dylan",
					LinkDepth: 10,
					Context:   "test",
				},
			},
		}

		items, err := rt.Execute()

		if err != nil {
			t.Error(err)
		}

		if l := len(items); l != 11 {
			t.Errorf("expected 10 items, got %v", l)
		}
	})

}
