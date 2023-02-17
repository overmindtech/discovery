package discovery

import (
	"testing"

	"github.com/overmindtech/sdp-go"
)

type ExpectExpand struct {
	NumRequests int

	// Note that this is not the number of unique sources, but teh number of
	// sources total. So if a source would be hit twice this will be 2
	NumSources int
}

func (e *ExpectExpand) Validate(t *testing.T, m map[*sdp.ItemRequest][]Source) {
	t.Helper()

	numSources := 0
	numRequests := 0

	for _, v := range m {
		numRequests++
		numSources = numSources + len(v)
	}

	if e.NumRequests != numRequests {
		t.Errorf("Expected %v requests, got %v", e.NumRequests, numRequests)
	}

	if e.NumSources != numSources {
		t.Errorf("Expected %v sources, got %v", e.NumSources, numSources)
	}
}

func TestSourceHostExpandRequest(t *testing.T) {
	sh, err := NewSourceHost()
	if err != nil {
		t.Fatalf("Error initializing SourceHost: %v", err)
	}

	sh.AddSources(
		&TestSource{
			ReturnScopes: []string{"test"},
			ReturnType:   "person",
		},
		&TestSource{
			ReturnScopes: []string{"test"},
			ReturnType:   "fish",
		},
		&TestSource{
			ReturnScopes: []string{sdp.WILDCARD},
			ReturnType:   "person",
		},
		&TestSource{
			ReturnScopes: []string{
				"testA",
				"testB",
			},
			ReturnType: "chair",
		},
		&TestSource{
			ReturnScopes: []string{"test"},
			ReturnType:   "hidden_person",
			IsHidden:     true,
		},
	)

	t.Run("Right type wrong scope", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "person",
			Scope: "wrong",
		}

		ee := ExpectExpand{
			NumRequests: 1,
			NumSources:  1,
		}

		ee.Validate(t, sh.ExpandRequest(&req))
	})

	t.Run("Right scope wrong type", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "wrong",
			Scope: "test",
		}

		ee := ExpectExpand{
			NumRequests: 0,
			NumSources:  0,
		}

		ee.Validate(t, sh.ExpandRequest(&req))
	})

	t.Run("Right both", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "person",
			Scope: "test",
		}

		ee := ExpectExpand{
			NumRequests: 1,
			NumSources:  2,
		}

		ee.Validate(t, sh.ExpandRequest(&req))
	})

	t.Run("Multi-scope", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "chair",
			Scope: "testB",
		}

		ee := ExpectExpand{
			NumRequests: 1,
			NumSources:  1,
		}

		ee.Validate(t, sh.ExpandRequest(&req))
	})

	t.Run("Wildcard scope", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "person",
			Scope: sdp.WILDCARD,
		}

		ee := ExpectExpand{
			NumRequests: 2,
			NumSources:  2,
		}

		ee.Validate(t, sh.ExpandRequest(&req))

		req = sdp.ItemRequest{
			Type:  "chair",
			Scope: sdp.WILDCARD,
		}

		ee = ExpectExpand{
			NumRequests: 2,
			NumSources:  2,
		}

		ee.Validate(t, sh.ExpandRequest(&req))
	})

	t.Run("Wildcard type", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  sdp.WILDCARD,
			Scope: "test",
		}

		ee := ExpectExpand{
			NumRequests: 2,
			NumSources:  3,
		}

		ee.Validate(t, sh.ExpandRequest(&req))
	})

	t.Run("Wildcard both", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  sdp.WILDCARD,
			Scope: sdp.WILDCARD,
		}

		ee := ExpectExpand{
			NumRequests: 5,
			NumSources:  5,
		}

		ee.Validate(t, sh.ExpandRequest(&req))
	})

	t.Run("Listing hidden source with wildcard scope", func(t *testing.T) {
		req := sdp.ItemRequest{
			Type:  "hidden_person",
			Scope: sdp.WILDCARD,
		}
		if x := len(sh.ExpandRequest(&req)); x != 0 {
			t.Errorf("expected to find 0 sources, found %v", x)
		}

		req = sdp.ItemRequest{
			Type:  "hidden_person",
			Scope: "test",
		}
		if x := len(sh.ExpandRequest(&req)); x != 1 {
			t.Errorf("expected to find 1 sources, found %v", x)
		}
	})
}

func TestSourceHostAddSources(t *testing.T) {
	sh, err := NewSourceHost()
	if err != nil {
		t.Fatalf("Error initializing SourceHost: %v", err)
	}

	src := TestSource{}

	sh.AddSources(&src)

	if x := len(sh.Sources()); x != 4 {
		t.Fatalf("Expected 4 source, got %v", x)
	}
}
