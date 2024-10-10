package discovery

import (
	"testing"

	"github.com/overmindtech/sdp-go"
)

type ExpectExpand struct {
	NumQueries int

	// Note that this is not the number of unique adapters, but teh number of
	// adapters total. So if a adapter would be hit twice this will be 2
	NumAdapters int
}

func (e *ExpectExpand) Validate(t *testing.T, m map[*sdp.Query][]Adapter) {
	t.Helper()

	numAdapters := 0
	numQueries := 0

	for _, v := range m {
		numQueries++
		numAdapters = numAdapters + len(v)
	}

	if e.NumQueries != numQueries {
		t.Errorf("Expected %v queries, got %v: %v", e.NumQueries, numQueries, m)
	}

	if e.NumAdapters != numAdapters {
		t.Errorf("Expected %v adapters, got %v: %v", e.NumAdapters, numAdapters, m)
	}
}

func TestAdapterHostExpandQuery(t *testing.T) {
	sh := NewAdapterHost()

	sh.AddAdapters(
		&TestAdapter{
			ReturnScopes: []string{"test"},
			ReturnType:   "person",
		},
		&TestAdapter{
			ReturnScopes: []string{"test"},
			ReturnType:   "fish",
		},
		&TestAdapter{
			ReturnScopes: []string{sdp.WILDCARD},
			ReturnType:   "person",
		},
		&TestAdapter{
			ReturnScopes: []string{
				"multiA",
				"multiB",
			},
			ReturnType: "chair",
		},
		&TestAdapter{
			ReturnScopes: []string{"test"},
			ReturnType:   "hidden_person",
			IsHidden:     true,
		},
	)

	t.Run("Right type wrong scope", func(t *testing.T) {
		req := sdp.Query{
			Type:  "person",
			Scope: "wrong",
		}

		ee := ExpectExpand{
			NumQueries:  1,
			NumAdapters: 1,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Right scope wrong type", func(t *testing.T) {
		req := sdp.Query{
			Type:  "wrong",
			Scope: "test",
		}

		ee := ExpectExpand{
			NumQueries:  0,
			NumAdapters: 0,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Right both", func(t *testing.T) {
		req := sdp.Query{
			Type:  "person",
			Scope: "test",
		}

		ee := ExpectExpand{
			NumQueries:  1,
			NumAdapters: 2,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Multi-scope", func(t *testing.T) {
		req := sdp.Query{
			Type:  "chair",
			Scope: "multiB",
		}

		ee := ExpectExpand{
			NumQueries:  1,
			NumAdapters: 1,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Wildcard scope", func(t *testing.T) {
		req := sdp.Query{
			Type:  "person",
			Scope: sdp.WILDCARD,
		}

		ee := ExpectExpand{
			NumQueries:  2,
			NumAdapters: 2,
		}

		ee.Validate(t, sh.ExpandQuery(&req))

		req = sdp.Query{
			Type:  "chair",
			Scope: sdp.WILDCARD,
		}

		ee = ExpectExpand{
			NumQueries:  2,
			NumAdapters: 2,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Wildcard type", func(t *testing.T) {
		req := sdp.Query{
			Type:  sdp.WILDCARD,
			Scope: "test",
		}

		ee := ExpectExpand{
			NumQueries:  2,
			NumAdapters: 3,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Wildcard both", func(t *testing.T) {
		req := sdp.Query{
			Type:  sdp.WILDCARD,
			Scope: sdp.WILDCARD,
		}

		ee := ExpectExpand{
			NumQueries:  5,
			NumAdapters: 5,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("substring match", func(t *testing.T) {
		req := sdp.Query{
			Type:  sdp.WILDCARD,
			Scope: "multi",
		}

		ee := ExpectExpand{
			NumQueries:  3,
			NumAdapters: 3,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Listing hidden adapter with wildcard scope", func(t *testing.T) {
		req := sdp.Query{
			Type:  "hidden_person",
			Scope: sdp.WILDCARD,
		}
		if x := len(sh.ExpandQuery(&req)); x != 0 {
			t.Errorf("expected to find 0 adapters, found %v", x)
		}

		req = sdp.Query{
			Type:  "hidden_person",
			Scope: "test",
		}
		if x := len(sh.ExpandQuery(&req)); x != 1 {
			t.Errorf("expected to find 1 adapter, found %v", x)
		}
	})
}

func TestAdapterHostAddAdapters(t *testing.T) {
	sh := NewAdapterHost()

	adapter := TestAdapter{}

	sh.AddAdapters(&adapter)

	if x := len(sh.Adapters()); x != 4 {
		t.Fatalf("Expected 4 adapters, got %v", x)
	}
}
