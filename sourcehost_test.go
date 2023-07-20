package discovery

import (
	"testing"

	"github.com/overmindtech/sdp-go"
)

type ExpectExpand struct {
	NumQueries int

	// Note that this is not the number of unique sources, but teh number of
	// sources total. So if a source would be hit twice this will be 2
	NumSources int
}

func (e *ExpectExpand) Validate(t *testing.T, m map[*sdp.Query][]Source) {
	t.Helper()

	numSources := 0
	numQueries := 0

	for _, v := range m {
		numQueries++
		numSources = numSources + len(v)
	}

	if e.NumQueries != numQueries {
		t.Errorf("Expected %v queries, got %v", e.NumQueries, numQueries)
	}

	if e.NumSources != numSources {
		t.Errorf("Expected %v sources, got %v", e.NumSources, numSources)
	}
}

func TestSourceHostExpandQuery(t *testing.T) {
	sh := NewSourceHost()

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
		req := sdp.Query{
			Type:  "person",
			Scope: "wrong",
		}

		ee := ExpectExpand{
			NumQueries: 1,
			NumSources: 1,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Right scope wrong type", func(t *testing.T) {
		req := sdp.Query{
			Type:  "wrong",
			Scope: "test",
		}

		ee := ExpectExpand{
			NumQueries: 0,
			NumSources: 0,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Right both", func(t *testing.T) {
		req := sdp.Query{
			Type:  "person",
			Scope: "test",
		}

		ee := ExpectExpand{
			NumQueries: 1,
			NumSources: 2,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Multi-scope", func(t *testing.T) {
		req := sdp.Query{
			Type:  "chair",
			Scope: "testB",
		}

		ee := ExpectExpand{
			NumQueries: 1,
			NumSources: 1,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Wildcard scope", func(t *testing.T) {
		req := sdp.Query{
			Type:  "person",
			Scope: sdp.WILDCARD,
		}

		ee := ExpectExpand{
			NumQueries: 2,
			NumSources: 2,
		}

		ee.Validate(t, sh.ExpandQuery(&req))

		req = sdp.Query{
			Type:  "chair",
			Scope: sdp.WILDCARD,
		}

		ee = ExpectExpand{
			NumQueries: 2,
			NumSources: 2,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Wildcard type", func(t *testing.T) {
		req := sdp.Query{
			Type:  sdp.WILDCARD,
			Scope: "test",
		}

		ee := ExpectExpand{
			NumQueries: 2,
			NumSources: 3,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Wildcard both", func(t *testing.T) {
		req := sdp.Query{
			Type:  sdp.WILDCARD,
			Scope: sdp.WILDCARD,
		}

		ee := ExpectExpand{
			NumQueries: 5,
			NumSources: 5,
		}

		ee.Validate(t, sh.ExpandQuery(&req))
	})

	t.Run("Listing hidden source with wildcard scope", func(t *testing.T) {
		req := sdp.Query{
			Type:  "hidden_person",
			Scope: sdp.WILDCARD,
		}
		if x := len(sh.ExpandQuery(&req)); x != 0 {
			t.Errorf("expected to find 0 sources, found %v", x)
		}

		req = sdp.Query{
			Type:  "hidden_person",
			Scope: "test",
		}
		if x := len(sh.ExpandQuery(&req)); x != 1 {
			t.Errorf("expected to find 1 sources, found %v", x)
		}
	})
}

func TestSourceHostAddSources(t *testing.T) {
	sh := NewSourceHost()

	src := TestSource{}

	sh.AddSources(&src)

	if x := len(sh.Sources()); x != 4 {
		t.Fatalf("Expected 4 source, got %v", x)
	}
}
