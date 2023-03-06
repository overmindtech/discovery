package discovery

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindtech/connect"
	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestExecuteQuery(t *testing.T) {
	src := TestSource{
		ReturnType:   "person",
		ReturnScopes: []string{"test"},
	}

	e := newStartedEngine(t, "TestExecuteQuery",
		&connect.NATSOptions{
			Servers:           NatsTestURLs,
			ConnectionName:    "test-connection",
			ConnectionTimeout: time.Second,
			MaxReconnects:     5,
		},
		&src,
	)

	t.Run("Basic happy-path Get query", func(t *testing.T) {
		q := &sdp.Query{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "foo",
			Scope:           "test",
			LinkDepth:       3,
			ItemSubject:     "items",
			ResponseSubject: "responses",
		}

		items, errs, err := e.ExecuteQuerySync(context.Background(), q)

		if err != nil {
			t.Error(err)
		}

		for _, e := range errs {
			t.Error(e)
		}

		if x := len(src.GetCalls); x != 1 {
			t.Errorf("expected source's Get() to have been called 1 time, got %v", x)
		}

		if x := len(items); x != 1 {
			t.Errorf("expected 1 item, got %v", x)
		}

		item := items[0]
		query := item.LinkedItemQueries[0]

		if ld := query.LinkDepth; ld != 2 {
			t.Errorf("expected linked item depth to be 1 less than the query (2), got %v", ld)
		}

		if is := query.ItemSubject; is != "items" {
			t.Errorf("expected linked item query itemsubject to be \"items\" got %v", is)
		}

		if is := query.ResponseSubject; is != "responses" {
			t.Errorf("expected linked item query ResponseSubject to be \"responses\" got %v", is)
		}

		if item.Metadata.SourceQuery != q {
			t.Error("source query mismatch")
		}

		if !bytes.Equal(item.Metadata.SourceQueryUUID, q.UUID) {
			t.Error("source query UUID mismatch")
		}
	})

	t.Run("Wrong scope Get query", func(t *testing.T) {
		q := &sdp.Query{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Query:     "foo",
			Scope:     "wrong",
			LinkDepth: 0,
		}

		_, errs, err := e.ExecuteQuerySync(context.Background(), q)

		if err == nil {
			t.Error("expected error but got nil")
		}

		if len(errs) == 1 {
			if errs[0].ErrorType != sdp.QueryError_NOSCOPE {
				t.Errorf("expected error type to be NOSCOPE, got %v", errs[0].ErrorType)
			}
		} else {
			t.Errorf("expected 1 error, got %v", len(errs))
		}

	})

	t.Run("Wrong type Get query", func(t *testing.T) {
		q := &sdp.Query{
			Type:      "house",
			Method:    sdp.RequestMethod_GET,
			Query:     "foo",
			Scope:     "test",
			LinkDepth: 0,
		}

		_, errs, err := e.ExecuteQuerySync(context.Background(), q)

		if err == nil {
			t.Error("expected erro but got nil")
		}

		if len(errs) == 1 {
			if errs[0].ErrorType != sdp.QueryError_NOSCOPE {
				t.Errorf("expected error type to be NOSCOPE, got %v", errs[0].ErrorType)
			}
		} else {
			t.Errorf("expected 1 error, got %v", len(errs))
		}
	})

	t.Run("Basic List query", func(t *testing.T) {
		q := &sdp.Query{
			Type:      "person",
			Method:    sdp.RequestMethod_LIST,
			Scope:     "test",
			LinkDepth: 5,
		}

		items, errs, err := e.ExecuteQuerySync(context.Background(), q)

		if err != nil {
			t.Error(err)
		}

		for _, e := range errs {
			t.Error(e)
		}

		if len(items) < 1 {
			t.Error("expected at least one item")
		}
	})

	t.Run("Basic Search query", func(t *testing.T) {
		q := &sdp.Query{
			Type:      "person",
			Method:    sdp.RequestMethod_SEARCH,
			Query:     "TEST",
			Scope:     "test",
			LinkDepth: 5,
		}

		items, errs, err := e.ExecuteQuerySync(context.Background(), q)

		if err != nil {
			t.Error(err)
		}

		for _, e := range errs {
			t.Error(e)
		}

		if len(items) < 1 {
			t.Error("expected at least one item")
		}
	})

}

func TestHandleQuery(t *testing.T) {
	personSource := TestSource{
		ReturnType: "person",
		ReturnScopes: []string{
			"test1",
			"test2",
		},
	}

	dogSource := TestSource{
		ReturnType: "dog",
		ReturnScopes: []string{
			"test1",
			"testA",
			"testB",
		},
	}

	e := newStartedEngine(t, "TestHandleQuery", nil, &personSource, &dogSource)

	t.Run("Wildcard type should be expanded", func(t *testing.T) {
		t.Cleanup(func() {
			personSource.ClearCalls()
			dogSource.ClearCalls()
		})

		req := sdp.Query{
			Type:      sdp.WILDCARD,
			Method:    sdp.RequestMethod_GET,
			Query:     "Dylan",
			Scope:     "test1",
			LinkDepth: 0,
		}

		// Run the handler
		e.HandleQuery(context.Background(), &req)

		// I'm expecting both sources to get a query since the type was *
		if l := len(personSource.GetCalls); l != 1 {
			t.Errorf("expected person backend to have 1 Get call, got %v", l)
		}

		if l := len(dogSource.GetCalls); l != 1 {
			t.Errorf("expected dog backend to have 1 Get call, got %v", l)
		}
	})

	t.Run("Wildcard scope should be expanded", func(t *testing.T) {
		t.Cleanup(func() {
			personSource.ClearCalls()
			dogSource.ClearCalls()
		})

		req := sdp.Query{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Query:     "Dylan1",
			Scope:     sdp.WILDCARD,
			LinkDepth: 0,
		}

		// Run the handler
		e.HandleQuery(context.Background(), &req)

		if l := len(personSource.GetCalls); l != 2 {
			t.Errorf("expected person backend to have 2 Get calls, got %v", l)
		}

		if l := len(dogSource.GetCalls); l != 0 {
			t.Errorf("expected dog backend to have 0 Get calls, got %v", l)
		}
	})

}

func TestWildcardSourceExpansion(t *testing.T) {

	personSource := TestSource{
		ReturnType: "person",
		ReturnScopes: []string{
			sdp.WILDCARD,
		},
	}

	e := newStartedEngine(t, "TestWildcardSourceExpansion", nil, &personSource)

	t.Run("query scope should be preserved", func(t *testing.T) {
		req := sdp.Query{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Query:     "Dylan1",
			Scope:     "something.specific",
			LinkDepth: 0,
		}

		// Run the handler
		e.HandleQuery(context.Background(), &req)

		if len(personSource.GetCalls) != 1 {
			t.Errorf("expected 1 get call got %v", len(personSource.GetCalls))
		}

		call := personSource.GetCalls[0]

		if expected := "something.specific"; call[0] != expected {
			t.Errorf("expected scope to be %v, got %v", expected, call[0])
		}

		if expected := "Dylan1"; call[1] != expected {
			t.Errorf("expected query to be %v, got %v", expected, call[1])
		}
	})
}

func TestSendQuerySync(t *testing.T) {
	SkipWithoutNats(t)

	src := TestSource{
		ReturnType: "person",
		ReturnScopes: []string{
			"test",
		},
	}

	e := newStartedEngine(t, "TestSendQuerySync", nil, &src)

	for i := 0; i < 250; i++ {
		u := uuid.New()

		var progress *sdp.QueryProgress
		var items []*sdp.Item

		progress = sdp.NewQueryProgress(&sdp.Query{
			Type:            "person",
			Method:          sdp.RequestMethod_GET,
			Query:           "Dylan",
			Scope:           "test",
			LinkDepth:       0,
			IgnoreCache:     false,
			UUID:            u[:],
			Timeout:         durationpb.New(10 * time.Minute),
			ItemSubject:     NewItemSubject(),
			ResponseSubject: NewResponseSubject(),
		})

		items, errs, err := progress.Execute(context.Background(), e.natsConnection)

		if err != nil {
			t.Fatal(err)
		}

		if len(errs) != 0 {
			for _, err := range errs {
				t.Error(err)
			}
		}

		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %v", len(items))
		}

		if progress.NumComplete() != 1 {
			t.Fatalf("expected 1 to be complete, got %v\nProgress: %v", progress.NumComplete(), progress)
		}

	}

}

func TestExpandQuery(t *testing.T) {
	t.Run("with a single source with a single scope", func(t *testing.T) {
		simple := TestSource{
			ReturnScopes: []string{
				"test1",
			},
		}
		e := newStartedEngine(t, "TestExpandQuery", nil, &simple)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
			Query:  "Debby",
			Scope:  "*",
		})

		if expected := 1; len(simple.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, len(simple.GetCalls))
		}
	})

	t.Run("with a single source with many scopes", func(t *testing.T) {
		many := TestSource{
			ReturnName: "many",
			ReturnScopes: []string{
				"test1",
				"test2",
				"test3",
			},
		}
		e := newStartedEngine(t, "TestExpandQuery", nil, &many)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
			Query:  "Debby",
			Scope:  "*",
		})

		if expected := 3; len(many.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, many.GetCalls)
		}
	})

	t.Run("with many sources with single scopes", func(t *testing.T) {
		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test2",
			},
		}
		e := newStartedEngine(t, "TestExpandQuery", nil, &sx, &sy)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
			Query:  "Daniel",
			Scope:  "*",
		})

		if expected := 1; len(sx.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.GetCalls)
		}

		if expected := 1; len(sy.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.GetCalls)
		}
	})

	t.Run("with many sources with many scopes", func(t *testing.T) {
		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
				"test2",
				"test3",
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test4",
				"test5",
				"test6",
			},
		}

		e := newStartedEngine(t, "TestExpandQuery", nil, &sx, &sy)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
			Query:  "Steven",
			Scope:  "*",
		})

		if expected := 3; len(sx.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.GetCalls)
		}

		if expected := 3; len(sy.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.GetCalls)
		}
	})

	t.Run("with many sources with many scopes which overlap GET", func(t *testing.T) {
		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
				"test2",
				"test3",
			},
			ReturnWeight: 10,
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test2",
				"test3",
				"test4",
			},
			ReturnWeight: 11,
		}

		e := newStartedEngine(t, "TestExpandQuery", nil, &sx, &sy)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.RequestMethod_GET,
			Query:  "Jane",
			Scope:  "*",
		})

		if expected := 1; len(sx.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.GetCalls)
		}

		if expected := 3; len(sy.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.GetCalls)
		}
	})

	t.Run("with many sources with many scopes which overlap LIST", func(t *testing.T) {
		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
				"test2",
				"test3",
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test2",
				"test3",
				"test4",
			},
		}

		e := newStartedEngine(t, "TestExpandQuery", nil, &sx, &sy)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.RequestMethod_LIST,
			Query:  "Jane",
			Scope:  "*",
		})

		if expected := 3; len(sx.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.ListCalls)
		}

		if expected := 3; len(sy.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.ListCalls)
		}
	})

	t.Run("with a single wildcard source", func(t *testing.T) {
		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		e := newStartedEngine(t, "TestExpandQuery", nil, &sx)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.RequestMethod_LIST,
			Query:  "Rachel",
			Scope:  "*",
		})

		if expected := 1; len(sx.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.ListCalls)
		}
	})

	t.Run("with a many wildcard sources", func(t *testing.T) {
		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		sz := TestSource{
			ReturnName: "sz",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		e := newStartedEngine(t, "TestExpandQuery", nil, &sx, &sy, &sz)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.RequestMethod_LIST,
			Query:  "Ross",
			Scope:  "*",
		})

		if expected := 1; len(sx.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.ListCalls)
		}

		if expected := 1; len(sy.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.ListCalls)
		}

		if expected := 1; len(sz.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sz.ListCalls)
		}
	})

	t.Run("with a many wildcard sources and static sources", func(t *testing.T) {
		sx := TestSource{
			ReturnName: "sx",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		sy := TestSource{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test1",
			},
		}

		sz := TestSource{
			ReturnName: "sz",
			ReturnScopes: []string{
				"test2",
				"test3",
			},
		}

		e := newStartedEngine(t, "TestExpandQuery", nil, &sx, &sy, &sz)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.RequestMethod_LIST,
			Query:  "Ross",
			Scope:  "*",
		})

		if expected := 1; len(sx.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.ListCalls)
		}

		if expected := 1; len(sy.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sy.ListCalls)
		}

		if expected := 2; len(sz.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sz.ListCalls)
		}
	})
}
