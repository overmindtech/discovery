package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindtech/discovery/tracing"
	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdp-go/auth"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExecuteQuery(t *testing.T) {
	src := TestAdapter{
		ReturnType:   "person",
		ReturnScopes: []string{"test"},
	}

	e := newStartedEngine(t, "TestExecuteQuery",
		&auth.NATSOptions{
			Servers:           NatsTestURLs,
			ConnectionName:    "test-connection",
			ConnectionTimeout: time.Second,
			MaxReconnects:     5,
		},
		&src,
	)

	t.Run("Basic happy-path Get query", func(t *testing.T) {
		q := &sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
			Query:  "foo",
			Scope:  "test",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 3,
			},
		}

		items, errs, err := e.ExecuteQuerySync(context.Background(), q)

		if err != nil {
			t.Error(err)
		}

		for _, e := range errs {
			t.Error(e)
		}

		if x := len(src.GetCalls); x != 1 {
			t.Errorf("expected adapter's Get() to have been called 1 time, got %v", x)
		}

		if len(items) == 0 {
			t.Fatal("expected 1 item, got none")
		}

		if len(items) > 1 {
			t.Errorf("expected 1 item, got %v", items)
		}

		item := items[0]

		if item.GetMetadata().GetSourceQuery() != q {
			t.Error("adapter query mismatch")
		}
	})

	t.Run("Wrong scope Get query", func(t *testing.T) {
		q := &sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
			Query:  "foo",
			Scope:  "wrong",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 0,
			},
		}

		_, errs, err := e.ExecuteQuerySync(context.Background(), q)

		if err == nil {
			t.Error("expected error but got nil")
		}

		if len(errs) == 1 {
			if errs[0].GetErrorType() != sdp.QueryError_NOSCOPE {
				t.Errorf("expected error type to be NOSCOPE, got %v", errs[0].GetErrorType())
			}
		} else {
			t.Errorf("expected 1 error, got %v", len(errs))
		}

	})

	t.Run("Wrong type Get query", func(t *testing.T) {
		q := &sdp.Query{
			Type:   "house",
			Method: sdp.QueryMethod_GET,
			Query:  "foo",
			Scope:  "test",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 0,
			},
		}

		_, errs, err := e.ExecuteQuerySync(context.Background(), q)

		if err == nil {
			t.Error("expected error but got nil")
		}

		if len(errs) == 1 {
			if errs[0].GetErrorType() != sdp.QueryError_NOSCOPE {
				t.Errorf("expected error type to be NOSCOPE, got %v", errs[0].GetErrorType())
			}
		} else {
			t.Errorf("expected 1 error, got %v", len(errs))
		}
	})

	t.Run("Basic List query", func(t *testing.T) {
		q := &sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_LIST,
			Scope:  "test",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 5,
			},
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
			Type:   "person",
			Method: sdp.QueryMethod_SEARCH,
			Query:  "TEST",
			Scope:  "test",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 5,
			},
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
	personAdapter := TestAdapter{
		ReturnType: "person",
		ReturnScopes: []string{
			"test1",
			"test2",
		},
	}

	dogAdapter := TestAdapter{
		ReturnType: "dog",
		ReturnScopes: []string{
			"test1",
			"testA",
			"testB",
		},
	}

	e := newStartedEngine(t, "TestHandleQuery", nil, &personAdapter, &dogAdapter)

	t.Run("Wildcard type should be expanded", func(t *testing.T) {
		t.Cleanup(func() {
			personAdapter.ClearCalls()
			dogAdapter.ClearCalls()
		})

		req := sdp.Query{
			Type:   sdp.WILDCARD,
			Method: sdp.QueryMethod_GET,
			Query:  "Dylan",
			Scope:  "test1",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 0,
			},
		}

		// Run the handler
		e.HandleQuery(context.Background(), &req)

		// I'm expecting both adapter to get a query since the type was *
		if l := len(personAdapter.GetCalls); l != 1 {
			t.Errorf("expected person backend to have 1 Get call, got %v", l)
		}

		if l := len(dogAdapter.GetCalls); l != 1 {
			t.Errorf("expected dog backend to have 1 Get call, got %v", l)
		}
	})

	t.Run("Wildcard scope should be expanded", func(t *testing.T) {
		t.Cleanup(func() {
			personAdapter.ClearCalls()
			dogAdapter.ClearCalls()
		})

		req := sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
			Query:  "Dylan1",
			Scope:  sdp.WILDCARD,
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 0,
			},
		}

		// Run the handler
		e.HandleQuery(context.Background(), &req)

		if l := len(personAdapter.GetCalls); l != 2 {
			t.Errorf("expected person backend to have 2 Get calls, got %v", l)
		}

		if l := len(dogAdapter.GetCalls); l != 0 {
			t.Errorf("expected dog backend to have 0 Get calls, got %v", l)
		}
	})

}

func TestWildcardAdapterExpansion(t *testing.T) {

	personAdapter := TestAdapter{
		ReturnType: "person",
		ReturnScopes: []string{
			sdp.WILDCARD,
		},
	}

	e := newStartedEngine(t, "TestWildcardAdapterExpansion", nil, &personAdapter)

	t.Run("query scope should be preserved", func(t *testing.T) {
		req := sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
			Query:  "Dylan1",
			Scope:  "something.specific",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 0,
			},
		}

		// Run the handler
		e.HandleQuery(context.Background(), &req)

		if len(personAdapter.GetCalls) != 1 {
			t.Errorf("expected 1 get call got %v", len(personAdapter.GetCalls))
		}

		call := personAdapter.GetCalls[0]

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctx, span := tracing.Tracer().Start(ctx, "TestSendQuerySync")
	defer span.End()

	src := TestAdapter{
		ReturnType: "person",
		ReturnScopes: []string{
			"test",
		},
	}

	e := newStartedEngine(t, "TestSendQuerySync", nil, &src)

	for i := 0; i < 250; i++ {
		u := uuid.New()
		t.Log("starting query: ", u)

		var progress *sdp.QueryProgress
		var items []*sdp.Item

		progress = sdp.NewQueryProgress(&sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
			Query:  "Dylan",
			Scope:  "test",
			RecursionBehaviour: &sdp.Query_RecursionBehaviour{
				LinkDepth: 0,
			},
			IgnoreCache: false,
			UUID:        u[:],
			Deadline:    timestamppb.New(time.Now().Add(10 * time.Minute)),
		})
		progress.StartTimeout = 10 * time.Millisecond

		items, errs, err := progress.Execute(ctx, e.natsConnection)

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
	t.Run("with a single adapter with a single scope", func(t *testing.T) {
		simple := TestAdapter{
			ReturnScopes: []string{
				"test1",
			},
		}
		e := newStartedEngine(t, "TestExpandQuery", nil, &simple)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
			Query:  "Debby",
			Scope:  "*",
		})

		if expected := 1; len(simple.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, len(simple.GetCalls))
		}
	})

	t.Run("with a single adapter with many scopes", func(t *testing.T) {
		many := TestAdapter{
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
			Method: sdp.QueryMethod_GET,
			Query:  "Debby",
			Scope:  "*",
		})

		if expected := 3; len(many.GetCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, many.GetCalls)
		}
	})

	t.Run("with many adapters with single scopes", func(t *testing.T) {
		sx := TestAdapter{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
			},
		}

		sy := TestAdapter{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test2",
			},
		}
		e := newStartedEngine(t, "TestExpandQuery", nil, &sx, &sy)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_GET,
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

	t.Run("with many adapters with many scopes", func(t *testing.T) {
		sx := TestAdapter{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
				"test2",
				"test3",
			},
		}

		sy := TestAdapter{
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
			Method: sdp.QueryMethod_GET,
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

	t.Run("with many adapters with many scopes which overlap GET", func(t *testing.T) {
		sx := TestAdapter{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
				"test2",
				"test3",
			},
			ReturnWeight: 10,
		}

		sy := TestAdapter{
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
			Method: sdp.QueryMethod_GET,
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

	t.Run("with many adapters with many scopes which overlap LIST", func(t *testing.T) {
		sx := TestAdapter{
			ReturnName: "sx",
			ReturnScopes: []string{
				"test1",
				"test2",
				"test3",
			},
		}

		sy := TestAdapter{
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
			Method: sdp.QueryMethod_LIST,
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

	t.Run("with a single wildcard adapter", func(t *testing.T) {
		sx := TestAdapter{
			ReturnName: "sx",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		e := newStartedEngine(t, "TestExpandQuery", nil, &sx)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_LIST,
			Query:  "Rachel",
			Scope:  "*",
		})

		if expected := 1; len(sx.ListCalls) != expected {
			t.Errorf("Expected %v calls, got %v", expected, sx.ListCalls)
		}
	})

	t.Run("with a many wildcard adapters", func(t *testing.T) {
		sx := TestAdapter{
			ReturnName: "sx",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		sy := TestAdapter{
			ReturnName: "sy",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		sz := TestAdapter{
			ReturnName: "sz",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		e := newStartedEngine(t, "TestExpandQuery", nil, &sx, &sy, &sz)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_LIST,
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

	t.Run("with a many wildcard adapters and static adapters", func(t *testing.T) {
		sx := TestAdapter{
			ReturnName: "sx",
			ReturnScopes: []string{
				sdp.WILDCARD,
			},
		}

		sy := TestAdapter{
			ReturnName: "sy",
			ReturnScopes: []string{
				"test1",
			},
		}

		sz := TestAdapter{
			ReturnName: "sz",
			ReturnScopes: []string{
				"test2",
				"test3",
			},
		}

		e := newStartedEngine(t, "TestExpandQuery", nil, &sx, &sy, &sz)

		e.HandleQuery(context.Background(), &sdp.Query{
			Type:   "person",
			Method: sdp.QueryMethod_LIST,
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
