package discovery

import (
	"context"
	"errors"
	"testing"

	"github.com/overmindtech/sdp-go"
)

func TestTypeAdapter(t *testing.T) {
	s := &TypeAdapter{
		sh: newTestAdapterHost(),
	}

	t.Run("satisfies Adapter interface", func(t *testing.T) {
		//lint:ignore S1021 Testing that it satisfies the interface
		var adapter Adapter

		adapter = &TypeAdapter{}

		t.Log(adapter)
	})

	t.Run("listing types", func(t *testing.T) {
		items, err := s.List(context.Background(), "global", false)

		if err != nil {
			t.Error(err)
		}

		for _, item := range items {
			err = item.Validate()

			if err != nil {
				t.Error(err)
			}

			// Check that hidden types aren't included
			if item.UniqueAttributeValue() == "secret" {
				t.Error("hidden type included")
			}
		}

		if len(items) == 0 {
			t.Error("empty list")
		}
	})

	t.Run("get a specific type", func(t *testing.T) {
		item, err := s.Get(context.Background(), "global", "secret", false)

		if err != nil {
			t.Error(err)
		}

		err = item.Validate()

		if err != nil {
			t.Error(err)
		}
	})

	t.Run("get a bad type", func(t *testing.T) {
		_, err := s.Get(context.Background(), "global", "nothing-here", false)

		if err == nil {
			t.Error("expected error got nil")
		}

		var ire *sdp.QueryError

		if errors.As(err, &ire) {
			if ire.GetErrorType() != sdp.QueryError_NOTFOUND {
				t.Errorf("Expected error type NOTFOUND, got %v", ire.GetErrorType())
			}
		}
	})
}

func TestScopeAdapter(t *testing.T) {
	s := &ScopeAdapter{
		sh: newTestAdapterHost(),
	}

	t.Run("satisfies Adapter interface", func(t *testing.T) {
		//lint:ignore S1021 Testing that it satisfies the interface
		var adapter Adapter

		adapter = &ScopeAdapter{}

		t.Log(adapter)
	})

	t.Run("listing Scopes", func(t *testing.T) {
		items, err := s.List(context.Background(), "global", false)

		if err != nil {
			t.Error(err)
		}

		for _, item := range items {
			err = item.Validate()

			if err != nil {
				t.Error(err)
			}

			// Check that hidden Scopes aren't included
			if item.UniqueAttributeValue() == "secret" {
				t.Error("hidden scope included")
			}
		}

		if len(items) == 0 {
			t.Error("empty list")
		}
	})

	t.Run("get a specific Scope", func(t *testing.T) {
		item, err := s.Get(context.Background(), "global", "secret", false)

		if err != nil {
			t.Error(err)
		}

		err = item.Validate()

		if err != nil {
			t.Error(err)
		}
	})

	t.Run("get a bad Scope", func(t *testing.T) {
		_, err := s.Get(context.Background(), "global", "nothing-here", false)

		if err == nil {
			t.Error("expected error got nil")
		}

		var ire *sdp.QueryError

		if errors.As(err, &ire) {
			if ire.GetErrorType() != sdp.QueryError_NOTFOUND {
				t.Errorf("Expected error Scope NOTFOUND, got %v", ire.GetErrorType())
			}
		}
	})
}

func newTestAdapterHost() *AdapterHost {
	sh := NewAdapterHost()
	if err := sh.AddAdapters(
		&TestAdapter{
			ReturnType: "aws-ec2instance",
			ReturnScopes: []string{
				"prodAccountInternetBanking",
				"devAccountInternetBanking",
				"global",
			},
			ReturnName: "test-aws-ec2instance-adapter",
		},
		&TestAdapter{
			ReturnType: "aws-elastic-load-balancer",
			ReturnScopes: []string{
				"devAccountInternetBanking",
				"some-other-scope",
				"global",
			},
			ReturnName: "test-aws-elastic-load-balancer-adapter",
		},
		&TestAdapter{
			ReturnType: "ip",
			ReturnScopes: []string{
				"global",
			},
			ReturnName: "test-ip-adapter",
		},
		&TestAdapter{
			ReturnType: "secret",
			ReturnScopes: []string{
				"global",
				"secret",
			},
			ReturnName: "test-secret-adapter",
			IsHidden:   true,
		},
	); err != nil {
		panic(err)
	}

	return sh
}
