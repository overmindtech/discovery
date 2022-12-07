package discovery

import (
	"testing"
)

func TestSourcesSource(t *testing.T) {
	t.Run("satisfies Source interface", func(t *testing.T) {
		//lint:ignore S1021 Testing that it satisfies the interface
		var src Source

		src = &SourcesSource{}

		t.Log(src)
	})
}

func TestMetaSourceSearch(t *testing.T) {
	s, err := NewMetaSource(newTestEngine())

	if err != nil {
		t.Fatal(err)
	}

	t.Run("searching for 'instance' type", func(t *testing.T) {
		types, err := s.SearchType("instance")

		if err != nil {
			t.Fatal(err)
		}

		if len(types) != 1 {
			t.Fatalf("expected 1 types got %v", len(types))
		}

		if types[0] != "aws-ec2instance" {
			t.Errorf("expected first resault to be aws-ec2instance, got %v", types[0])
		}
	})

	t.Run("searching for 'ec2' type", func(t *testing.T) {
		types, err := s.SearchType("ec2")

		if err != nil {
			t.Fatal(err)
		}

		if len(types) != 1 {
			t.Fatalf("expected 1 types got %v", len(types))
		}

		if types[0] != "aws-ec2instance" {
			t.Errorf("expected first resault to be aws-ec2instance, got %v", types[0])
		}
	})

	t.Run("searching for 'aws' type", func(t *testing.T) {
		types, err := s.SearchType("aws")

		if err != nil {
			t.Fatal(err)
		}

		if len(types) != 2 {
			t.Fatalf("expected 2 types got %v", len(types))
		}
	})

	t.Run("searching for 'loadbalancer' type", func(t *testing.T) {
		types, err := s.SearchType("loadbalancer")

		if err != nil {
			t.Fatal(err)
		}

		if len(types) != 1 {
			t.Fatalf("expected 1 types got %v", len(types))
		}

		if types[0] != "aws-elasticloadbalancer" {
			t.Errorf("expected first resault to be aws-elasticloadbalancer, got %v", types[0])
		}
	})

}

func newTestEngine() *Engine {
	e := Engine{
		Name: "test",
	}

	e.AddSources(
		&TestSource{
			ReturnType: "aws-ec2instance",
			ReturnContexts: []string{
				"prodAccountInternetBanking",
				"devAccountInternetBanking",
				"global",
			},
			ReturnName: "test-aws-ec2instance-source",
		},
		&TestSource{
			ReturnType: "aws-elasticloadbalancer",
			ReturnContexts: []string{
				"devAccountInternetBanking",
				"some-other-context",
				"global",
			},
			ReturnName: "test-aws-elasticloadbalancer-source",
		},
		&TestSource{
			ReturnType: "ip",
			ReturnContexts: []string{
				"global",
			},
			ReturnName: "test-ip-source",
		},
	)

	return &e
}
