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

func TestMetaSourceSearchType(t *testing.T) {
	s, err := NewMetaSource(newTestEngine())

	if err != nil {
		t.Fatal(err)
	}

	t.Run("searching for 'instance' type", func(t *testing.T) {
		types, err := s.SearchField(Type, "instance")

		if err != nil {
			t.Fatal(err)
		}

		if len(types) != 1 {
			t.Fatalf("expected 1 types got %v", len(types))
		}

		if types[0].Value != "aws-ec2instance" {
			t.Errorf("expected first resault to be aws-ec2instance, got %v", types[0])
		}
	})

	t.Run("searching for 'ec2' type", func(t *testing.T) {
		types, err := s.SearchField(Type, "ec2")

		if err != nil {
			t.Fatal(err)
		}

		if len(types) != 1 {
			t.Fatalf("expected 1 types got %v", len(types))
		}

		if types[0].Value != "aws-ec2instance" {
			t.Errorf("expected first resault to be aws-ec2instance, got %v", types[0])
		}
	})

	t.Run("searching for 'aws' type", func(t *testing.T) {
		types, err := s.SearchField(Type, "aws")

		if err != nil {
			t.Fatal(err)
		}

		if len(types) != 2 {
			t.Fatalf("expected 2 types got %v", len(types))
		}
	})

	t.Run("searching for 'ip' type", func(t *testing.T) {
		types, err := s.SearchField(Type, "ip")

		if err != nil {
			t.Fatal(err)
		}

		if len(types) != 1 {
			t.Fatalf("expected 1 types got %v", len(types))
		}
	})

	t.Run("searching for 'elas' type", func(t *testing.T) {
		types, err := s.SearchField(Type, "elas")

		if err != nil {
			t.Fatal(err)
		}

		if len(types) != 1 {
			t.Fatalf("expected 1 types got %v", len(types))
		}
	})
}

func TestMetaSourceSearchContext(t *testing.T) {
	s, err := NewMetaSource(newTestEngine())

	if err != nil {
		t.Fatal(err)
	}

	t.Run("searching using prefixes", func(t *testing.T) {
		prefixes := []string{
			"prod",
			"prodAccount",
			"prodAccountInternet",
			"prodAccountInternetBanking",
		}

		for _, prefix := range prefixes {
			t.Run(prefix, func(t *testing.T) {
				results, err := s.SearchField(Contexts, prefix)

				if err != nil {
					t.Fatal(err)
				}

				if len(results) == 0 {
					t.Fatalf("empty results")
				}

				if results[0].Value != "prodAccountInternetBanking" {
					t.Errorf("expected first resault to be prodAccountInternetBanking, got %v", results[0])
				}
			})
		}
	})

	t.Run("searching using full words", func(t *testing.T) {
		words := []string{
			"Account",
			"Internet",
			"InternetBanking",
		}

		for _, word := range words {
			t.Run(word, func(t *testing.T) {
				results, err := s.SearchField(Contexts, word)

				if err != nil {
					t.Fatal(err)
				}

				if len(results) == 0 {
					t.Fatal("no results found")
				}
			})
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
