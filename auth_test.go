package discovery

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/nats-io/nkeys"
	"github.com/overmindtech/nats-token-exchange/client"
)

func GetAllPermissionsConfig() (*OAuthTokenClient, error) {
	var domain string
	var clientID string
	var clientSecret string
	var exists bool

	errorFormat := "environment variable %v not found. Set uo your test environment first. See: https://github.com/overmindtech/auth0-test-data"

	// Read secrets form the environment
	if domain, exists = os.LookupEnv("OVERMIND_NTE_ALLPERMS_DOMAIN"); !exists || domain == "" {
		return nil, fmt.Errorf(errorFormat, "OVERMIND_NTE_ALLPERMS_DOMAIN")
	}

	if clientID, exists = os.LookupEnv("OVERMIND_NTE_ALLPERMS_CLIENT_ID"); !exists || clientID == "" {
		return nil, fmt.Errorf(errorFormat, "OVERMIND_NTE_ALLPERMS_CLIENT_ID")
	}

	if clientSecret, exists = os.LookupEnv("OVERMIND_NTE_ALLPERMS_CLIENT_SECRET"); !exists || clientSecret == "" {
		return nil, fmt.Errorf(errorFormat, "OVERMIND_NTE_ALLPERMS_CLIENT_SECRET")
	}

	c := NewOAuthTokenClient(
		clientID,
		clientSecret,
		fmt.Sprintf("https://%v/oauth/token", domain),
		"http://nats-token-exchange:8080",
	)

	return c, nil
}

func TestBasicTokenClient(t *testing.T) {
	c := NewBasicTokenClient("tokeny_mc_tokenface")

	keys, err := nkeys.CreateUser()

	if err != nil {
		t.Fatal(err)
	}

	var token string

	token, err = c.GetNATSToken(context.Background(), keys)

	if err != nil {
		t.Error(err)
	}

	if token != "tokeny_mc_tokenface" {
		t.Error("token mismatch")
	}
}

func TestOAuthTokenClient(t *testing.T) {
	c, err := GetAllPermissionsConfig()

	if err != nil {
		t.Fatal(err)
	}

	EnsureTestAccount(c.natsClient.AuthApi)

	keys, err := nkeys.CreateUser()

	if err != nil {
		t.Fatal(err)
	}

	var token string

	token, err = c.GetNATSToken(context.Background(), keys)

	if err != nil {
		t.Error(err)
	}

	fmt.Print(token)
}

var TestAccountCreated bool

func EnsureTestAccount(a *client.AuthApiService) error {
	if !TestAccountCreated {
		// This is the account that OAuth embeds in test tokens and therefore must
		// be created
		name := "test-account"

		req := a.AccountsPost(context.Background()).InlineObject1(client.InlineObject1{
			Name: &name,
		})

		_, _, err := req.Execute()

		if err != nil {
			return err
		}

		TestAccountCreated = true
	}

	return nil
}
