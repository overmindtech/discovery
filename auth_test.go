package discovery

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/nats-io/nkeys"
	"github.com/overmindtech/nats-token-exchange/client"
)

func TestBasicTokenClient(t *testing.T) {
	keys, err := nkeys.CreateUser()

	if err != nil {
		t.Fatal(err)
	}

	c := NewBasicTokenClient("tokeny_mc_tokenface", keys)

	var token string

	token, err = c.GetJWT()

	if err != nil {
		t.Error(err)
	}

	if token != "tokeny_mc_tokenface" {
		t.Error("token mismatch")
	}

	data := []byte{1, 156, 230, 4, 23, 175, 11}

	signed, err := c.Sign(data)

	if err != nil {
		t.Fatal(err)
	}

	err = keys.Verify(data, signed)

	if err != nil {
		t.Error(err)
	}
}

func GetTestOAuthTokenClient(t *testing.T) *OAuthTokenClient {
	var domain string
	var clientID string
	var clientSecret string
	var exists bool

	errorFormat := "environment variable %v not found. Set uo your test environment first. See: https://github.com/overmindtech/auth0-test-data"

	// Read secrets form the environment
	if domain, exists = os.LookupEnv("OVERMIND_NTE_ALLPERMS_DOMAIN"); !exists || domain == "" {
		t.Fatalf(errorFormat, "OVERMIND_NTE_ALLPERMS_DOMAIN")
	}

	if clientID, exists = os.LookupEnv("OVERMIND_NTE_ALLPERMS_CLIENT_ID"); !exists || clientID == "" {
		t.Fatalf(errorFormat, "OVERMIND_NTE_ALLPERMS_CLIENT_ID")
	}

	if clientSecret, exists = os.LookupEnv("OVERMIND_NTE_ALLPERMS_CLIENT_SECRET"); !exists || clientSecret == "" {
		t.Fatalf(errorFormat, "OVERMIND_NTE_ALLPERMS_CLIENT_SECRET")
	}

	return NewOAuthTokenClient(
		clientID,
		clientSecret,
		fmt.Sprintf("https://%v/oauth/token", domain),
		"http://nats-token-exchange:8080",
	)
}

func TestOAuthTokenClient(t *testing.T) {
	c := GetTestOAuthTokenClient(t)

	EnsureTestAccount(c.natsClient.AuthApi)

	var err error

	_, err = c.GetJWT()

	if err != nil {
		t.Error(err)
	}

	// Make sure it can sign
	data := []byte{1, 156, 230, 4, 23, 175, 11}

	_, err = c.Sign(data)

	if err != nil {
		t.Fatal(err)
	}

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