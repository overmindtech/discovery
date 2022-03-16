package discovery

import (
	"context"
	"fmt"
	"testing"

	"github.com/nats-io/nkeys"
	"github.com/overmindtech/nats-token-exchange/client"
)

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
	c := NewOAuthTokenClient(
		"SDhOZYZhqMATxNDlhyzdnRu366qETDfS",
		"GEhibCMxerzBsanfEyUaQG6kCvpvLinDOxpcNDg8_bKpYQnkdrIakefIv_8PyxWg",
		"https://dev-qsurrmp8.eu.auth0.com/oauth/token",
		"http://nats-token-exchange:8080",
	)

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
