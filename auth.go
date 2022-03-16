package discovery

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"

	"github.com/nats-io/nkeys"
	"github.com/overmindtech/nats-token-exchange/client"
	"golang.org/x/oauth2/clientcredentials"
)

const UserAgentVersion = "0.1"

// TokenClient Represents something that is capable of getting NATS JWT tokens
// for a given set of NKeys
type TokenClient interface {
	GetNATSToken(ctx context.Context, keys nkeys.KeyPair) (string, error)
}

// BasicTokenClient stores a static token and returns it when called, ignoring
// any provided NKeys or context since it already has the token and doesn't need
// to make any requests
type BasicTokenClient struct {
	staticToken string
}

// NewBasicTokenClient Creates a new basic token client that simply returns a static token
func NewBasicTokenClient(token string) *BasicTokenClient {
	return &BasicTokenClient{
		staticToken: token,
	}
}

func (b *BasicTokenClient) GetNATSToken(_ context.Context, _ nkeys.KeyPair) (string, error) {
	return b.staticToken, nil
}

// OAuthTokenClient Gets a NATS token by first authenticating to OAuth using the
// Client Credentials Flow, then using that token to retrieve a NATS token
type OAuthTokenClient struct {
	oAuthClient *clientcredentials.Config
	natsConfig  *client.Configuration
	natsClient  *client.APIClient
}

// NewOAuthTokenClient Generates a token client that authenticates to OAuth
// using the client credentials flow, then uses that auth to get a NATS token.
// `clientID` and `clientSecret` are used to authenticate using the client
// credentials flow with an API at `oAuthTokenURL`. `natsTokenExchangeURL` is
// the root URL of the NATS token exchange API that will be used e.g.
// https://api.server.test/v1
func NewOAuthTokenClient(clientID string, clientSecret string, oAuthTokenURL string, natsTokenExchangeURL string) *OAuthTokenClient {
	conf := &clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     oAuthTokenURL,
		EndpointParams: url.Values{
			"audience": []string{"https://auth.overmind.tech"},
		},
	}

	// Get an authenticated client that we can then make more HTTP calls with
	authenticatedClient := conf.Client(context.TODO())

	// Configure the token exchange client to use the newly authenticated HTTP
	// client among other things
	tokenExchangeConf := &client.Configuration{
		DefaultHeader: make(map[string]string),
		UserAgent:     fmt.Sprintf("Overmind/%v (%v/%v)", UserAgentVersion, runtime.GOOS, runtime.GOARCH),
		Debug:         false,
		Servers: client.ServerConfigurations{
			{
				URL:         natsTokenExchangeURL,
				Description: "NATS Token Exchange Server",
			},
		},
		OperationServers: map[string]client.ServerConfigurations{},
		HTTPClient:       authenticatedClient,
	}

	nClient := client.NewAPIClient(tokenExchangeConf)

	return &OAuthTokenClient{
		oAuthClient: conf,
		natsConfig:  tokenExchangeConf,
		natsClient:  nClient,
	}
}

// GetNATSToken Authenticates to OAuth, gets a token, then exchanges that at the
// nats-token-exchange for a NATS token that will work for the supplied set of
// NKeys
func (o *OAuthTokenClient) GetNATSToken(ctx context.Context, keys nkeys.KeyPair) (string, error) {
	var pubKey string
	var hostname string
	var token string
	var response *http.Response
	var err error

	pubKey, err = keys.PublicKey()

	if err != nil {
		return "", err
	}

	hostname, err = os.Hostname()

	if err != nil {
		return "", err
	}

	// Create the request for a NATS token
	request := o.natsClient.AuthApi.TokensPost(ctx).InlineObject(client.InlineObject{
		UserPubKey: &pubKey,
		UserName:   &hostname,
	})

	token, response, err = request.Execute()

	if err != nil {
		return "", fmt.Errorf("getting NATS token failed: %v\nResponse: %v", err, response)
	}

	return token, nil
}
