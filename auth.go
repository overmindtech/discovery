package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/overmindtech/nats-token-exchange/client"
)

type GrantType string

const ClientCredentials GrantType = "client_credentials"

// OAuthPayload The payload that will be sent in order to obtain an OAuth token
type OAuthPayload struct {
	ClientID     string    `json:"client_id"`
	ClientSecret string    `json:"client_secret"`
	Audience     string    `json:"audience"`
	GrantType    GrantType `json:"grant_type"`
}

// OAuthResponse Represents the responses that are returns from the OAuth server
type OAuthResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
}

// GetOAuthToken Gets a new OAuth token using a given payload
func GetOAuthToken(tokenURL string, payload OAuthPayload) (OAuthResponse, error) {
	var response OAuthResponse

	payloadBytes, err := json.Marshal(payload)

	if err != nil {
		return response, err
	}

	payloadReader := bytes.NewReader(payloadBytes)

	// payload := strings.NewReader("{\"client_id\":\"SDhOZYZhqMATxNDlhyzdnRu366qETDfS\",\"client_secret\":\"GEhibCMxerzBsanfEyUaQG6kCvpvLinDOxpcNDg8_bKpYQnkdrIakefIv_8PyxWg\",\"audience\":\"https://auth.overmind.tech\",\"grant_type\":\"client_credentials\"}")

	req, err := http.NewRequest("POST", tokenURL, payloadReader)

	if err != nil {
		return response, err
	}

	req.Header.Add("content-type", "application/json")

	var res *http.Response

	res, err = http.DefaultClient.Do(req)

	if err != nil {
		return response, err
	}

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	err = json.Unmarshal(body, &response)

	return response, err
}

func foo() {
	config := client.NewConfiguration()

	c := client.NewAPIClient(config)

	healthCheck := c.DefaultApi.HealthzGet(context.Background())

	resp, err := healthCheck.Execute()

	fmt.Println(resp)
	fmt.Println(err)

}
