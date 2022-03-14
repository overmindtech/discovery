package discovery

import "testing"

func TestGetToken(t *testing.T) {
	testPayload := OAuthPayload{
		ClientID:     "SDhOZYZhqMATxNDlhyzdnRu366qETDfS",
		ClientSecret: "GEhibCMxerzBsanfEyUaQG6kCvpvLinDOxpcNDg8_bKpYQnkdrIakefIv_8PyxWg",
		Audience:     "https://auth.overmind.tech",
		GrantType:    ClientCredentials,
	}

	_, err := GetOAuthToken("https://dev-qsurrmp8.eu.auth0.com/oauth/token", testPayload)

	if err != nil {
		t.Error(err)
	}
}
