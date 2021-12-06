package discovery

import (
	"net"
	"net/url"
	"testing"
	"time"
)

var NatsTestURLs = []string{
	"nats://nats:4222",
	"nats://127.0.0.1:4222",
}

// SkipWithoutNats Skips a test if NATS is not available
func SkipWithoutNats(t *testing.T) {
	errors := make([]error, 0)

	for _, urlString := range NatsTestURLs {
		url, err := url.Parse(urlString)

		if err != nil {
			t.Errorf("could not parse NATS URL: %v. Error: %v", urlString, err)
		}

		conn, err := net.DialTimeout("tcp", net.JoinHostPort(url.Hostname(), url.Port()), time.Second)

		if err == nil {
			conn.Close()
			return
		}

		errors = append(errors, err)
	}

	for _, e := range errors {
		t.Log(e)
	}

	t.Skip("NATS not available")
}
