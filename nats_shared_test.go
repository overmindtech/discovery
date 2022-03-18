package discovery

import (
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"
)

var NatsTestURLs = []string{
	"nats://nats:4222",
	"nats://localhost:4222",
}

var NatsAuthTestURLs = []string{"nats://nats-auth:4222"}

// SkipWithoutNats Skips a test if NATS is not available
func SkipWithoutNats(t *testing.T) {
	var err error

	for _, url := range NatsTestURLs {
		err := testURL(url)

		if err == nil {
			return
		}
	}

	if err != nil {
		t.Error(err)
		t.Skip("NATS not available")
	}
}

// SkipWithoutNatsAuth Skips a test if authenticated NATS is not available
func SkipWithoutNatsAuth(t *testing.T) {
	var err error

	for _, url := range NatsAuthTestURLs {
		err := testURL(url)

		if err == nil {
			return
		}
	}

	if err != nil {
		t.Error(err)
		t.Skip("NATS not available")
	}
}

func testURL(testURL string) error {
	url, err := url.Parse(testURL)

	if err != nil {
		return fmt.Errorf("could not parse NATS URL: %v. Error: %v", testURL, err)
	}

	conn, err := net.DialTimeout("tcp", net.JoinHostPort(url.Hostname(), url.Port()), time.Second)

	if err == nil {
		conn.Close()
		return nil
	}

	return err
}
