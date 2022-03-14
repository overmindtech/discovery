package discovery

import (
	"context"
	"fmt"

	"github.com/overmindtech/nats-token-exchange/client"
)

func GetToken() (string, error) {
	config := client.NewConfiguration()

	c := client.NewAPIClient(config)

	healthCheck := c.DefaultApi.HealthzGet(context.Background())

	resp, err := healthCheck.Execute()

	fmt.Println(resp)
	fmt.Println(err)

	return "", nil
}
