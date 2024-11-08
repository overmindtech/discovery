package discovery

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/google/uuid"
	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdp-go/auth"
	"github.com/overmindtech/sdp-go/sdpconnect"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/oauth2"
)

func AddEngineFlags(command *cobra.Command) {
	command.PersistentFlags().String("source-name", "", "The name of the source")
	cobra.CheckErr(viper.BindEnv("source-name", "SOURCE_NAME"))
	command.PersistentFlags().String("source-uuid", "", "The UUID of the source, is this is blank it will be auto-generated. This is used in heartbeats and shouldn't be supplied usually")
	cobra.CheckErr(viper.BindEnv("source-uuid", "SOURCE_UUID"))
	command.PersistentFlags().String("source-access-token", "", "The access token to use to authenticate the source for managed sources")
	cobra.CheckErr(viper.BindEnv("source-access-token", "SOURCE_ACCESS_TOKEN"))
	command.PersistentFlags().String("source-token-type", "", "The type of token to use to authenticate the source for managed sources")
	cobra.CheckErr(viper.BindEnv("source-token-type", "SOURCE_TOKEN_TYPE"))
	command.PersistentFlags().Bool("overmind-managed-source", false, "If you are running the source yourself or if it is managed by Overmind")
	_ = command.Flags().MarkHidden("overmind-managed-source")
	cobra.CheckErr(viper.BindEnv("overmind-managed-source", "OVERMIND_MANAGED_SOURCE"))

	command.PersistentFlags().String("app", "https://app.overmind.tech", "The URL of the Overmind app to use")
	cobra.CheckErr(viper.BindEnv("app", "APP"))
	command.PersistentFlags().String("api-key", "", "The API key to use to authenticate to the Overmind API")
	cobra.CheckErr(viper.BindEnv("api-key", "OVM_API_KEY", "API_KEY"))

	command.PersistentFlags().Int("max-parallel", 0, "The maximum number of parallel executions")
	cobra.CheckErr(viper.BindEnv("max-parallel", "MAX_PARALLEL"))
}

func EngineConfigFromViper(engineType, version string) (*EngineConfig, error) {
	var sourceName string
	if viper.GetString("source-name") == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("error getting hostname: %w", err)
		}
		sourceName = fmt.Sprintf("%s-%s", engineType, hostname)
	} else {
		sourceName = viper.GetString("source-name")
	}

	sourceUUIDString := viper.GetString("source-uuid")
	var sourceUUID uuid.UUID
	if sourceUUIDString == "" {
		sourceUUID = uuid.New()
	} else {
		var err error
		sourceUUID, err = uuid.Parse(sourceUUIDString)
		if err != nil {
			return nil, fmt.Errorf("error parsing source-uuid: %w", err)
		}
	}

	// this is a workaround until we can remove nats only authentication. Going forward all sources must send a heartbeat
	if (viper.GetString("nats-jwt") != "" && viper.GetString("nats-nkey-seed") != "") && (viper.GetString("api-key") == "" || viper.GetString("source-access-token") == "") {
		log.Debug("Using nats jwt and nkey-seed for authentication")
	} else {
		if viper.GetBool("overmind-managed-source") && viper.GetString("source-access-token") == "" {
			return nil, fmt.Errorf("source-access-token must be set for managed sources")
		}
		if !viper.GetBool("overmind-managed-source") && viper.GetString("api-key") == "" {
			return nil, fmt.Errorf("api-key must be set for local sources")
		}
	}

	var managedSource sdp.SourceManaged
	if viper.GetBool("overmind-managed-source") {
		managedSource = sdp.SourceManaged_MANAGED
	} else {
		managedSource = sdp.SourceManaged_LOCAL
	}

	maxParallelExecutions := viper.GetInt("max-parallel")
	if maxParallelExecutions == 0 {
		maxParallelExecutions = runtime.NumCPU()
	}

	return &EngineConfig{
		EngineType:            engineType,
		Version:               version,
		SourceName:            sourceName,
		SourceUUID:            sourceUUID,
		OvermindManagedSource: managedSource,
		SourceAccessToken:     viper.GetString("source-access-token"),
		SourceTokenType:       viper.GetString("source-token-type"),
		App:                   viper.GetString("app"),
		ApiKey:                viper.GetString("api-key"),
		MaxParallelExecutions: maxParallelExecutions,
	}, nil
}

// MapFromEngineConfig Returns the config as a map
func MapFromEngineConfig(ec *EngineConfig) map[string]any {
	var apiKeyClientSecret string
	if ec.ApiKey != "" {
		apiKeyClientSecret = "[REDACTED]"
	}
	var sourceAccessToken string
	if ec.SourceAccessToken != "" {
		sourceAccessToken = "[REDACTED]"
	}

	return map[string]interface{}{
		"engine-type":             ec.EngineType,
		"version":                 ec.Version,
		"source-name":             ec.SourceName,
		"source-uuid":             ec.SourceUUID,
		"source-access-token":     sourceAccessToken,
		"source-token-type":       ec.SourceTokenType,
		"managed-source":          ec.OvermindManagedSource,
		"app":                     ec.App,
		"api-key":                 apiKeyClientSecret,
		"max-parallel-executions": ec.MaxParallelExecutions,
	}
}

func (ec *EngineConfig) CreateClients(oi sdp.OvermindInstance) (auth.TokenClient, *HeartbeatOptions, error) {
	apiUrl := oi.ApiUrl.String()
	var tokenClient auth.TokenClient
	var tokenSource oauth2.TokenSource
	var err error
	if ec.SourceAccessToken != "" {
		tokenClient, err = auth.NewStaticTokenClient(apiUrl, ec.SourceAccessToken, ec.SourceTokenType)
		if err != nil {
			err = fmt.Errorf("error creating static token client %w", err)
			sentry.CaptureException(err)
			log.WithError(err).Fatal("error creating static token client")
		}
		tokenSource = oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: ec.SourceAccessToken,
			TokenType:   ec.SourceTokenType,
		})

	} else if ec.ApiKey != "" {
		tokenClient, err = auth.NewAPIKeyClient(apiUrl, ec.ApiKey)
		if err != nil {
			err = fmt.Errorf("error creating API key client %w", err)
			sentry.CaptureException(err)
			log.WithError(err).Fatal("error creating API key client")
		}
		tokenSource = auth.NewAPIKeyTokenSource(ec.ApiKey, apiUrl)
	} else {
		return nil, nil, fmt.Errorf("api-key or source-access-token must be set")
	}
	transport := oauth2.Transport{
		Source: tokenSource,
		Base:   http.DefaultTransport,
	}
	authenticatedClient := http.Client{
		Transport: otelhttp.NewTransport(&transport),
	}
	heartbeatOptions := HeartbeatOptions{
		ManagementClient: sdpconnect.NewManagementServiceClient(
			&authenticatedClient,
			apiUrl,
		),
		Frequency: time.Second * 30,
	}
	return tokenClient, &heartbeatOptions, nil

}
