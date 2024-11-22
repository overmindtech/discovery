package discovery

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/google/uuid"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
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

	command.PersistentFlags().StringArray("nats-servers", []string{"nats://localhost:4222", "nats://nats:4222"}, "A list of NATS servers to connect to")
	cobra.CheckErr(viper.BindEnv("nats-servers", "NATS_SERVERS"))
	command.PersistentFlags().String("nats-jwt", "", "The JWT token that should be used to authenticate to NATS, provided in raw format e.g. eyJ0eXAiOiJKV1Q...")
	cobra.CheckErr(viper.BindEnv("nats-jwt", "NATS_JWT"))
	command.PersistentFlags().String("nats-nkey-seed", "", "The NKey seed which corresponds to the NATS JWT e.g. SUAFK6QUC...")
	cobra.CheckErr(viper.BindEnv("nats-nkey-seed", "NATS_NKEY_SEED"))
	command.PersistentFlags().String("nats-connection-name", "", "The name that the source should use to connect to NATS")
	cobra.CheckErr(viper.BindEnv("nats-connection-name", "NATS_CONNECTION_NAME"))
	command.PersistentFlags().Int("nats-connection-timeout", 10, "The timeout for connecting to NATS")
	cobra.CheckErr(viper.BindEnv("nats-connection-timeout", "NATS_CONNECTION_TIMEOUT"))

	command.PersistentFlags().Int("max-parallel", 0, "The maximum number of parallel executions")
	cobra.CheckErr(viper.BindEnv("max-parallel", "MAX_PARALLEL"))
}

func EngineConfigFromViper(engineType, version string) (*EngineConfig, error) {
	var sourceName string
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("error getting hostname: %w", err)
	}

	if viper.GetString("source-name") == "" {
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

	// setup natsOptions
	var natsConnectionName string
	if viper.GetString("nats-connection-name") == "" {
		natsConnectionName = hostname
	}
	natsOptions := auth.NATSOptions{
		NumRetries:        -1,
		RetryDelay:        5 * time.Second,
		Servers:           viper.GetStringSlice("nats-servers"),
		ConnectionName:    natsConnectionName,
		ConnectionTimeout: time.Duration(viper.GetInt("nats-connection-timeout")) * time.Second,
		MaxReconnects:     -1,
		ReconnectWait:     1 * time.Second,
		ReconnectJitter:   1 * time.Second,
	}

	// decide if we are using apiUrl := oi.ApiUrl.String()
	// what if its manual vs local vs nats only
	appURL := viper.GetString("app")

	natsOnly := false
	// this is a workaround until we can remove nats only authentication. Going forward all sources must send a heartbeat
	if (viper.GetString("nats-jwt") != "" && viper.GetString("nats-nkey-seed") != "") && (viper.GetString("api-key") == "" || viper.GetString("source-access-token") == "") {
		log.Debug("Using nats jwt and nkey-seed for authentication")
		natsOnly = true
	} else {
		if viper.GetBool("overmind-managed-source") {
			// If managed source, we expect a token
			if viper.GetString("source-access-token") == "" {
				return nil, fmt.Errorf("source-access-token must be set for managed sources")
			}
		} else {
			// If unmanaged we can have unauthenticated sources for local
			// testing, or we can use an API key
			if allow, exists := os.LookupEnv("ALLOW_UNAUTHENTICATED"); exists && allow == "true" {
				log.Debug("Using unauthenticated mode as ALLOW_UNAUTHENTICATED is set")
			} else if viper.GetString("api-key") == "" {
				return nil, fmt.Errorf("api-key must be set for local sources")
			}
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
		App:                   appURL,
		ApiKey:                viper.GetString("api-key"),
		NATSOptions:           &natsOptions,
		NATSJwt:               viper.GetString("nats-jwt"),
		NATSNkeySeed:          viper.GetString("nats-nkey-seed"),
		NATSOnly:              natsOnly,
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
		"nats-servers":            ec.NATSOptions.Servers,
		"nats-connection-name":    ec.NATSOptions.ConnectionName,
		"nats-connection-timeout": ec.NATSConnectionTimeout,
		"nats-queue-name":         ec.NATSQueueName,
		"nats-only":               ec.NATSOnly,
	}
}

func (ec *EngineConfig) CreateClients() (*HeartbeatOptions, error) {
	if ec.OvermindManagedSource == sdp.SourceManaged_LOCAL {
		tokenClient, err := auth.NewAPIKeyClient(ec.App, ec.ApiKey)
		if err != nil {
			err = fmt.Errorf("error creating API key client %w", err)
			sentry.CaptureException(err)
			log.WithError(err).Fatal("error creating API key client")
		}
		tokenSource := auth.NewAPIKeyTokenSource(ec.ApiKey, ec.App)
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
				ec.App,
			),
			Frequency: time.Second * 30,
		}
		ec.NATSOptions.TokenClient = tokenClient
		// lets print out the config
		log.WithFields(MapFromEngineConfig(ec)).Info("Engine config")
		return &heartbeatOptions, nil
	} else if ec.OvermindManagedSource == sdp.SourceManaged_MANAGED {
		tokenClient, err := auth.NewStaticTokenClient(ec.App, ec.SourceAccessToken, ec.SourceTokenType)
		if err != nil {
			err = fmt.Errorf("error creating static token client %w", err)
			sentry.CaptureException(err)
			log.WithError(err).Fatal("error creating static token client")
		}
		tokenSource := oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: ec.SourceAccessToken,
			TokenType:   ec.SourceTokenType,
		})
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
				ec.App,
			),
			Frequency: time.Second * 30,
		}
		ec.NATSOptions.TokenClient = tokenClient
		// lets print out the config
		log.WithFields(MapFromEngineConfig(ec)).Info("Engine config")
		return &heartbeatOptions, nil
	} else if ec.NATSOnly {
		tokenClient, err := createNATSTokenClient(ec.NATSJwt, ec.NATSNkeySeed)
		log.Info("Using NATS authentication, no heartbeat will be sent")
		if err != nil {
			log.WithError(err).Fatal("Error validating NATS authentication info")
		}
		ec.NATSOptions.TokenClient = tokenClient
		// lets print out the config
		log.WithFields(MapFromEngineConfig(ec)).Info("Engine config")
		return nil, nil
	} else if allow, exists := os.LookupEnv("ALLOW_UNAUTHENTICATED"); exists && allow == "true" {
		// this is a special case for testing the api-server
		log.Debug("Using unauthenticated mode as ALLOW_UNAUTHENTICATED is set")
		return nil, nil
	}
	err := fmt.Errorf("unable to setup authentication. source managed %v. nats only:%v", ec.OvermindManagedSource, ec.NATSOnly)
	sentry.CaptureException(err)
	log.WithError(err).Fatal("unable to setup authentication")
	return nil, err
}

// createNATSTokenClient Creates a basic token client that will authenticate to NATS
// using the given values
func createNATSTokenClient(natsJWT string, natsNKeySeed string) (auth.TokenClient, error) {
	var kp nkeys.KeyPair
	var err error

	if natsJWT == "" {
		return nil, errors.New("nats-jwt was blank. This is required when using authentication")
	}

	if natsNKeySeed == "" {
		return nil, errors.New("nats-nkey-seed was blank. This is required when using authentication")
	}

	if _, err = jwt.DecodeUserClaims(natsJWT); err != nil {
		return nil, fmt.Errorf("could not parse nats-jwt: %w", err)
	}

	if kp, err = nkeys.FromSeed([]byte(natsNKeySeed)); err != nil {
		return nil, fmt.Errorf("could not parse nats-nkey-seed: %w", err)
	}

	return auth.NewBasicTokenClient(natsJWT, kp), nil
}
